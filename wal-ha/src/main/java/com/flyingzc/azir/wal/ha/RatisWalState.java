package com.flyingzc.azir.wal.ha;

import com.flyingzc.azir.wal.*;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.server.protocol.TermIndex;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.*;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;

/** 节点独占的业务状态代次，持久安装记录未清除前不会发布可服务状态。 */
final class RatisWalState implements AutoCloseable {
    private static final Set<Path> LOCAL = java.util.concurrent.ConcurrentHashMap.newKeySet();
    private final ReentrantLock gate = new ReentrantLock();
    private final Path directory;
    private final WalReplicaIdentity identity;
    private final Set<String> shards;
    private final WalOptions options;
    private final WalCommitAuthority authority;
    private final WalReplicaBackendFactory factory;
    private final WalBackupOptions limits;
    private final Hook hook;
    private FileChannel lockChannel;
    private FileLock lock;
    private WalReplicaBackendFactory.Lease lease;
    private RatisStartupException pendingBackend;
    private volatile WalMvccRuntime core;
    private volatile boolean ready;
    private boolean closed;
    private RatisGenerationRecord active;
    private final RetirementHook retirementHook;

    /** 可观察的安装边界只向包内故障测试开放。 */
    enum Stage { BEFORE_INPUT_FORCE, AFTER_INSTALL_RECORD, AFTER_RESTORE, BEFORE_ACTIVE, AFTER_ACTIVE, AFTER_CLEARED,
        AFTER_IMAGE_PREPARED, AFTER_IMAGE_COMPLETE, AFTER_IMAGE_PUBLISHED, BEFORE_IMAGE_ACTIVE, AFTER_IMAGE_ACTIVE, BEFORE_LOG_PURGE, AFTER_LOG_PURGE,
        BEFORE_READY_DIRECTORY_FORCE, AFTER_READY_DIRECTORY_FORCE, BEFORE_COMMIT_APPEND, BEFORE_COMMIT_APPLIED, AFTER_COMMIT_APPLIED }

    /** 真实退休进度的包内故障边界。 */
    enum RetirementStage { AFTER_RECORD, AFTER_BACKEND_PROGRESS, AFTER_FILE_DELETE, AFTER_TOMBSTONE }
    /** 测试注入只发生在真实操作之后。 */
    @FunctionalInterface interface RetirementHook {
        /** 在持久退休边界注入中断。 */
        void hit(RetirementStage stage) throws IOException;
    }

    /** 故障测试不替代真实文件刷盘与发布。 */
    @FunctionalInterface interface Hook {
        /** 在实际边界后注入失败。 */
        void hit(Stage stage) throws IOException;
        /** 日志回收携带实际首索引和已覆盖上界，普通装配仍使用空钩子。 */
        default void hit(Stage stage, long firstIndex, long coveredIndex) throws IOException { hit(stage); }
    }

    /** 包内故障验证在真实自动维护位置暂停，正式入口的钩子为空且不暴露网络控制接口。 */
    void lifecycleStage(Stage stage) throws IOException { hook.hit(stage); }

    /** 回收证明来自实际原生日志和完整镜像上界，不使用推测位置。 */
    void lifecycleStage(Stage stage, long firstIndex, long coveredIndex) throws IOException { hook.hit(stage, firstIndex, coveredIndex); }

    /** 原固定后端装配保留现有本地路径，不自动升级为可覆盖的代次工厂。 */
    RatisWalState(WalMvccRuntime core) {
        this.core = core; ready = true; directory = null; identity = null; shards = Set.of(); options = null; authority = null; factory = null; limits = null; hook = stage -> { }; retirementHook = stage -> { };
    }

    /** 建立独占业务根目录；存在安装操作时不打开旧代次，也不把残留目录当作空节点。 */
    RatisWalState(Path requested, WalReplicaIdentity identity, Set<String> shards, WalOptions options,
                  WalCommitAuthority authority, WalReplicaBackendFactory factory, WalBackupOptions limits, Hook hook) throws IOException {
        this(requested, identity, shards, options, authority, factory, limits, hook, stage -> { });
    }

    /** 正式装配默认无注入，退休测试与安装测试各自独立。 */
    RatisWalState(Path requested, WalReplicaIdentity identity, Set<String> shards, WalOptions options,
                  WalCommitAuthority authority, WalReplicaBackendFactory factory, WalBackupOptions limits, Hook hook, RetirementHook retirementHook) throws IOException {
        this.identity = identity; this.shards = Set.copyOf(shards); this.options = options; this.authority = authority; this.factory = factory; this.limits = limits; this.hook = hook;
        this.retirementHook = retirementHook;
        if (Files.isSymbolicLink(requested)) { throw new IOException("business generation root is symbolic"); }
        Files.createDirectories(requested); directory = requested.toRealPath();
        if (!LOCAL.add(directory)) { throw new IOException("business generations already open in this JVM"); }
        try {
            lockChannel = FileChannel.open(directory.resolve("writer.lock"), StandardOpenOption.CREATE, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS); lock = lockChannel.tryLock();
            if (lock == null) { throw new IOException("business generations already owned"); }
            if (Files.exists(directory.resolve("ACTIVE"), LinkOption.NOFOLLOW_LINKS)) { active = checkedRecord("ACTIVE"); }
            if (!Files.exists(directory.resolve("INSTALLING"), LinkOption.NOFOLLOW_LINKS)) {
                if (active == null) {
                    if (Files.exists(directory.resolve("BOOTSTRAPPING"))) { active = checkedRecord("BOOTSTRAPPING"); if (active.term() != -1) { throw new IOException("invalid bootstrap phase"); } }
                    else {
                        try (var paths = Files.list(directory)) { if (paths.anyMatch(path -> !path.getFileName().toString().equals("writer.lock"))) { throw new IOException("orphan generation state cannot bootstrap"); } }
                        active = new RatisGenerationRecord(identity, UUID.randomUUID(), -1, -1, 0, ""); active.publish(directory.resolve("BOOTSTRAPPING"));
                    }
                    openCore(active, true); if (core.stats().publishedSequence() != 0) { throw new IOException("bootstrap generation has unexpected decisions"); } active.publish(directory.resolve("ACTIVE"));
                } else {
                    openCore(active, false);
                    if (Files.exists(directory.resolve("BOOTSTRAPPING")) && (!active.equals(checkedRecord("BOOTSTRAPPING")) || core.stats().publishedSequence() != 0)) { throw new IOException("bootstrap generation phase mismatch"); }
                }
            } else { checkedRecord("INSTALLING"); }
        } catch (Throwable failure) {
            throw RatisStartupException.afterFailure(failure, this::close);
        }
    }

    /** 固定装配不支持快照替换，避免覆写调用方的现有数据库。 */
    boolean supportsSnapshots() { return directory != null; }

    /** 自动生命周期必须有受限后端退休协议，不能默认递归删除自定义存储。 */
    boolean supportsRetirement() { return factory != null && factory.supportsRetirement(); }

    /** 自动后台镜像要求每个后端都能在准备阶段固定实际读视图。 */
    boolean supportsPinnedSnapshots() { gate.lock(); try { return lease != null && lease.stores().values().stream().allMatch(com.flyingzc.azir.wal.WalMaterializedStore::supportsPinnedSnapshot); } finally { gate.unlock(); } }

    /** 已配置的自动版本回收要求每个当前和后续代次都有有限后端维护能力。 */
    private boolean boundedGcRequired;
    /** 持久自动快照策略同样约束之后恢复或安装的租约。 */
    private boolean pinnedSnapshotsRequired;

    /** 启用前核对当前租约，后续安装也必须满足同一能力合同。 */
    void requireBoundedGc() throws IOException {
        gate.lock();
        try {
            if (lease == null) { requirePendingLease(); }
            else if (lease.stores().values().stream().anyMatch(store -> !store.supportsBoundedMaintenance())) { throw new IOException("replica backend cannot perform bounded version GC"); }
            boundedGcRequired = true;
        } finally { gate.unlock(); }
    }

    /** 未完成安装尚无租约，登记能力要求后在恢复真正打开租约时强制校验。 */
    void requirePinnedSnapshots() throws IOException {
        gate.lock();
        try {
            if (lease == null) { requirePendingLease(); }
            else if (lease.stores().values().stream().anyMatch(store -> !store.supportsPinnedSnapshot())) { throw new IOException("replica backend cannot pin snapshot views"); }
            pinnedSnapshotsRequired = true;
        } finally { gate.unlock(); }
    }

    /** 只允许身份匹配的持久恢复操作延后租约检查，普通缺失租约仍然失败。 */
    private void requirePendingLease() throws IOException {
        if (directory == null || closed || !Files.exists(directory.resolve("INSTALLING"), LinkOption.NOFOLLOW_LINKS)) { throw new IOException("replica backend lease unavailable"); }
        checkedRecord("INSTALLING");
    }

    /** 回收和指标属于同一个固定代次，不能在回收过程中关闭该代连接池。 */
    record GcSample(com.flyingzc.azir.wal.WalMaterializedStore.GcProgress progress, WalMvccRuntime.Stats stats, java.util.UUID generation) { }

    /** 独立工作者回收实际已应用版本，事务安全点仍由内核的活跃快照决定。 */
    GcSample collectVersionGarbage(RatisGcOptions options) {
        if (!gate.tryLock()) { return null; }
        try {
            if (!boundedGcRequired || !ready || closed || core == null || !coreHealthy() || !core.lifecycle().accepting()) { return null; }
            var progress = core.collectGarbageBounded(options.keyLimit(), options.rowLimit(), options.workMillis());
            return new GcSample(progress, core.stats(), active.generation());
        } finally { gate.unlock(); }
    }

    /** 已激活快照边界必须覆盖待回收位置，安装未完成时不提供回收许可。 */
    boolean activatedSnapshotCovers(long index) throws IOException {
        return activatedSnapshotCovers(index, RatisRecordFile.REAL);
    }

    /** 可见 ACTIVE 可能来自上次目录同步失败，删除日志前必须重新确认实际记录。 */
    boolean activatedSnapshotCovers(long index, RatisRecordFile.Io io) throws IOException {
        if (!gate.tryLock()) { return false; }
        try {
            if (!ready || closed || active == null || Files.exists(directory.resolve("INSTALLING"))) { return false; }
            var actual = checkedRecord("ACTIVE");
            if (!actual.generation().equals(active.generation()) || actual.index() < active.index() || actual.sequence() > core.stats().publishedSequence()) { throw new IOException("active snapshot durability evidence conflicts with current lease"); }
            if (actual.index() < index) { return false; }
            RatisRecordFile.confirm(directory.resolve("ACTIVE"), actual.encode(), io); active = actual; return true;
        }
        finally { gate.unlock(); }
    }

    /** 就绪标记只在完整安装、指针同步和操作清理之后为真。 */
    boolean ready() { return ready; }

    /** 本地日志内核失效时立即取消业务资格，不在就绪探针里访问数据库。 */
    boolean coreHealthy() { var current = core; if (current == null) { return false; } var status = current.lifecycle(); return status.fatal() == null && !status.closed(); }

    /** 成员提升前实际核对后端可读及前缀未回退，不能只信缓存的健康标志。 */
    boolean backendHealthy() {
        return backendHealthy(core, 1000);
    }

    /** 探测固定内核代次，时间为协作预算；数据库调用仍受已经验证的驱动超时限制。 */
    boolean backendHealthy(WalMvccRuntime expected, long budgetMillis) {
        long until = System.nanoTime() + java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(budgetMillis); boolean locked = false;
        try {
            locked = gate.tryLock(budgetMillis, java.util.concurrent.TimeUnit.MILLISECONDS);
            if (!locked || !ready || core == null || core != expected || !coreHealthy()) { return false; }
            var before = core.stats(); var checkpoints = new java.util.HashMap<String, Long>();
            if (lease != null) { for (var entry : lease.stores().entrySet()) {
                if (System.nanoTime() >= until) { return false; }
                checkpoints.put(entry.getKey(), entry.getValue().checkpoint().sequence());
            } }
            if (System.nanoTime() >= until) { return false; }
            var after = core.stats(); if (System.nanoTime() >= until) { return false; }
            // 物化可能在查询期间推进，上界必须取查询后的发布水位，下界取查询前已知物化水位。
            for (var entry : checkpoints.entrySet()) { if (entry.getValue() < before.materializedSequences().getOrDefault(entry.getKey(), 0L) || entry.getValue() > after.publishedSequence()) { return false; } }
            return true;
        } catch (RuntimeException unavailable) { return false; }
        catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); return false; }
        finally { if (locked) { gate.unlock(); } }
    }

    /** 安装入口先取消服务资格，随后再排空已经提交的应用队列。 */
    void stopServing() { ready = false; }

    /** 内部安装者可在启动阶段访问内核，业务入口仍须检查 ready。 */
    WalMvccRuntime core() { var result = core; if (result == null) { throw new IllegalStateException("business state not open"); } return result; }

    /** 托管观测的存储样本与实际代次一起固定，缓存消费者不会混淆换代前后的计数。 */
    record LocalSample(WalMvccRuntime.Stats stats, java.util.UUID generation) { }

    /** 观测在独立工作者调用，代次忙则跳过，不能阻塞安装去等待监控。 */
    LocalSample localSample() {
        if (!gate.tryLock()) { return null; }
        try { if (!ready || closed || core == null || active == null || !coreHealthy()) { return null; } return new LocalSample(core.stats(), active.generation()); }
        finally { gate.unlock(); }
    }

    /** 已关闭的旧句柄无法跨代次复用，角色事件对当前内核只做本地退休。 */
    void fence(long epoch) { var current = core; if (current != null && epoch > 0) { current.fenceEpoch(epoch); } }

    /** 先取消服务资格再排空旧内核，不中断 WAL I/O，也不删除后端。 */
    void pause() throws IOException { gate.lock(); try { ready = false; closeCore(); } finally { gate.unlock(); } }

    /** 启动或 Ratis 安装完成后恢复；已存在的安装操作必须优先续做。 */
    void initialize(Path snapshot, TermIndex position) throws IOException {
        if (!supportsSnapshots()) { return; }
        gate.lock();
        try {
            if (closed) { throw new IOException("generation manager closed"); }
            ready = false;
            if (Files.exists(directory.resolve("INSTALLING"), LinkOption.NOFOLLOW_LINKS)) { complete(checkedRecord("INSTALLING")); }
            if (snapshot != null) {
                String hash = hash(snapshot); var header = RatisSnapshotBundle.inspect(snapshot, RaftGroupId.valueOf(identity.groupId()), position, limits);
                if (active != null && active.index() == position.getIndex()) {
                    if (active.term() != position.getTerm() || !active.hash().equals(hash)) { throw new IOException("active snapshot identity mismatch"); }
                } else {
                    if (active != null && active.index() > position.getIndex()) { throw new IOException("Raft snapshot behind active generation"); }
                    complete(prepare(snapshot, position, header, hash));
                }
            } else if (active == null || active.index() >= 0) { throw new IOException("active generation requires its Raft snapshot"); }
            if (core == null) { openCore(active, false); }
            // 正式装配先同步 Raft 投票目录，再调用本方法；此标记删除后不得丢失投票状态重格式化。
            if (Files.exists(directory.resolve("BOOTSTRAPPING"))) { RatisGenerationRecord.remove(directory.resolve("BOOTSTRAPPING")); }
            // 上次删除控制记录后若目录同步失败，重试可能只看到记录已消失；放行前仍重新确认目录。
            hook.hit(Stage.BEFORE_READY_DIRECTORY_FORCE); RatisGenerationRecord.forceDirectory(directory); hook.hit(Stage.AFTER_READY_DIRECTORY_FORCE);
            ready = true;
        } finally { gate.unlock(); }
    }

    /** 快照 RPC 确认前独立持久化输入与操作，异步业务导入尚未完成也能安全恢复。 */
    void durableBeforeSnapshotAck(Path snapshot, TermIndex position) throws IOException {
        gate.lock();
        try {
            if (closed) { throw new IOException("generation manager closed"); }
            String hash = hash(snapshot);
            if (active != null && active.index() >= position.getIndex()) {
                if (active.index() == position.getIndex() && (active.term() != position.getTerm() || !active.hash().equals(hash))) { throw new IOException("snapshot ACK active identity mismatch"); }
                return;
            }
            if (Files.exists(directory.resolve("INSTALLING"))) {
                var pending = checkedRecord("INSTALLING");
                if (pending.index() >= position.getIndex()) {
                    if (pending.index() == position.getIndex() && (pending.term() != position.getTerm() || !pending.hash().equals(hash))) { throw new IOException("snapshot ACK pending identity mismatch"); }
                    if (!hash(work(pending).resolve("snapshot.bundle")).equals(pending.hash())) { throw new IOException("snapshot ACK recovery input changed"); } return;
                }
                complete(pending);
            }
            ready = false; var header = RatisSnapshotBundle.inspect(snapshot, RaftGroupId.valueOf(identity.groupId()), position, limits); prepare(snapshot, position, header, hash);
        } finally { gate.unlock(); }
    }

    /** 先同步输入与操作，再允许任何确认或导入；实际代次只从本机生成的 UUID 派生。 */
    private RatisGenerationRecord prepare(Path snapshot, TermIndex position, RatisSnapshotBundle.Header header, String hash) throws IOException {
        var plan = new RatisGenerationRecord(identity, UUID.randomUUID(), position.getTerm(), position.getIndex(), header.boundary().sequence(), hash);
        Path work = work(plan); Files.createDirectory(work); plan.publish(work.resolve("INSTALLATION")); copyBundle(snapshot, work.resolve("snapshot.bundle")); RatisGenerationRecord.forceDirectory(directory);
        plan.publish(directory.resolve("INSTALLING")); hook.hit(Stage.AFTER_INSTALL_RECORD); return plan;
    }

    /** 后台计算的完整文件摘要与不可变文件签名，短发布阶段只核对签名。 */
    record SnapshotNote(RatisSnapshotBundle.Header header, Path bundle, String hash, Object fileKey, long bytes, java.nio.file.attribute.FileTime modified) { }

    /** 大镜像摘要离开应用线程，签名变化时拒绝发布，不能混用不同文件内容。 */
    SnapshotNote prepareSnapshotNote(RatisSnapshotBundle.Header header, Path bundle) throws IOException {
        var before = Files.readAttributes(bundle, java.nio.file.attribute.BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS); String digest = hash(bundle);
        var after = Files.readAttributes(bundle, java.nio.file.attribute.BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        if (!after.isRegularFile() || !java.util.Objects.equals(before.fileKey(), after.fileKey()) || before.size() != after.size() || !before.lastModifiedTime().equals(after.lastModifiedTime())) { throw new IOException("snapshot file changed during activation preparation"); }
        return new SnapshotNote(header, bundle, digest, after.fileKey(), after.size(), after.lastModifiedTime());
    }

    /** 本地完整快照边界短发布，重启不用把自己的完整后端反复导入新空间。 */
    void noteSnapshot(SnapshotNote note) throws IOException {
        noteSnapshot(note, RatisRecordFile.REAL);
    }

    /** 包内 I/O 注入验证实际激活替换的不确定结果，正式入口始终使用真实文件系统。 */
    void noteSnapshot(SnapshotNote note, RatisRecordFile.Io io) throws IOException {
        gate.lock();
        try {
            if (closed || !ready || active == null || Files.exists(directory.resolve("INSTALLING"))) { throw new IOException("generation not ready for snapshot publication"); }
            var current = Files.readAttributes(note.bundle(), java.nio.file.attribute.BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
            if (!current.isRegularFile() || !java.util.Objects.equals(current.fileKey(), note.fileKey()) || current.size() != note.bytes() || !current.lastModifiedTime().equals(note.modified())) { throw new IOException("snapshot file changed before activation"); }
            var header = note.header();
            if (header.position().getIndex() < active.index() || header.boundary().sequence() > core.stats().publishedSequence()
                    || header.position().getIndex() == active.index() && (header.position().getTerm() != active.term() || !note.hash().equals(active.hash()))) { throw new IOException("snapshot activation regressed or conflicts with generation"); }
            var next = new RatisGenerationRecord(identity, active.generation(), header.position().getTerm(), header.position().getIndex(), header.boundary().sequence(), note.hash());
            next.publish(directory.resolve("ACTIVE"), io); active = next;
        } finally { gate.unlock(); }
    }

    /** Ratis 状态目录缺失或落后时，可用独立持久副本重新发布未完成安装的快照。 */
    Path pendingBundle() throws IOException { return supportsSnapshots() && Files.exists(directory.resolve("INSTALLING")) ? work(checkedRecord("INSTALLING")).resolve("snapshot.bundle") : null; }

    /** 持久记录里的位置供状态机核对，不能从文件名猜测业务提交序号。 */
    TermIndex pendingPosition() throws IOException { var plan = checkedRecord("INSTALLING"); return TermIndex.valueOf(plan.term(), plan.index()); }

    /** 完整恢复新空间，再发布活动指针；晚到的失败不能回到旧代次服务。 */
    private void complete(RatisGenerationRecord plan) throws IOException {
        closeCore(); Path bundle = work(plan).resolve("snapshot.bundle");
        if (!hash(bundle).equals(plan.hash())) { throw new IOException("durable installation bundle changed"); }
        var position = TermIndex.valueOf(plan.term(), plan.index()); Path backup = work(plan).resolve("backup");
        if (!Files.exists(backup)) { RatisSnapshotBundle.extract(bundle, backup, RaftGroupId.valueOf(identity.groupId()), position, limits); }
        var info = WalBackup.inspect(backup, limits); if (info.sequence() != plan.sequence()) { throw new IOException("installation business sequence changed"); }
        Path generation = generation(plan); ensureGeneration(plan);
        lease = factory.open(identity, plan.generation()); validateLease(); Path wal = generation.resolve("wal");
        if (!Files.exists(wal) || Files.exists(wal.resolve("RESTORING"))) { WalBackup.restoreReplica(backup, wal, lease.stores(), options, limits, identity); }
        hook.hit(Stage.AFTER_RESTORE);
        core = WalMvccRuntime.openReplica(wal, shards, lease.stores(), options, WalRetentionOptions.preserveAll(), identity, authority);
        if (core.stats().publishedSequence() != plan.sequence()) { throw new IOException("installed generation boundary mismatch"); }
        hook.hit(Stage.BEFORE_ACTIVE); plan.publish(directory.resolve("ACTIVE")); active = plan; hook.hit(Stage.AFTER_ACTIVE);
        RatisGenerationRecord.remove(directory.resolve("INSTALLING")); hook.hit(Stage.AFTER_CLEARED);
    }

    /** 打开已发布代次时必须存在持久副本身份，不能把丢失的目录隐式初始化为空。 */
    private void openCore(RatisGenerationRecord record, boolean fresh) throws IOException {
        Path generation = generation(record); Path wal = generation.resolve("wal");
        ensureNotRetired(record);
        if (!fresh && !Files.isRegularFile(wal.resolve("REPLICA"), LinkOption.NOFOLLOW_LINKS)) { throw new IOException("active generation data missing"); }
        Files.createDirectories(generation); RatisGenerationRecord.forceDirectory(directory); if (fresh) { ensureGeneration(record); }
        // 后端打开失败可能仍持有池和空间保护，先保留清理责任，不能另开同一代次。
        if (pendingBackend != null) { pendingBackend.retryCleanup(); pendingBackend = null; }
        try { lease = factory.open(identity, record.generation()); }
        catch (RatisStartupException failure) { pendingBackend = failure; throw failure; }
        validateLease();
        core = WalMvccRuntime.openReplica(wal, shards, lease.stores(), options, WalRetentionOptions.preserveAll(), identity, authority);
        if (core.stats().publishedSequence() < record.sequence()) { throw new IOException("active generation lost snapshot prefix"); }
        if (!fresh) { ensureGeneration(record); }
    }

    /** 本轮已验证的当前代次可补所有权标记，未知旧目录不自动接管。 */
    private void ensureGeneration(RatisGenerationRecord record) throws IOException {
        ensureNotRetired(record); Path path = generation(record); if (Files.isSymbolicLink(path)) { throw new IOException("generation directory is symbolic"); }
        Files.createDirectories(path); Path marker = path.resolve("GENERATION");
        if (Files.exists(marker, LinkOption.NOFOLLOW_LINKS)) { verifyOwner(RatisGenerationRecord.read(marker), record.generation()); }
        else { new RatisGenerationRecord(identity, record.generation(), -1, -1, 0, "").publish(marker); }
        RatisGenerationRecord.forceDirectory(directory);
    }

    /** 本地墓碑与正在退休的对象都不能重新成为服务代次。 */
    private void ensureNotRetired(RatisGenerationRecord record) throws IOException {
        if (Files.exists(directory.resolve("retired").resolve(record.generation().toString()), LinkOption.NOFOLLOW_LINKS)) { throw new IOException("generation already retired"); }
        Path pending = directory.resolve("RETIRING");
        if (Files.exists(pending, LinkOption.NOFOLLOW_LINKS) && RatisRetirementRecord.read(pending).target().generation().equals(record.generation())) { throw new IOException("generation retirement in progress"); }
    }

    /** 一轮只推进一个旧对象，保留当前代次及当前安装输入，所有删除有实际预算。 */
    void retire(RatisRetirementWork work) throws IOException {
        if (!supportsSnapshots() || !factory.supportsRetirement()) { throw new IOException("node backend cannot retire safely"); }
        if (!gate.tryLock()) { throw new IOException("node state maintenance busy"); }
        var alive = new java.util.concurrent.atomic.AtomicBoolean(true);
        try {
            if (!ready || closed || core == null || core.lifecycle().fatal() != null) { throw new IOException("node not ready for retirement"); }
            RatisRetirementFiles.scratch(directory, false, work); RatisRetirementFiles.scratch(directory.resolve("retired"), false, work);
            Path journal = directory.resolve("RETIRING"); RatisRetirementRecord progress;
            if (Files.exists(journal, LinkOption.NOFOLLOW_LINKS)) { progress = RatisRetirementRecord.read(journal); verifyOwner(progress.target(), progress.target().generation()); }
            else {
                var target = selectRetirement(work); if (target == null) { return; }
                verifyRetirable(target.generation()); checkTombstoneBudget(work);
                progress = new RatisRetirementRecord(target, "", false); progress.publish(journal); retirementHook.hit(RetirementStage.AFTER_RECORD);
            }
            final var target = progress.target();
            var permit = new WalGenerationRetirement(identity, target.generation(), () -> {
                if (!alive.get() || !gate.isHeldByCurrentThread() || lock == null || !lock.isValid() || closed || !ready) { throw new IOException("retirement permit no longer valid"); }
                work.check(); verifyRetirable(target.generation());
            });
            permit.verify();
            while (!progress.backendDone() && work.inspected < work.options.backendShardLimit()) {
                var backend = factory.retire(permit, progress.cursor(), 1); work.inspected += backend.inspected(); work.dropped += backend.dropped();
                progress = new RatisRetirementRecord(target, backend.cursor(), backend.complete()); progress.publish(journal); retirementHook.hit(RetirementStage.AFTER_BACKEND_PROGRESS);
                if (backend.inspected() == 0 && !backend.complete()) { throw new IOException("backend retirement made no progress"); }
            }
            if (!progress.backendDone()) { return; }
            var deletion = (RatisRetirementFiles.Hook) path -> retirementHook.hit(RetirementStage.AFTER_FILE_DELETE);
            permit.verify(); if (!RatisRetirementFiles.clean(generation(target), true, work, deletion)) { return; }
            permit.verify(); if (!RatisRetirementFiles.clean(work(target), false, work, deletion)) { return; }
            Path tombstones = directory.resolve("retired"); if (Files.isSymbolicLink(tombstones)) { throw new IOException("retired catalog is symbolic"); } Files.createDirectories(tombstones);
            Path tombstone = tombstones.resolve(target.generation().toString());
            if (Files.exists(tombstone)) { verifyOwner(RatisGenerationRecord.read(tombstone), target.generation()); } else { target.publish(tombstone); }
            retirementHook.hit(RetirementStage.AFTER_TOMBSTONE);
            work.deleting(); RatisGenerationRecord.remove(journal); work.removed++; work.completed++;
        } catch (RatisRetirementWork.Limit exhausted) { work.exhausted = true; }
        finally { alive.set(false); gate.unlock(); }
    }

    /** 每次都从持久指针检查保护集合，不能凭过期候选列表删除。 */
    private void verifyRetirable(java.util.UUID generation) throws IOException {
        var current = checkedRecord("ACTIVE"); if (current.generation().equals(generation)) { throw new IOException("cannot retire active generation"); }
        Path installing = directory.resolve("INSTALLING"); if (Files.exists(installing) && checkedRecord("INSTALLING").generation().equals(generation)) { throw new IOException("cannot retire installing generation"); }
        if (Files.exists(directory.resolve("BOOTSTRAPPING"))) { throw new IOException("cannot retire during bootstrap"); }
    }

    /** 扫描有限根目录，候选只来自有固定所有权标记的协议目录。 */
    private RatisGenerationRecord selectRetirement(RatisRetirementWork work) throws IOException {
        var candidates = new java.util.TreeMap<String, RatisGenerationRecord>();
        try (var entries = Files.list(directory)) { var iterator = entries.iterator(); while (iterator.hasNext()) {
            work.scan(); Path path = iterator.next(); String name = path.getFileName().toString();
            if (!name.matches("(generation|install)-[a-f0-9-]{36}")) { continue; }
            java.util.UUID generation = java.util.UUID.fromString(name.substring(name.indexOf('-') + 1)); if (generation.equals(active.generation())) { continue; }
            if (Files.exists(directory.resolve("INSTALLING")) && checkedRecord("INSTALLING").generation().equals(generation)) { continue; }
            if (Files.isSymbolicLink(path)) { throw new IOException("retirement candidate is symbolic"); }
            Path marker = path.resolve(name.startsWith("generation-") ? "GENERATION" : "INSTALLATION");
            var owner = RatisGenerationRecord.read(marker); verifyOwner(owner, generation); candidates.put(generation.toString(), owner);
        } }
        return candidates.isEmpty() ? null : candidates.firstEntry().getValue();
    }

    /** 元数据墓碑有有限总量，到上限背压而不是删除历史后允许复活。 */
    private void checkTombstoneBudget(RatisRetirementWork work) throws IOException {
        Path retired = directory.resolve("retired"); if (!Files.exists(retired)) { return; } if (Files.isSymbolicLink(retired)) { throw new IOException("retired catalog is symbolic"); }
        int count = 0; try (var entries = Files.list(retired)) { var iterator = entries.iterator(); while (iterator.hasNext()) { work.scan(); iterator.next(); if (++count >= 4096) { throw new IOException("local retirement tombstone budget exhausted"); } } }
    }

    /** 所有权覆盖整个节点和完整代次 UUID，不以文件夹短名作为授权。 */
    private void verifyOwner(RatisGenerationRecord record, java.util.UUID generation) throws IOException { if (!record.identity().equals(identity) || !record.generation().equals(generation)) { throw new IOException("retirement ownership mismatch"); } }

    /** 保护集合与实际清理处于同一代次锁内，安装不能在取样之后改变保护边界。 */
    void cleanupSnapshots(RatisRetirementWork work, SnapshotCleaner cleaner) throws IOException {
        if (!gate.tryLock()) { throw new IOException("node snapshot maintenance busy"); }
        try {
            work.check(); if (!ready || closed) { throw new IOException("node not ready for snapshot maintenance"); }
            var protectedIndices = new java.util.HashSet<Long>();
            if (active != null && active.index() >= 0) { protectedIndices.add(checkedRecord("ACTIVE").index()); }
            if (Files.exists(directory.resolve("INSTALLING"))) { protectedIndices.add(checkedRecord("INSTALLING").index()); }
            cleaner.clean(work, java.util.Set.copyOf(protectedIndices));
        } catch (RatisRetirementWork.Limit exhausted) { work.exhausted = true; }
        finally { gate.unlock(); }
    }

    /** 清理回调只能在代次锁内运行，快照存储不能反向取得代次锁。 */
    @FunctionalInterface interface SnapshotCleaner {
        /** 在固定保护集合下执行有界文件清理。 */
        void clean(RatisRetirementWork work, java.util.Set<Long> protectedIndices) throws IOException;
    }

    /** 未完成退休是可观测状态，预算到期不会丢掉续做对象。 */
    boolean retirementPending() { return supportsSnapshots() && Files.exists(directory.resolve("RETIRING")); }

    /** 后端能力、路由、可靠持久化和有限超时均在导入或初始化前检查。 */
    private void validateLease() throws IOException {
        if (!shards.containsAll(lease.stores().keySet()) || lease.stores().values().stream().anyMatch(store -> !store.supportsReplicaOwnership() || !store.supportsReplicaRestore())) { throw new IOException("generation backend capabilities mismatch"); }
        for (var store : lease.stores().values()) {
            if (boundedGcRequired && !store.supportsBoundedMaintenance()) { throw new IOException("new generation cannot perform configured bounded version GC"); }
            if (pinnedSnapshotsRequired && !store.supportsPinnedSnapshot()) { throw new IOException("new generation cannot pin configured automatic snapshots"); }
            store.validateProduction();
        }
    }

    /** 元数据必须匹配当前接收节点，不能克隆另一个节点的业务根目录。 */
    private RatisGenerationRecord checkedRecord(String name) throws IOException {
        var record = RatisGenerationRecord.read(directory.resolve(name)); if (!record.identity().equals(identity)) { throw new IOException("generation node identity mismatch"); } return record;
    }

    /** 每次安装的私有操作目录和代次目录都只能由固定 UUID 派生。 */
    private Path work(RatisGenerationRecord plan) { return directory.resolve("install-" + plan.generation()); }

    /** 不接受远端提供的数据库或本地目录名称。 */
    private Path generation(RatisGenerationRecord plan) { return directory.resolve("generation-" + plan.generation()); }

    /** 复制接收文件到独立持久操作目录，避免 Ratis 后续替换状态目录丢掉续做输入。 */
    private void copyBundle(Path source, Path target) throws IOException {
        if (Files.size(source) > limits.maxBytes()) { throw new IOException("installation bundle exceeds budget"); }
        try (var input = FileChannel.open(source, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS);
             var output = FileChannel.open(target, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS)) {
            var buffer = ByteBuffer.allocate(65_536); long bytes = 0; long started = System.nanoTime();
            for (int read; (read = input.read(buffer)) >= 0;) {
                if (read == 0 || (bytes += read) > limits.maxBytes() || System.nanoTime() - started > java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(limits.timeoutMillis())) { throw new IOException("installation copy exceeds budget or made no progress"); }
                buffer.flip(); RatisGenerationRecord.write(output, buffer); buffer.clear();
            }
            hook.hit(Stage.BEFORE_INPUT_FORCE); output.force(true);
        }
        RatisGenerationRecord.forceDirectory(target.getParent());
    }

    /** 固定内存核对完整安装输入，源摘要变更不能续写原操作。 */
    private String hash(Path path) throws IOException {
        try {
            if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS) || Files.size(path) > limits.maxBytes()) { throw new IOException("installation bundle missing or exceeds budget"); }
            var digest = java.security.MessageDigest.getInstance("SHA-256"); var buffer = new byte[65_536]; long started = System.nanoTime();
            try (var input = Files.newInputStream(path, LinkOption.NOFOLLOW_LINKS)) { for (int read; (read = input.read(buffer)) != -1;) {
                if (read == 0 || System.nanoTime() - started > java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(limits.timeoutMillis())) { throw new IOException("installation checksum exceeds budget or made no progress"); } digest.update(buffer, 0, read);
            } }
            return java.util.HexFormat.of().formatHex(digest.digest());
        } catch (java.security.NoSuchAlgorithmException failure) { throw new IOException(failure); }
    }

    /** 先排空内核再关闭连接池，失败保留对应句柄；旧数据保留供后续安全退休。 */
    private void closeCore() throws IOException {
        var previous = core;
        if (previous != null) { previous.fenceEpoch(Long.MAX_VALUE); previous.close(); core = null; }
        if (pendingBackend != null) { pendingBackend.retryCleanup(); pendingBackend = null; }
        var resource = lease;
        if (resource != null) { resource.close(); lease = null; }
    }

    /** 原固定装配和代次装配都只关闭自己持有的资源，独占锁最后释放。 */
    @Override public void close() throws IOException {
        gate.lock();
        try {
            if (closed) { return; } ready = false;
            // 内核关闭失败时仍保留代次锁与租约，不能让后台写者和新进程同时使用后端。
            closeCore(); if (lock != null) { lock.release(); lock = null; }
            if (lockChannel != null) { lockChannel.close(); lockChannel = null; }
            if (directory != null) { LOCAL.remove(directory); } closed = true;
        } finally { gate.unlock(); }
    }
}
