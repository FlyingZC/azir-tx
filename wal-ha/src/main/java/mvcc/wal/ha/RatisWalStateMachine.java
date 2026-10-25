package mvcc.wal.ha;

import mvcc.wal.*;
import org.apache.ratis.protocol.*;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.statemachine.StateMachineStorage;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.util.LifeCycle;
import java.nio.file.Files;
import java.nio.file.Path;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/** 只应用真实 Raft 已提交批次，实际安装和非业务索引更新使用同一有界顺序队列。 */
final class RatisWalStateMachine extends BaseStateMachine {
    private final WalReplicaIdentity identity;
    private final RatisWalState state;
    private final RatisWalNode.Authority authority;
    private final java.util.concurrent.ExecutorService apply;
    private final java.util.concurrent.ExecutorService probes;
    private final java.util.concurrent.ExecutorService images;
    private final Object imageGuard = new Object();
    private CompletableFuture<Long> imageJob;
    private volatile Throwable applicationFailure;
    private final java.util.function.BooleanSupplier eligible;
    private volatile long probeMillis = 1000;
    private final Object budget = new Object();
    private long queuedBytes;
    private int queuedTasks;
    private volatile boolean closed;
    private final WalBackupOptions limits;
    private final RatisSnapshotStorage snapshots;
    private final java.util.concurrent.atomic.AtomicLong durableAckIndex = new java.util.concurrent.atomic.AtomicLong(-1);

    /** 一个平台安装者避免磁盘等待固定全部虚拟载体，队列最多三十二 MiB 和六十四任务。 */
    RatisWalStateMachine(WalReplicaIdentity identity, RatisWalState state, RatisWalNode.Authority authority, WalBackupOptions limits) {
        this(identity, state, authority, limits, () -> true);
    }

    /** 诊断资格来自本节点健康策略，后端探测在独立线程执行。 */
    RatisWalStateMachine(WalReplicaIdentity identity, RatisWalState state, RatisWalNode.Authority authority, WalBackupOptions limits, java.util.function.BooleanSupplier eligible) {
        this.identity = identity; this.state = state; this.authority = authority; this.limits = limits; this.eligible = eligible;
        snapshots = state.supportsSnapshots() ? new RatisSnapshotStorage(RaftGroupId.valueOf(identity.groupId()), limits) : null;
        apply = Executors.newSingleThreadExecutor(Thread.ofPlatform().name("wal-raft-apply-" + identity.nodeId()).factory());
        images = new java.util.concurrent.ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS, new java.util.concurrent.ArrayBlockingQueue<>(1),
                Thread.ofPlatform().name("wal-snapshot-image-" + identity.nodeId()).factory(), new java.util.concurrent.ThreadPoolExecutor.AbortPolicy());
        probes = new java.util.concurrent.ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS, new java.util.concurrent.ArrayBlockingQueue<>(1),
                Thread.ofPlatform().name("wal-health-probe-" + identity.nodeId()).factory(), new java.util.concurrent.ThreadPoolExecutor.AbortPolicy());
    }

    /** 启动必须先恢复持久安装操作及完整镜像，才能将状态机置为可运行。 */
    @Override public void initialize(RaftServer server, RaftGroupId group, RaftStorage storage) throws IOException {
        getLifeCycle().transition(LifeCycle.State.STARTING);
        super.initialize(server, group, storage);
        if (snapshots != null) { RaftDurability.initialize(storage); snapshots.init(storage); restoreLatest(); }
        getLifeCycle().transition(LifeCycle.State.RUNNING);
    }

    /** 代次装配返回真实快照存储，原固定后端继续保持不支持快照。 */
    @Override public StateMachineStorage getStateMachineStorage() { return snapshots == null ? super.getStateMachineStorage() : snapshots; }

    /** 暂停资格立即撤销，再按实际应用顺序关闭旧内核，满足 Ratis 安装生命周期。 */
    @Override public void pause() {
        state.stopServing(); getLifeCycle().transition(LifeCycle.State.PAUSING);
        try { drainImage(); await(enqueue(1, () -> { state.pause(); return Message.valueOf("PAUSED"); })); getLifeCycle().transition(LifeCycle.State.PAUSED); }
        catch (IOException failure) { throw new java.util.concurrent.CompletionException(failure); }
    }

    /** 新状态目录到达后同步、校验并安装新代次，最后才更新应用位置与生命周期。 */
    @Override public void reinitialize() throws IOException {
        if (snapshots == null) { throw new IOException("fixed backend node cannot install snapshots"); }
        getLifeCycle().transition(LifeCycle.State.STARTING); restoreLatest(); applicationFailure = null; getLifeCycle().transition(LifeCycle.State.RUNNING);
    }

    /** 先补齐可独立续做的快照文件，再从已验证的最高位置恢复业务代次。 */
    private void restoreLatest() throws IOException {
        var latest = snapshots.reload(); Path pending = state.pendingBundle();
        if (pending != null) {
            var position = state.pendingPosition(); if (latest == null || latest.getIndex() < position.getIndex()) { latest = snapshots.publish(pending, position); }
        }
        if (latest == null) { state.initialize(null, null); }
        else {
            latest = snapshots.syncReceived(); state.initialize(latest.getFile().getPath(), latest.getTermIndex()); setLastAppliedTermIndex(latest.getTermIndex());
        }
    }

    /** gRPC 成功回复以前核对并 force 当前已接收镜像，再保证独立恢复输入与操作持久化。 */
    void durableSnapshotReceived(TermIndex acknowledged) throws IOException {
        if (snapshots == null) { throw new IOException("fixed backend node cannot acknowledge installed snapshots"); }
        var latest = snapshots.syncReceived();
        if (latest.getIndex() < acknowledged.getIndex() || latest.getIndex() == acknowledged.getIndex() && latest.getTerm() != acknowledged.getTerm()) { throw new IOException("received snapshot does not cover ACK position"); }
        RaftDurability.current(getServer().join(), getGroupId());
        state.durableBeforeSnapshotAck(latest.getFile().getPath(), latest.getTermIndex());
        durableAckIndex.accumulateAndGet(latest.getIndex(), Math::max);
    }

    /** 只在 ACK 持久性检查实际完成后更新，不能用应用位置冒充接收持久确认。 */
    long durableSnapshotAckIndex() { return durableAckIndex.get(); }

    /** 回收前重新验证并同步当前完整镜像，不能仅凭旧缓存的索引删除日志。 */
    long durableSnapshotBoundary(long required) throws IOException {
        if (snapshots == null || failed()) { throw new IOException("snapshot unavailable for purge"); }
        var complete = snapshots.syncReceived(); if (complete.getIndex() < required || !state.activatedSnapshotCovers(required)) { throw new IOException("activated durable snapshot does not cover purge"); }
        return complete.getIndex();
    }

    /** 维护只清理完整快照目录中的过期文件，不推进或删除共识日志。 */
    void cleanupSnapshots(RatisRetirementWork work, java.util.Set<Long> protectedIndices) throws IOException { if (snapshots != null) { snapshots.cleanup(work, protectedIndices); } }

    /** 镜像发布持有快照存储锁时跳过文件维护，避免维护占住代次锁后长期阻塞健康探测。 */
    boolean maintainFiles(RatisRetirementWork work) throws IOException {
        synchronized (imageGuard) {
            if (imageJob != null && !imageJob.isDone()) { return false; }
            cleanupBuilds(work);
            if (!work.exhausted) { state.cleanupSnapshots(work, this::cleanupSnapshots); }
            if (!work.exhausted) { state.retire(work); }
            return true;
        }
    }

    /** 异步安装尚未结束时，后续日志 ACK 也必须先保证其快照基底可恢复。 */
    void durableSnapshotBaseForAppend() throws IOException {
        if (state.ready()) { return; }
        if (snapshots == null) { throw new IOException("fixed backend not ready for append ACK"); }
        long required = getServer().join().getDivision(getGroupId()).getRaftLog().getSnapshotIndex();
        if (required < 0) { return; }
        var latest = snapshots.syncReceived(); if (latest.getIndex() < required) { throw new IOException("append ACK snapshot base missing"); }
        state.durableBeforeSnapshotAck(latest.getFile().getPath(), latest.getTermIndex());
    }

    /** 固定应用边界和后端读视图以后后台导出，同一时刻最多一个完整镜像任务。 */
    CompletableFuture<Long> requestSnapshot() throws IOException {
        if (snapshots == null) { throw new IOException("snapshot requires generation backend factory"); }
        synchronized (imageGuard) {
            if (closed) { throw new IOException("snapshot state machine closed"); }
            if (imageJob == null || imageJob.isDone()) {
                imageJob = new CompletableFuture<>(); var started = imageJob;
                try { images.execute(() -> { try { started.complete(buildImage()); } catch (Throwable failure) { started.completeExceptionally(failure); } }); }
                catch (RuntimeException rejected) { started.completeExceptionally(rejected); }
            }
            return imageJob;
        }
    }

    /** 同步管理入口只等待同一个后台任务，超时不会取消正在执行的共享 I/O。 */
    @Override public long takeSnapshot() throws IOException {
        var job = requestSnapshot();
        try { return job.get(limits.timeoutMillis(), TimeUnit.MILLISECONDS); }
        catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new IOException("snapshot wait interrupted; inspect actual progress", interrupted); }
        catch (java.util.concurrent.ExecutionException | java.util.concurrent.TimeoutException unresolved) { throw new IOException("snapshot failed or unresolved", unresolved); }
    }

    /** 准备输出绑定实际内核、Raft 位置与配置，后台线程不能重新猜测边界。 */
    private record ImageCut(WalMvccRuntime core, TermIndex position, long configurationIndex, RaftProtos.RaftConfigurationProto configuration, Path work, WalReplicaBackup backup) { }

    /** 已发布但上次尚未完成激活的镜像，补做时必须绑定同一个内核。 */
    private record ExistingCut(WalMvccRuntime core, TermIndex position) { }

    /** 大数据库导出、封装与内容校验离开应用线程，发布前重新检查当前代次。 */
    private long buildImage() throws Exception {
        // 上次名称已发布而后续同步报错时，先在镜像线程重载，不能在应用线程扫描大文件。
        snapshots.reload();
        var cut = new java.util.concurrent.atomic.AtomicReference<ImageCut>(); var existing = new java.util.concurrent.atomic.AtomicReference<ExistingCut>();
        var preparation = enqueue(1, () -> {
            if (closed || !state.ready() || failed()) { throw new IOException("business generation not ready for snapshot"); }
            var position = getLastAppliedTermIndex(); if (position == null || position.getTerm() < 1 || position.getIndex() < 0) { return Message.valueOf("NO_SNAPSHOT"); }
            var current = snapshots.getLatestSnapshot(); if (current != null && current.getIndex() >= position.getIndex()) {
                if (current.getIndex() > position.getIndex()) { throw new IOException("snapshot is ahead of applied generation"); }
                existing.set(new ExistingCut(state.core(), current.getTermIndex())); return Message.valueOf("EXISTING_SNAPSHOT");
            }
            var configuration = getServer().join().getDivision(getGroupId()).getRaftConf(); var proto = RaftProtos.RaftConfigurationProto.newBuilder();
            configuration.getCurrentPeers(RaftProtos.RaftPeerRole.FOLLOWER).forEach(peer -> proto.addPeers(peer.getRaftPeerProto()));
            configuration.getPreviousPeers(RaftProtos.RaftPeerRole.FOLLOWER).forEach(peer -> proto.addOldPeers(peer.getRaftPeerProto()));
            configuration.getCurrentPeers(RaftProtos.RaftPeerRole.LISTENER).forEach(peer -> proto.addListeners(peer.getRaftPeerProto()));
            configuration.getPreviousPeers(RaftProtos.RaftPeerRole.LISTENER).forEach(peer -> proto.addOldListeners(peer.getRaftPeerProto()));
            var buildId = java.util.UUID.randomUUID(); Path work = snapshots.getSnapshotDir().toPath().resolve(".build-" + buildId); Files.createDirectory(work);
            new RatisGenerationRecord(identity, buildId, -1, -1, 0, "").publish(work.resolve("BUILD")); RatisGenerationRecord.forceDirectory(work.getParent()); var core = state.core();
            try {
                WalReplicaBackup pinned = null;
                if (state.supportsPinnedSnapshots()) { pinned = core.prepareReplicaBackup(authority, work.resolve("backup"), limits); }
                else { core.backupReplica(authority, work.resolve("backup"), limits); } // 旧自定义 SPI 的手动镜像保留串行合同。
                cut.set(new ImageCut(core, position, configuration.getLogEntryIndex(), proto.build(), work, pinned));
            }
            catch (Throwable failure) { try { discardBuild(work); } catch (Throwable cleanup) { failure.addSuppressed(cleanup); } throw failure; }
            return Message.valueOf("SNAPSHOT_PREPARED");
        });
        try { await(preparation); }
        catch (IOException abandoned) {
            // 观察超时不取消应用线程；若准备随后完成，由其完成回调关闭全部固定读视图。
            preparation.whenComplete((ignored, failure) -> {
                var orphan = cut.getAndSet(null); if (orphan == null) { return; }
                try { if (orphan.backup() != null) { orphan.backup().close(); } } catch (Exception cleanup) { abandoned.addSuppressed(cleanup); }
                try { discardBuild(orphan.work()); } catch (Exception cleanup) { abandoned.addSuppressed(cleanup); }
            });
            throw abandoned;
        }
        var fixed = cut.get(); if (fixed == null) { var previous = existing.get(); return previous == null ? -1 : activateExisting(previous); }
        try (var backup = fixed.backup()) {
            state.lifecycleStage(RatisWalState.Stage.AFTER_IMAGE_PREPARED);
            if (backup != null) { backup.complete(); }
            state.lifecycleStage(RatisWalState.Stage.AFTER_IMAGE_COMPLETE); Path bundle = fixed.work().resolve("snapshot.bundle");
            var header = RatisSnapshotBundle.pack(fixed.work().resolve("backup"), bundle, getGroupId(), fixed.position(), fixed.configurationIndex(), fixed.configuration(), limits);
            // 复制完整镜像不占应用队列，只有活动指针的短发布再次按应用顺序执行。
            if (closed || !state.ready() || failed() || state.core() != fixed.core()) { throw new IOException("snapshot generation changed during export"); }
            var published = snapshots.publish(bundle, fixed.position());
            state.lifecycleStage(RatisWalState.Stage.AFTER_IMAGE_PUBLISHED);
            var note = state.prepareSnapshotNote(header, published.getFile().getPath());
            await(enqueue(1, () -> { if (closed || !state.ready() || failed() || state.core() != fixed.core()) { throw new IOException("snapshot generation changed before activation"); }
                state.lifecycleStage(RatisWalState.Stage.BEFORE_IMAGE_ACTIVE); state.noteSnapshot(note); state.lifecycleStage(RatisWalState.Stage.AFTER_IMAGE_ACTIVE); return Message.valueOf("SNAPSHOT"); }));
            return fixed.position().getIndex();
        } finally { discardBuild(fixed.work()); }
    }

    /** 已有镜像重新确认持久性并完成 ACTIVE，不能以位置已存在省略最后激活步骤。 */
    private long activateExisting(ExistingCut fixed) throws IOException {
        var current = snapshots.syncReceived();
        if (!current.getTermIndex().equals(fixed.position())) { throw new IOException("snapshot changed during activation retry"); }
        var header = RatisSnapshotBundle.inspect(current.getFile().getPath(), getGroupId(), current.getTermIndex(), limits);
        var note = state.prepareSnapshotNote(header, current.getFile().getPath());
        await(enqueue(1, () -> {
            if (closed || !state.ready() || failed() || state.core() != fixed.core() || getLastAppliedTermIndex().getIndex() < fixed.position().getIndex()) { throw new IOException("generation changed during snapshot activation retry"); }
            state.lifecycleStage(RatisWalState.Stage.BEFORE_IMAGE_ACTIVE); state.noteSnapshot(note); state.lifecycleStage(RatisWalState.Stage.AFTER_IMAGE_ACTIVE); return Message.valueOf("SNAPSHOT_REACTIVATED");
        }));
        return fixed.position().getIndex();
    }

    /** 安装远端快照以前等待本机镜像任务退出，应用线程保持可用于完成或拒绝其发布。 */
    private void drainImage() throws IOException {
        CompletableFuture<Long> job; synchronized (imageGuard) { job = imageJob; }
        if (job == null || job.isDone()) { return; }
        try { job.get(limits.timeoutMillis(), TimeUnit.MILLISECONDS); }
        catch (java.util.concurrent.ExecutionException cancelledByPause) { /* 本机导出因 ready 撤销而拒绝发布，资源已由任务释放。 */ }
        catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new IOException("snapshot drain interrupted", interrupted); }
        catch (java.util.concurrent.TimeoutException running) { throw new IOException("snapshot export did not drain", running); }
    }

    /** 没有镜像任务持有构建树时，才允许核对并回收崩溃留下的私有暂存。 */
    void cleanupBuilds(RatisRetirementWork work) throws IOException {
        if (snapshots == null) { return; }
        synchronized (imageGuard) {
            if (imageJob != null && !imageJob.isDone()) { return; }
            try (var entries = Files.list(snapshots.getSnapshotDir().toPath())) { var iterator = entries.iterator(); while (iterator.hasNext()) {
                work.scan(); var root = iterator.next(); String name = root.getFileName().toString(); if (!name.matches("\\.build-[a-f0-9-]{36}")) { continue; }
                if (!Files.isDirectory(root, java.nio.file.LinkOption.NOFOLLOW_LINKS)) { throw new IOException("snapshot build root is not private directory"); }
                var id = java.util.UUID.fromString(name.substring(7)); Path marker = root.resolve("BUILD");
                if (Files.exists(marker, java.nio.file.LinkOption.NOFOLLOW_LINKS)) {
                    var owner = RatisGenerationRecord.read(marker); if (!owner.identity().equals(identity) || !owner.generation().equals(id)) { throw new IOException("snapshot build owner mismatch"); }
                    RatisRetirementFiles.cleanBuild(root, work);
                } else {
                    // 标记发布前尚未创建数据输入，只接收空目录和半写的私有记录文件。
                    try (var children = Files.list(root)) { var childIterator = children.iterator(); while (childIterator.hasNext()) { work.scan(); var child = childIterator.next(); if (!child.getFileName().toString().matches("\\.record-[a-f0-9-]{36}") || !Files.isRegularFile(child, java.nio.file.LinkOption.NOFOLLOW_LINKS)) { throw new IOException("unmarked snapshot build contains unknown input"); } } }
                    RatisRetirementFiles.cleanBuild(root, work);
                }
            } } catch (RatisRetirementWork.Limit exhausted) { work.exhausted = true; }
        }
    }

    /** 只清理本次生成的私有工作树，已发布快照和活动代次均位于其他路径。 */
    private static void discardBuild(Path work) throws IOException {
        try (var paths = Files.walk(work)) { for (Path path : paths.sorted(java.util.Comparator.reverseOrder()).toList()) { Files.delete(path); } }
        RatisGenerationRecord.forceDirectory(work.getParent());
    }

    /** 等待维护任务不会取消正在刷盘的共享通道，超时仍保持当前安装状态。 */
    private void await(CompletableFuture<?> task) throws IOException {
        try { task.get(limits.timeoutMillis(), TimeUnit.MILLISECONDS); }
        catch (InterruptedException failure) { Thread.currentThread().interrupt(); throw new IOException("state machine maintenance interrupted", failure); }
        catch (java.util.concurrent.ExecutionException | java.util.concurrent.TimeoutException failure) { throw new IOException("state machine maintenance failed or unresolved", failure); }
    }

    /** 业务提案先由本地决议者准备，原始网络消息不能直接变成业务写入。 */
    @Override public TransactionContext startTransaction(RaftClientRequest request) throws IOException {
        byte[] data = request.getMessage().getContent().toByteArray();
        if (RatisReadFence.matches(data)) { authority.validate(RatisReadFence.decode(data), data); }
        else { authority.validate(WalReplicationBatch.decode(data)); }
        return TransactionContext.newBuilder().setStateMachine(this).setClientRequest(request).setLogData(request.getMessage().getContent()).build();
    }

    /** 严格日志顺序点再校验任期，准备之后失去领导权的提案必须拒绝。 */
    @Override public TransactionContext preAppendTransaction(TransactionContext transaction) throws IOException {
        byte[] data = transaction.getStateMachineLogEntry().getLogData().toByteArray();
        if (RatisReadFence.matches(data)) { authority.validate(RatisReadFence.decode(data), data); }
        else { state.lifecycleStage(RatisWalState.Stage.BEFORE_COMMIT_APPEND); authority.validate(WalReplicationBatch.decode(data)); }
        return transaction;
    }

    /** 此回调只会收到多数派已提交日志，复制应用不重新裁决业务冲突。 */
    @Override public CompletableFuture<Message> applyTransaction(TransactionContext transaction) {
        var entry = transaction.getLogEntry(); byte[] payload = entry.getStateMachineLogEntry().getLogData().toByteArray();
        return enqueue(payload.length, true, () -> {
            if (snapshots != null) { RaftDurability.current(getServer().join(), getGroupId()); }
            if (RatisReadFence.matches(payload)) {
                var fence = RatisReadFence.decode(payload);
                if (!fence.origin().engineId().equals(identity.engineId()) || !fence.origin().groupId().equals(identity.groupId()) || fence.epoch() != entry.getTerm()) {
                    throw new IOException("committed read fence mismatch");
                }
                updateLastAppliedTermIndex(entry.getTerm(), entry.getIndex()); return Message.valueOf("FENCE_COMMITTED");
            }
            var batch = WalReplicationBatch.decode(payload);
            if (!batch.origin().engineId().equals(identity.engineId()) || !batch.origin().groupId().equals(identity.groupId()) || batch.epoch() != entry.getTerm()) {
                throw new IOException("committed group or epoch mismatch");
            }
            // 此处共识日志已提交且经过多数派持久性校验，包内测试可在本地发布前延迟。
            state.lifecycleStage(RatisWalState.Stage.BEFORE_COMMIT_APPLIED);
            state.core().applyCommitted(authority, batch.commits()); state.lifecycleStage(RatisWalState.Stage.AFTER_COMMIT_APPLIED); updateLastAppliedTermIndex(entry.getTerm(), entry.getIndex()); return Message.valueOf("APPLIED");
        });
    }

    /** no-op 和配置记录也按实际应用顺序更新，不能越过尚未安装的异步业务记录。 */
    @Override public void notifyTermIndexUpdated(long term, long index) { enqueue(1, true, () -> { updateLastAppliedTermIndex(term, index); return Message.valueOf("INDEX"); }); }

    /** 查询排在此前实际应用之后，Raft LINEARIZABLE 配置还提供多数派读索引。 */
    @Override public CompletableFuture<Message> query(Message request) {
        if (!RatisReplicaStatus.matches(request.getContent().toByteArray())) { return enqueue(1, () -> Message.valueOf("READ_BARRIER")); }
        return diagnostic(-1);
    }

    /** 内部诊断未到应用边界时明确失败，不无限登记等待者，也不在应用线程递归排队。 */
    @Override public CompletableFuture<Message> queryStale(Message request, long minimum) {
        if (!RatisReplicaStatus.matches(request.getContent().toByteArray())) { return super.queryStale(request, minimum); }
        return diagnostic(minimum);
    }

    /** 先固定顺序水位，再在独立有界队列探测 JDBC，最后重新核对代次与应用位置。 */
    private CompletableFuture<Message> diagnostic(long minimum) {
        var sampled = new java.util.concurrent.atomic.AtomicReference<WalMvccRuntime>();
        return enqueue(1, () -> {
            if (minimum >= 0 && (getLastAppliedTermIndex() == null || getLastAppliedTermIndex().getIndex() < minimum)) { throw new IOException("replica has not applied diagnostic boundary"); }
            if (state.ready()) { sampled.set(state.core()); } return statusResponse();
        }).thenCompose(before -> {
            try {
                var cut = RatisReplicaStatus.decode(before.getContent().toByteArray());
                if (!cut.healthy()) { return CompletableFuture.completedFuture(before); }
                return CompletableFuture.supplyAsync(() -> state.backendHealthy(sampled.get(), Math.min(probeMillis, limits.timeoutMillis())), probes)
                        .thenCompose(backend -> enqueue(1, () -> {
                            var after = RatisReplicaStatus.decode(statusResponse().getContent().toByteArray());
                            boolean unchanged = state.ready() && state.core() == sampled.get() && after.appliedIndex() >= cut.appliedIndex() && after.publishedSequence() >= cut.publishedSequence();
                            return Message.valueOf(org.apache.ratis.thirdparty.com.google.protobuf.ByteString.copyFrom(new RatisReplicaStatus(identity.groupId(), identity.nodeId(), after.appliedIndex(), after.publishedSequence(), after.healthy() && backend && unchanged).encode()));
                        }));
            } catch (Exception failure) { return CompletableFuture.failedFuture(failure); }
        });
    }

    /** 快速顺序取样仅观察本地状态，不在日志应用线程执行 JDBC。 */
    private Message statusResponse() throws IOException {
            long index = getLastAppliedTermIndex() == null ? -1 : getLastAppliedTermIndex().getIndex(); long published = 0; boolean healthy = false;
            if (state.ready()) {
                var lifecycle = state.core().lifecycle(); published = state.core().stats().publishedSequence();
                healthy = eligible.getAsBoolean() && !failed() && lifecycle.accepting() && !lifecycle.closed() && lifecycle.fatal() == null && lifecycle.materializationFailure() == null && !lifecycle.proposalBlocked();
            }
            return Message.valueOf(org.apache.ratis.thirdparty.com.google.protobuf.ByteString.copyFrom(new RatisReplicaStatus(identity.groupId(), identity.nodeId(), index, published, healthy).encode()));
    }

    /** 当前回调暂只退休本地旧句柄，完整控制线程和故障转移在后续集成中补齐。 */
    @Override public void notifyNotLeader(Collection<TransactionContext> pendingEntries) { authority.lostLeadership(); }

    /** 有界排队，即使跟随者存储慢也不能无限持有日志消息对象。 */
    private CompletableFuture<Message> enqueue(int bytes, Work task) {
        return enqueue(bytes, false, task);
    }

    /** 提交应用失败以后冻结应用水位，后续控制日志与 no-op 不能跳过失败记录。 */
    private CompletableFuture<Message> enqueue(int bytes, boolean application, Work task) {
        var future = new CompletableFuture<Message>();
        boolean reserved = false;
        try {
            synchronized (budget) {
                while (!closed && (queuedTasks >= 64 || queuedBytes + bytes > 32L * 1024 * 1024)) { budget.wait(); }
                if (closed) { throw new IOException("state machine closed"); }
                queuedTasks++; queuedBytes += bytes;
                reserved = true;
            }
            apply.execute(() -> {
                try { if (application && applicationFailure != null) { throw new IOException("previous committed application failed", applicationFailure); } future.complete(task.run()); }
                catch (Throwable failure) { if (application && applicationFailure == null) { applicationFailure = failure; } future.completeExceptionally(failure); }
                finally { synchronized (budget) { queuedTasks--; queuedBytes -= bytes; budget.notifyAll(); } }
            });
        } catch (Exception failure) {
            if (reserved) { synchronized (budget) { queuedTasks--; queuedBytes -= bytes; budget.notifyAll(); } }
            future.completeExceptionally(failure);
        }
        return future;
    }

    /** 目标诊断与健康管理使用一致的有限探测期限。 */
    void configureProbeBudget(long millis) { probeMillis = millis; }

    /** 业务入口与健康管理可以无阻塞观察应用失效。 */
    boolean failed() { return applicationFailure != null; }

    /** 安装任务可传播磁盘和协议异常，不能吞掉变成成功响应。 */
    @FunctionalInterface private interface Work {
        /** 按顺序安装或观察一个已提交状态。 */
        Message run() throws Exception;
    }

    /** 不中断在途 WAL I/O，只有实际安装者排空后才能关闭底层引擎。 */
    @Override public void close() throws IOException {
        closed = true; synchronized (budget) { budget.notifyAll(); } images.shutdown();
        try { if (!images.awaitTermination(30, TimeUnit.SECONDS)) { throw new IOException("snapshot images did not drain"); } }
        catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new IOException("snapshot image close interrupted", interrupted); }
        probes.shutdown(); apply.shutdown();
        try { if (!probes.awaitTermination(30, TimeUnit.SECONDS)) { throw new IOException("backend probes did not drain"); } }
        catch (InterruptedException failure) { Thread.currentThread().interrupt(); throw new IOException("backend probe close interrupted", failure); }
        try { if (!apply.awaitTermination(30, TimeUnit.SECONDS)) { throw new IOException("committed application did not drain"); } }
        catch (InterruptedException failure) { Thread.currentThread().interrupt(); throw new IOException("state machine close interrupted", failure); }
    }
}
