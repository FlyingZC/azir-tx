package mvcc.wal;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/** 完整备份的校验与离线恢复；只操作新目标，RESTORING 标记阻止部分恢复被服务。 */
public final class WalBackup {
    private static final Set<Path> LOCAL_RESTORES = java.util.concurrent.ConcurrentHashMap.newKeySet();

    /** 仅供包内验收观察真实文件刷盘，普通恢复始终执行 force(true)。 */
    @FunctionalInterface interface ForceHook {
        /** 同步已经写出的自有恢复文件，失败时不得发布恢复目录。 */
        void force(Path path, FileChannel channel) throws IOException;
    }
    private static final ForceHook NORMAL_FORCE = (path, channel) -> channel.force(true);

    /** 不可变的运维结果，字节数是备份文件总量而非数据库实际磁盘量。 */
    public record Info(UUID backupId, UUID engineId, long sequence, String digest, Set<String> shards,
                       Set<String> storedShards, Map<String, Long> rows, long bytes, long elapsedMillis) {
        /** 固定路由与计数，调用方不能修改校验结果。 */
        public Info { shards = Set.copyOf(shards); storedShards = Set.copyOf(storedShards); rows = Map.copyOf(rows); }
    }

    /** 有界 I/O 的公共时间预算，只取消本次维护，不取消共享日志通道。 */
    static final class Budget {
        final WalBackupOptions options;
        final long start = System.nanoTime();
        long bytes;

        /** 为一次备份、校验或恢复建立独立预算。 */
        Budget(WalBackupOptions options) { this.options = java.util.Objects.requireNonNull(options); }

        /** 每个分块检查中断和单调期限，不能通过墙钟变化延长维护窗口。 */
        void check() throws IOException {
            if (Thread.currentThread().isInterrupted() || System.nanoTime() - start >= TimeUnit.MILLISECONDS.toNanos(options.timeoutMillis())) {
                throw new IOException("backup or restore interrupted or timed out");
            }
        }

        /** 写出前检查总字节预算，避免不完整备份耗尽目标文件系统。 */
        void add(long count) throws IOException {
            check(); if (count < 0 || count > options.maxBytes() - bytes) { throw new IOException("backup exceeds byte budget"); } bytes += count;
        }
    }

    /** 已验证清单、摘要和恢复镜像，数据本身仍以流式方式处理。 */
    private record Validated(Path root, WalBackupManifest manifest, String manifestHash, WalCheckpointState image, Info info) { }

    /** 备份工具不允许实例化。 */
    private WalBackup() { }

    /** 检查备份完整性，不修改备份、运行时目录或数据库。 */
    public static Info inspect(Path directory, WalBackupOptions options) throws IOException { return validate(directory, new Budget(options)).info(); }

    /** 从已验证备份恢复到全新目录和专属数据库，同一备份的中断恢复允许安全续做。 */
    public static Info restore(Path backup, Path target, Map<String, WalMaterializedStore> stores,
                               WalOptions runtimeOptions, WalBackupOptions options) throws IOException {
        return restoreForTest(backup, target, stores, runtimeOptions, options, stage -> { });
    }

    /** 完整复制镜像只恢复至全新副本空间，保留业务身份但使用接收节点自己的身份。 */
    public static Info restoreReplica(Path backup, Path target, Map<String, WalMaterializedStore> stores,
                                      WalOptions runtimeOptions, WalBackupOptions options, WalReplicaIdentity identity) throws IOException {
        return restoreInternal(backup, target, stores, runtimeOptions, options, stage -> { }, NORMAL_FORCE, java.util.Objects.requireNonNull(identity));
    }

    /** 包内验证副本恢复真实写出、刷盘和发布边界，不暴露生产故障注入接口。 */
    static Info restoreReplicaForTest(Path backup, Path target, Map<String, WalMaterializedStore> stores,
                                      WalOptions runtimeOptions, WalBackupOptions options, WalReplicaIdentity identity,
                                      Consumer<WalMvccRuntime.Stage> hook, ForceHook force) throws IOException {
        return restoreInternal(backup, target, stores, runtimeOptions, options, hook, force, java.util.Objects.requireNonNull(identity));
    }

    /** 包内注入只用于实际写入后的恢复故障验收，不暴露到普通应用流程。 */
    static Info restoreForTest(Path backup, Path target, Map<String, WalMaterializedStore> stores,
                               WalOptions runtimeOptions, WalBackupOptions options, Consumer<WalMvccRuntime.Stage> hook) throws IOException {
        return restoreForTest(backup, target, stores, runtimeOptions, options, hook, NORMAL_FORCE);
    }

    /** 注入真实文件刷盘边界，验证续做时已存在的完整文件也必须重新同步。 */
    static Info restoreForTest(Path backup, Path target, Map<String, WalMaterializedStore> stores,
                               WalOptions runtimeOptions, WalBackupOptions options, Consumer<WalMvccRuntime.Stage> hook, ForceHook force) throws IOException {
        return restoreInternal(backup, target, stores, runtimeOptions, options, hook, force, null);
    }

    /** 同一恢复协议处理两种明确模式，普通入口不能导入复制镜像或重绑定已有存储。 */
    private static Info restoreInternal(Path backup, Path target, Map<String, WalMaterializedStore> stores,
                                        WalOptions runtimeOptions, WalBackupOptions options, Consumer<WalMvccRuntime.Stage> hook,
                                        ForceHook force, WalReplicaIdentity replica) throws IOException {
        var budget = new Budget(options); var validated = validate(backup, budget); var manifest = validated.manifest();
        if (replica == null ? manifest.replica() != null : manifest.replica() == null
                || !replica.engineId().equals(manifest.engineId()) || !replica.groupId().equals(manifest.replica().groupId())) {
            throw new IOException("restore mode, replica engine or group mismatch");
        }
        stores = Map.copyOf(stores);
        if (!stores.keySet().equals(manifest.storedShards()) || stores.values().stream().anyMatch(store -> !store.supportsBackup()
                || replica != null && !store.supportsReplicaRestore())) {
            throw new IOException("restore backend routes or capabilities mismatch");
        }
        validateMemory(validated.image(), runtimeOptions);
        Path destination = canonicalTarget(target);
        if (destination.startsWith(validated.root()) || validated.root().startsWith(destination)) { throw new IOException("restore target overlaps backup"); }
        String suffix = WalCodec.digest(destination.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8)).substring(0, 24);
        String replicaSuffix = replica == null ? "" : "\u0000" + replica.fingerprint();
        byte[] operation = HexFormat.of().parseHex(WalCodec.digest((validated.manifestHash() + "\u0000" + destination + replicaSuffix)
                .getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        UUID operationId = new UUID(ByteBuffer.wrap(operation).getLong(), ByteBuffer.wrap(operation).getLong(8));
        Path lockPath = destination.getParent().resolve(".azir-restore-" + suffix + ".lock");
        if (!LOCAL_RESTORES.add(lockPath)) { throw new IOException("restore already running for target"); }
        try (var lockChannel = FileChannel.open(lockPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS)) {
            var lock = lockChannel.tryLock(); if (lock == null) { throw new IOException("restore target locked by another process"); }
            try (lock) {
                byte[] marker = (validated.manifestHash() + "\n" + (replica == null ? "" : replica.fingerprint() + "\n")).getBytes(java.nio.charset.StandardCharsets.US_ASCII);
                Path stage = destination.getParent().resolve(".azir-restore-" + manifest.backupId() + "-" + suffix);
                Path working;
                if (Files.exists(destination, LinkOption.NOFOLLOW_LINKS)) {
                    requireMarker(destination, marker); working = destination;
                } else {
                    if (!Files.exists(stage, LinkOption.NOFOLLOW_LINKS)) { Files.createDirectory(stage); WalFiles.forceDirectory(stage.getParent()); }
                    requireDirectory(stage);
                    if (!Files.exists(stage.resolve("RESTORING"), LinkOption.NOFOLLOW_LINKS)) {
                        try (var entries = Files.list(stage)) { if (entries.findAny().isPresent()) { throw new IOException("unowned restore staging contents"); } }
                        WalFiles.writeNew(stage.resolve("RESTORING"), marker); WalFiles.forceDirectory(stage);
                    }
                    requireMarker(stage, marker); working = stage;
                }
                // 所有恢复入口都先校验完整备份和目标归属，才允许改变新数据库或暂存目录。
                for (var entry : manifest.files()) {
                    if (!entry.path().startsWith("local/")) { continue; }
                    Path output = checked(working, entry.path().substring(6), false);
                    if (entry.path().equals("local/REPLICA")) { writeReplicaIdentity(output, replica, budget, force); }
                    else { copy(checked(validated.root(), entry.path(), true), output, entry.path(), budget, false, entry, force); }
                    hook.accept(WalMvccRuntime.Stage.AFTER_RESTORE_FILE_COPY);
                }
                forceDirectories(working, manifest.files().stream().filter(file -> file.path().startsWith("local/"))
                        .map(file -> working.resolve(file.path().substring(6)).getParent()).toList());
                for (var data : manifest.data()) {
                    budget.check(); var store = stores.get(data.shard());
                    var spec = new WalRestoreSpec(manifest.backupId(), operationId, manifest.engineId(), data.shard(), manifest.boundary(), replica);
                    boolean importing = store.beginRestore(spec);
                    if (importing) { importData(validated, data, store, spec, budget, hook); }
                    String hash = file(manifest, data.file()).hash(); store.finishRestore(spec, data.rows(), hash);
                }
                validateLocal(working, manifest, budget, replica); budget.check(); hook.accept(WalMvccRuntime.Stage.BEFORE_RESTORE_PUBLISH);
                if (!working.equals(destination)) {
                    if (Files.exists(destination, LinkOption.NOFOLLOW_LINKS)) { throw new IOException("restore target appeared before publication"); }
                    Files.move(working, destination, StandardCopyOption.ATOMIC_MOVE); WalFiles.forceDirectory(destination.getParent());
                }
                hook.accept(WalMvccRuntime.Stage.AFTER_RESTORE_MOVE);
                Files.delete(destination.resolve("RESTORING")); WalFiles.forceDirectory(destination);
                hook.accept(WalMvccRuntime.Stage.AFTER_RESTORE_READY); return info(manifest, validated.info().bytes(), budget);
            }
        } finally { LOCAL_RESTORES.remove(lockPath); }
    }

    /** 提交决议和物化已被运行时暂停后创建备份，任何完成标记之前的失败都留下不可用备份。 */
    static Info create(Path destination, WalLog log, Set<String> shards, Map<String, WalMaterializedStore> stores,
                       Budget budget, Consumer<WalMvccRuntime.Stage> hook) throws IOException {
        Path target = canonicalTarget(destination);
        if (target.startsWith(log.directory()) || log.directory().startsWith(target)) { throw new IOException("backup overlaps active WAL directory"); }
        Files.createDirectory(target); Files.createDirectory(target.resolve("local")); Files.createDirectory(target.resolve("data"));
        var entries = new ArrayList<WalBackupManifest.Entry>(); var data = new ArrayList<WalBackupManifest.Data>();
        var boundary = log.lastSequence() == 0 ? new WalCheckpoint(0, "") : new WalCheckpoint(log.lastSequence(), log.receipt(log.lastSequence()).digest());
        for (var file : log.backupFiles().entrySet().stream().sorted(Map.Entry.comparingByKey()).toList()) {
            Path source = checked(log.directory(), file.getKey().substring(6), true);
            entries.add(copy(source, checked(target, file.getKey(), false), file.getKey(), budget, true, null, NORMAL_FORCE));
            hook.accept(WalMvccRuntime.Stage.AFTER_BACKUP_FILE_COPY);
        }
        int ordinal = 0;
        for (String shard : stores.keySet().stream().sorted().toList()) {
            String name = String.format(java.util.Locale.ROOT, "data/%04d.bin", ordinal++); Path path = checked(target, name, false);
            var digest = sha(); long rows;
            try (var channel = FileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
                 var output = new BufferedOutputStream(new CountedOutput(new DigestOutputStream(java.nio.channels.Channels.newOutputStream(channel), digest), budget), 64 * 1024)) {
                var writer = new WalBackupData.Writer(output, new WalBackupData.Header(log.engineId(), shard, boundary));
                stores.get(shard).exportSnapshot(boundary, row -> { budget.check(); writer.accept(row); });
                rows = writer.finish(); output.flush(); channel.force(true);
            }
            entries.add(new WalBackupManifest.Entry(name, Files.size(path), HexFormat.of().formatHex(digest.digest())));
            data.add(new WalBackupManifest.Data(shard, name, rows));
        }
        var manifest = new WalBackupManifest(UUID.randomUUID(), log.engineId(), boundary, shards, stores.keySet(), entries, data, log.replicaIdentity());
        byte[] bytes = manifest.encode(); budget.add(bytes.length);
        validateContents(target, manifest, budget); forceDirectories(target, entries.stream().map(entry -> target.resolve(entry.path()).getParent()).toList());
        Path temporary = target.resolve("COMPLETE.tmp"); WalFiles.writeNew(temporary, bytes);
        hook.accept(WalMvccRuntime.Stage.BEFORE_BACKUP_COMPLETE);
        Files.move(temporary, target.resolve("COMPLETE"), StandardCopyOption.ATOMIC_MOVE); WalFiles.forceDirectory(target); WalFiles.forceDirectory(target.getParent());
        hook.accept(WalMvccRuntime.Stage.AFTER_BACKUP_COMPLETE); return info(manifest, budget.bytes, budget);
    }

    /** 决议暂停期间只复制本地恢复输入并固定后端读视图，完整冷数据在后续维护线程导出。 */
    static Prepared prepareReplica(Path destination, WalLog log, Set<String> shards, Map<String, WalMaterializedStore> stores,
                                   Budget budget, Consumer<WalMvccRuntime.Stage> hook) throws IOException {
        Path target = canonicalTarget(destination); checkDestination(target, log.directory());
        Files.createDirectory(target); Files.createDirectory(target.resolve("local")); Files.createDirectory(target.resolve("data"));
        var entries = new ArrayList<WalBackupManifest.Entry>(); var views = new java.util.LinkedHashMap<String, WalMaterializedStore.SnapshotView>();
        var boundary = log.lastSequence() == 0 ? new WalCheckpoint(0, "") : new WalCheckpoint(log.lastSequence(), log.receipt(log.lastSequence()).digest());
        try {
            for (var file : log.backupFiles().entrySet().stream().sorted(Map.Entry.comparingByKey()).toList()) {
                entries.add(copy(checked(log.directory(), file.getKey().substring(6), true), checked(target, file.getKey(), false), file.getKey(), budget, true, null, NORMAL_FORCE));
                hook.accept(WalMvccRuntime.Stage.AFTER_BACKUP_FILE_COPY);
            }
            for (String shard : stores.keySet().stream().sorted().toList()) { budget.check(); views.put(shard, stores.get(shard).openSnapshot(boundary)); }
            budget.check(); return new Prepared(target, log.engineId(), log.replicaIdentity(), boundary, shards, entries, views, budget, hook);
        } catch (Throwable failure) {
            for (var view : views.values()) { try { view.close(); } catch (Throwable close) { failure.addSuppressed(close); } }
            if (failure instanceof IOException io) { throw io; } if (failure instanceof Error error) { throw error; } throw new IOException("prepare replica backup failed", failure);
        }
    }

    /** 已复制的本地状态独立于源后续检查点，读视图独立于源后续物化和 GC。 */
    static final class Prepared implements AutoCloseable {
        private final Path target; private final UUID engine; private final WalReplicaIdentity replica; private final WalCheckpoint boundary;
        private final Set<String> shards; private final List<WalBackupManifest.Entry> entries; private final Map<String, WalMaterializedStore.SnapshotView> views;
        private final Budget budget; private final Consumer<WalMvccRuntime.Stage> hook;
        /** 全部输入在同一确认边界固定以后才可以构造。 */
        Prepared(Path target, UUID engine, WalReplicaIdentity replica, WalCheckpoint boundary, Set<String> shards,
                 List<WalBackupManifest.Entry> entries, Map<String, WalMaterializedStore.SnapshotView> views, Budget budget, Consumer<WalMvccRuntime.Stage> hook) {
            this.target = target; this.engine = engine; this.replica = replica; this.boundary = boundary; this.shards = Set.copyOf(shards); this.entries = new ArrayList<>(entries); this.views = Map.copyOf(views); this.budget = budget; this.hook = hook;
        }
        /** 固定边界不再读取源日志当前位置。 */
        WalCheckpoint boundary() { return boundary; }
        /** 后台完成所有冷数据输出、逐文件验证和完成标记发布。 */
        Info complete() throws IOException {
            var data = new ArrayList<WalBackupManifest.Data>(); int ordinal = 0;
            for (String shard : views.keySet().stream().sorted().toList()) {
                budget.check(); String name = String.format(java.util.Locale.ROOT, "data/%04d.bin", ordinal++); Path path = checked(target, name, false); var digest = sha(); long rows;
                try (var channel = FileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
                     var output = new BufferedOutputStream(new CountedOutput(new DigestOutputStream(java.nio.channels.Channels.newOutputStream(channel), digest), budget), 64 * 1024)) {
                    var writer = new WalBackupData.Writer(output, new WalBackupData.Header(engine, shard, boundary));
                    views.get(shard).export(row -> { budget.check(); writer.accept(row); }); rows = writer.finish(); output.flush(); channel.force(true);
                }
                entries.add(new WalBackupManifest.Entry(name, Files.size(path), HexFormat.of().formatHex(digest.digest()))); data.add(new WalBackupManifest.Data(shard, name, rows));
            }
            var manifest = new WalBackupManifest(UUID.randomUUID(), engine, boundary, shards, views.keySet(), entries, data, replica); byte[] bytes = manifest.encode(); budget.add(bytes.length);
            validateContents(target, manifest, budget); forceDirectories(target, entries.stream().map(entry -> target.resolve(entry.path()).getParent()).toList());
            WalFiles.writeNew(target.resolve("COMPLETE.tmp"), bytes); hook.accept(WalMvccRuntime.Stage.BEFORE_BACKUP_COMPLETE);
            Files.move(target.resolve("COMPLETE.tmp"), target.resolve("COMPLETE"), StandardCopyOption.ATOMIC_MOVE); WalFiles.forceDirectory(target); WalFiles.forceDirectory(target.getParent());
            hook.accept(WalMvccRuntime.Stage.AFTER_BACKUP_COMPLETE); return info(manifest, budget.bytes, budget);
        }
        /** 任意一个视图关闭失败也继续关闭其余连接，不留下无限期数据库读事务。 */
        @Override public void close() throws IOException {
            IOException failure = null;
            for (var view : views.values()) { try { view.close(); } catch (Exception close) { if (failure == null) { failure = new IOException("close pinned backup views failed", close); } else { failure.addSuppressed(close); } } }
            if (failure != null) { throw failure; }
        }
    }

    /** 维护开始前拒绝已占用目录和活动日志目录的重叠目标。 */
    static void checkDestination(Path destination, Path active) throws IOException {
        Path target = canonicalTarget(destination);
        if (Files.exists(target, LinkOption.NOFOLLOW_LINKS) || target.startsWith(active) || active.startsWith(target)) { throw new IOException("backup target occupied or overlaps WAL"); }
    }

    /** 对完整备份逐文件校验大小、摘要、内容身份和数据流语义。 */
    private static Validated validate(Path directory, Budget budget) throws IOException {
        Path root = directory.toRealPath(); requireDirectory(root);
        byte[] raw = WalFiles.readBounded(checked(root, "COMPLETE", true), WalBackupManifest.MAX_BYTES);
        var manifest = WalBackupManifest.decode(raw); budget.add(raw.length);
        for (var entry : manifest.files()) { budget.add(entry.size()); }
        WalCheckpointState image = validateContents(root, manifest, budget);
        return new Validated(root, manifest, WalCodec.digest(raw), image, info(manifest, budget.bytes, budget));
    }

    /** 摘要保护文件内容，格式验证另外保护键顺序、边界及必需文件集合。 */
    private static WalCheckpointState validateContents(Path root, WalBackupManifest manifest, Budget budget) throws IOException {
        for (var entry : manifest.files()) {
            Path path = checked(root, entry.path(), true);
            if (Files.size(path) != entry.size() || !checksum(path, budget).equals(entry.hash())) { throw new IOException("backup file checksum mismatch: " + entry.path()); }
        }
        WalCheckpointState image = validateLocal(checked(root, "local", false), manifest, budget);
        for (var data : manifest.data()) {
            try (InputStream input = Files.newInputStream(checked(root, data.file(), true), StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)) {
                long rows = WalBackupData.read(input, new WalBackupData.Header(manifest.engineId(), data.shard(), manifest.boundary()), row -> budget.check());
                if (rows != data.rows()) { throw new IOException("backup row count mismatch"); }
            }
        }
        return image;
    }

    /** 核对本地恢复入口、尾日志、路由和每页提交凭据，不仅核对复制时新生成的摘要。 */
    private static WalCheckpointState validateLocal(Path local, WalBackupManifest manifest, Budget budget) throws IOException {
        return validateLocal(local, manifest, budget, manifest.replica());
    }

    /** 原始镜像核对源身份，恢复现场核对接收身份；引擎与组仍然不可变。 */
    private static WalCheckpointState validateLocal(Path local, WalBackupManifest manifest, Budget budget, WalReplicaIdentity replica) throws IOException {
        requireDirectory(local);
        // 在解析 CURRENT 之前先拒绝任何嵌套符号链接，不能让未列入清单的入口引导读取外部文件。
        try (var paths = Files.walk(local, 2)) {
            var iterator = paths.iterator(); int count = 0;
            while (iterator.hasNext()) {
                Path path = iterator.next();
                if (++count > 104_100 || Files.isSymbolicLink(path)
                        || !Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS) && !Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
                    throw new IOException("invalid local backup file tree");
                }
            }
        }
        var current = WalCheckpointFiles.read(local); var expected = new HashSet<String>(); expected.add("local/ENGINE");
        if (replica != null) {
            if (!replica.equals(WalReplicaIdentity.read(local))) { throw new IOException("backup replica identity mismatch"); }
            expected.add("local/REPLICA");
        }
        byte[] engine = WalFiles.readBounded(checked(local, "ENGINE", true), 60); validateHeader(engine, manifest, replica == null ? 1 : 3, null);
        if (manifest.boundary().sequence() == 0) {
            if (current != null) { throw new IOException("empty backup has a checkpoint"); }
            byte[] wal = WalFiles.readBounded(checked(local, "transactions.wal", true), 60);
            if (!java.util.Arrays.equals(engine, wal)) { throw new IOException("empty backup WAL identity mismatch"); }
            expected.add("local/transactions.wal");
        } else {
            if (current == null || !current.state().engineId().equals(manifest.engineId()) || !current.state().shards().equals(manifest.shards())
                    || !current.state().storedShards().equals(manifest.storedShards()) || current.state().sequence() != manifest.boundary().sequence()
                    || !current.state().boundary().digest().equals(manifest.boundary().digest())) { throw new IOException("backup checkpoint identity mismatch"); }
            String prefix = "local/" + current.generation().getFileName() + "/";
            expected.add("local/CURRENT"); expected.add(prefix + "snapshot.bin"); expected.add(prefix + "tail.wal");
            validateHeader(WalFiles.readBounded(checked(local, current.generation().getFileName() + "/tail.wal", true), 100), manifest, replica == null ? 2 : 4, current.imageHash());
            for (var archive : current.state().archives()) {
                budget.check(); expected.add("local/" + archive.fileName());
                WalReceiptArchive.open(local, manifest.engineId(), archive).forEach(receipt -> {
                    try { budget.check(); } catch (IOException failure) { throw new java.io.UncheckedIOException(failure); }
                });
            }
        }
        for (var data : manifest.data()) { expected.add(data.file()); }
        if (!expected.equals(manifest.files().stream().map(WalBackupManifest.Entry::path).collect(java.util.stream.Collectors.toSet()))) {
            throw new IOException("backup file catalog is incomplete or contains unknown files");
        }
        return current == null ? null : current.state();
    }

    /** 文件头必须绑定同一引擎、路由和镜像，完整但损坏的日志头不能被备份合法化。 */
    private static void validateHeader(byte[] bytes, WalBackupManifest manifest, int version, String imageHash) throws IOException {
        int size = version == 1 || version == 3 ? 60 : 100; if (bytes.length != size) { throw new IOException("backup WAL header length mismatch"); }
        var input = ByteBuffer.wrap(bytes);
        byte[] routes = HexFormat.of().parseHex(WalCodec.digest(String.join("\u0000", manifest.shards().stream().sorted().toList()).getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        if (input.getInt() != 0x415A574C || input.getInt() != version || !new UUID(input.getLong(), input.getLong()).equals(manifest.engineId())) {
            throw new IOException("backup WAL header identity mismatch");
        }
        byte[] actualRoutes = new byte[32]; input.get(actualRoutes);
        if (!java.util.Arrays.equals(routes, actualRoutes) || ByteBuffer.wrap(bytes).getInt(size - 4) != WalFiles.crc(bytes, 0, size - 4)) { throw new IOException("backup WAL header checksum mismatch"); }
        if (version == 2 || version == 4) {
            long cut = input.getLong(); byte[] hash = new byte[32]; input.get(hash);
            if (cut != manifest.boundary().sequence() || !HexFormat.of().formatHex(hash).equals(imageHash)) { throw new IOException("backup WAL checkpoint mismatch"); }
        }
    }

    /** 仅在已校验、带本次恢复标记的新空间写接收身份，重试也重新 force。 */
    private static void writeReplicaIdentity(Path target, WalReplicaIdentity replica, Budget budget, ForceHook force) throws IOException {
        budget.check(); byte[] bytes = java.util.Objects.requireNonNull(replica).encode();
        try (var channel = FileChannel.open(target, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS)) {
            WalFiles.write(channel, ByteBuffer.wrap(bytes)); force.force(target, channel);
        }
    }

    /** 流式读入有限批次，实际导入后再校验文件摘要，失败时后端继续保持恢复中。 */
    private static void importData(Validated validated, WalBackupManifest.Data data, WalMaterializedStore store, WalRestoreSpec spec,
                                   Budget budget, Consumer<WalMvccRuntime.Stage> hook) throws IOException {
        var batch = new ArrayList<WalBackupRow>(); long[] bytes = {0}; var digest = sha();
        try (InputStream raw = Files.newInputStream(checked(validated.root(), data.file(), true), StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS);
             InputStream input = new java.security.DigestInputStream(raw, digest)) {
            long rows = WalBackupData.read(input, new WalBackupData.Header(spec.engineId(), spec.shard(), spec.boundary()), row -> {
                budget.check(); long size = 256L + row.key().getBytes(java.nio.charset.StandardCharsets.UTF_8).length + row.version().valueLength();
                if (size > budget.options.batchBytes()) { throw new IOException("single restore row exceeds batch budget"); }
                if (!batch.isEmpty() && (batch.size() >= budget.options.batchKeys() || bytes[0] + size > budget.options.batchBytes())) {
                    store.restoreBatch(spec, List.copyOf(batch)); hook.accept(WalMvccRuntime.Stage.AFTER_RESTORE_BATCH); batch.clear(); bytes[0] = 0;
                }
                batch.add(row); bytes[0] += size;
            });
            if (rows != data.rows() || !HexFormat.of().formatHex(digest.digest()).equals(file(validated.manifest(), data.file()).hash())) { throw new IOException("backup changed during restore"); }
        }
        if (!batch.isEmpty()) { store.restoreBatch(spec, List.copyOf(batch)); hook.accept(WalMvccRuntime.Stage.AFTER_RESTORE_BATCH); }
    }

    /** 检查无后端版本的恢复预算，不能发布一个注定因内存不足而无法启动的目录。 */
    private static void validateMemory(WalCheckpointState image, WalOptions options) throws IOException {
        java.util.Objects.requireNonNull(options); if (image == null) { return; }
        var keys = new HashSet<WalMvccRuntime.Key>(); long bytes = 0;
        for (var version : image.versions()) {
            var mutation = version.mutation(); if (image.storedShards().contains(mutation.shard())) { continue; }
            if (keys.add(new WalMvccRuntime.Key(mutation.shard(), mutation.key()))) { bytes += 128L + 2L * (mutation.shard().length() + mutation.key().length()); }
            bytes += mutation.retainedBytes(); if (bytes > options.maxMemoryBytes()) { throw new IOException("restore exceeds local version memory budget"); }
        }
    }

    /** 从不可变清单查找文件描述，缺失项不能默认为空文件。 */
    private static WalBackupManifest.Entry file(WalBackupManifest manifest, String name) throws IOException {
        return manifest.files().stream().filter(entry -> entry.path().equals(name)).findFirst().orElseThrow(() -> new IOException("missing backup file"));
    }

    /** 分块复制、校验和同步文件，续做只覆盖带有效恢复标记的自有暂存文件。 */
    private static WalBackupManifest.Entry copy(Path source, Path target, String name, Budget budget, boolean account,
                                                WalBackupManifest.Entry expected, ForceHook force) throws IOException {
        Files.createDirectories(target.getParent());
        if (expected != null && Files.exists(target, LinkOption.NOFOLLOW_LINKS) && Files.isRegularFile(target, LinkOption.NOFOLLOW_LINKS)
                && Files.size(target) == expected.size() && checksum(target, budget).equals(expected.hash())) {
            // 前次进程可能在写完但尚未刷盘时退出；摘要正确只证明内容可读，不证明它已持久化。
            try (var channel = FileChannel.open(target, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS)) { force.force(target, channel); }
            return expected;
        }
        var digest = sha(); long size = 0;
        try (var input = FileChannel.open(source, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS);
             var output = FileChannel.open(target, account ? StandardOpenOption.CREATE_NEW : StandardOpenOption.CREATE,
                     StandardOpenOption.WRITE, account ? StandardOpenOption.WRITE : StandardOpenOption.TRUNCATE_EXISTING, LinkOption.NOFOLLOW_LINKS)) {
            var buffer = ByteBuffer.allocate(64 * 1024);
            while (true) {
                budget.check(); int read = input.read(buffer); if (read < 0) { break; } if (read == 0) { throw new IOException("backup copy made no progress"); }
                if (account) { budget.add(read); } size += read; digest.update(buffer.array(), 0, read); buffer.flip(); WalFiles.write(output, buffer); buffer.clear();
            }
            force.force(target, output);
        }
        var entry = new WalBackupManifest.Entry(name, size, HexFormat.of().formatHex(digest.digest()));
        if (expected != null && !expected.equals(entry)) { throw new IOException("backup changed while copying"); }
        return entry;
    }

    /** 以固定内存计算文件摘要，单个归档可大于运行时缓存预算。 */
    private static String checksum(Path path, Budget budget) throws IOException {
        var digest = sha(); byte[] buffer = new byte[64 * 1024];
        try (var input = Files.newInputStream(path, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)) {
            while (true) { budget.check(); int read = input.read(buffer); if (read < 0) { break; } if (read == 0) { throw new IOException("backup checksum made no progress"); } digest.update(buffer, 0, read); }
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    /** 获取所有路径段都不含符号链接的文件位置，禁止越出指定根目录。 */
    private static Path checked(Path root, String relative, boolean file) throws IOException {
        Path path = root.resolve(relative).normalize(); if (!path.startsWith(root)) { throw new IOException("backup path escapes root"); }
        Path current = root;
        for (Path part : root.relativize(path)) { current = current.resolve(part); if (Files.isSymbolicLink(current)) { throw new IOException("backup path contains symbolic link"); } }
        if (file && !Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) { throw new IOException("backup file missing or not regular"); }
        return path;
    }

    /** 固定目标父目录的实际位置，目标本身必须有普通文件名。 */
    private static Path canonicalTarget(Path requested) throws IOException {
        Path path = requested.toAbsolutePath().normalize(); if (path.getFileName() == null) { throw new IOException("invalid backup target"); }
        return path.getParent().toRealPath().resolve(path.getFileName());
    }

    /** 恢复标记只允许本次完整清单接续，已有普通目录永远不能隐式覆盖。 */
    private static void requireMarker(Path root, byte[] expected) throws IOException {
        requireDirectory(root);
        if (!java.util.Arrays.equals(expected, WalFiles.readBounded(checked(root, "RESTORING", true), 256))) { throw new IOException("restore target belongs to another operation"); }
    }

    /** 对固定恢复目录拒绝符号链接和非目录。 */
    private static void requireDirectory(Path root) throws IOException {
        if (!Files.isDirectory(root, LinkOption.NOFOLLOW_LINKS)) { throw new IOException("backup directory missing or symbolic"); }
    }

    /** 子目录先于父目录同步，最后由 COMPLETE 或恢复标记清除发布可用状态。 */
    private static void forceDirectories(Path root, List<Path> parents) throws IOException {
        var directories = new HashSet<Path>(); directories.add(root);
        for (Path parent : parents) { for (Path current = parent; current != null && current.startsWith(root); current = current.getParent()) { directories.add(current); } }
        for (Path directory : directories.stream().sorted(java.util.Comparator.comparingInt(Path::getNameCount).reversed()).toList()) { WalFiles.forceDirectory(directory); }
    }

    /** 创建固定的 SHA-256 摘要器，不把环境算法缺失解释成成功。 */
    private static MessageDigest sha() throws IOException {
        try { return MessageDigest.getInstance("SHA-256"); } catch (java.security.NoSuchAlgorithmException failure) { throw new IOException(failure); }
    }

    /** 生成与已验证清单一致的公开维护结果。 */
    private static Info info(WalBackupManifest manifest, long bytes, Budget budget) {
        var rows = new HashMap<String, Long>(); for (var data : manifest.data()) { rows.put(data.shard(), data.rows()); }
        return new Info(manifest.backupId(), manifest.engineId(), manifest.boundary().sequence(), manifest.boundary().digest(),
                manifest.shards(), manifest.storedShards(), rows, bytes, TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - budget.start));
    }

    /** 在实际写出前扣减文件预算，不依赖写完后的磁盘大小检查。 */
    private static final class CountedOutput extends OutputStream {
        private final OutputStream output;
        private final Budget budget;
        /** 装配本次备份独立的输出预算。 */
        CountedOutput(OutputStream output, Budget budget) { this.output = output; this.budget = budget; }
        /** 单字节写入也不能绕过预算。 */
        @Override public void write(int value) throws IOException { budget.add(1); output.write(value); }
        /** 分块写出前检查资源，再交给摘要和磁盘流。 */
        @Override public void write(byte[] bytes, int offset, int length) throws IOException { budget.add(length); output.write(bytes, offset, length); }
        /** 刷新当前自有输出，不操作共享 WAL。 */
        @Override public void flush() throws IOException { output.flush(); }
        /** 关闭自有备份输出，不能关闭应用数据库或运行时。 */
        @Override public void close() throws IOException { output.close(); }
    }
}
