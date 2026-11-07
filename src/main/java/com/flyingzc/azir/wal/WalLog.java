package com.flyingzc.azir.wal;

import com.flyingzc.azir.common.ErrorCode;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.zip.CRC32C;

/** 单写者追加日志，完整事务独立校验，多笔事务共享一次可靠刷盘。 */
final class WalLog implements AutoCloseable {
    private static final Set<Object> LOCAL_WRITERS = java.util.concurrent.ConcurrentHashMap.newKeySet();
    /** 仅供包内故障验收使用的真实 I/O 边界，正常路径全部透传。 */
    interface IoHook {
        /** 限制单次实际写入长度，用来构造真正的部分帧。 */
        default int writeLimit(long position, int remaining) { return remaining; }
        /** 在实际写入后模拟磁盘满或设备错误。 */
        default void afterWrite(long position) throws IOException { }
        /** 在实际刷盘之前注入失败。 */
        default void beforeForce() throws IOException { }
        /** 在实际刷盘成功后注入确认丢失。 */
        default void afterForce() throws IOException { }
        /** 限制检查点文件单次真实写入，构造镜像、凭据或指针的部分写出。 */
        default int checkpointWriteLimit(String fileName, long position, int remaining) { return remaining; }
        /** 检查点文件实际写入后注入设备错误。 */
        default void afterCheckpointWrite(String fileName, long position) throws IOException { }
        /** 检查点文件刷盘前注入失败。 */
        default void beforeCheckpointForce(String fileName) throws IOException { }
        /** 检查点文件刷盘成功后模拟确认丢失。 */
        default void afterCheckpointForce(String fileName) throws IOException { }
    }
    static final IoHook NORMAL_IO = new IoHook() { };
    private static final int MAGIC = 0x415A574C;
    private static final int HEADER_SIZE = 60;
    private static final int FRAME_HEADER = 16;
    private FileChannel channel;
    private final FileChannel lockChannel;
    private final Object directoryIdentity;
    private Path path;
    private final Path root;
    private final byte[] routesHash;
    private final WalRetentionOptions retention;
    private Path generation;
    private WalCheckpointState snapshot;
    private long baseSequence;
    private WalReceipt baseBoundary;
    private long retiredThrough;
    private int headerSize = HEADER_SIZE;
    private WalBloom archivedBloom = WalBloom.allocate(0, 16 * 1024 * 1024);
    private final List<WalReceiptArchive.Descriptor> archives = new ArrayList<>();
    private final java.util.LinkedHashMap<String, WalReceiptArchive> archiveCache = new java.util.LinkedHashMap<>(16, .75f, true);
    private long archiveCacheBytes;
    private final FileLock lock;
    private final long maxBytes;
    private final List<Entry> entries = new ArrayList<>();
    private final Map<UUID, WalReceipt> receipts = new HashMap<>();
    private UUID engineId;
    private long forces;
    private long fileBytes;
    private long receiptArchiveBytes;
    private final WalReplicaIdentity replica;
    private boolean closed;
    private IoHook ioHook = NORMAL_IO;

    /** 保存轻量日志索引，写集按需读取，避免内存淘汰后仍被日志索引持有。 */
    private record Entry(long offset, int length, WalReceipt receipt) { }

    /** 独占日志文件，创建并同步身份头，或严格恢复已有日志。 */
    WalLog(Path directory, Set<String> shards, long maxBytes) throws IOException {
        this(directory, shards, maxBytes, NORMAL_IO);
    }

    /** 完成正常身份初始化和恢复后再启用注入，不把测试故障混入初始化。 */
    WalLog(Path directory, Set<String> shards, long maxBytes, IoHook ioHook) throws IOException {
        this(directory, shards, maxBytes, ioHook, WalRetentionOptions.preserveAll());
    }

    /** 在独占目录内恢复检查点代和剩余日志，旧格式在首次安全检查点前保持兼容。 */
    WalLog(Path directory, Set<String> shards, long maxBytes, IoHook ioHook, WalRetentionOptions retention) throws IOException {
        this(directory, shards, maxBytes, ioHook, retention, null);
    }

    /** 复制模式使用固定引擎及版本三/四文件头，旧程序不能按本地事务方式接管。 */
    WalLog(Path directory, Set<String> shards, long maxBytes, IoHook ioHook, WalRetentionOptions retention, WalReplicaIdentity replica) throws IOException {
        this.maxBytes = maxBytes;
        this.replica = replica;
        this.retention = java.util.Objects.requireNonNull(retention);
        if (Files.exists(directory.resolve("RESTORING"), java.nio.file.LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("WAL directory restore has not been published");
        }
        Files.createDirectories(directory);
        Path canonical = directory.toRealPath();
        Object fileKey = Files.readAttributes(canonical, java.nio.file.attribute.BasicFileAttributes.class).fileKey();
        directoryIdentity = fileKey == null ? canonical : fileKey;
        // 同 JVM 再开再关锁文件也可能释放 POSIX 锁，因此必须先阻止本进程的重复打开。
        if (!LOCAL_WRITERS.add(directoryIdentity)) { throw new IOException("WAL directory already has a local writer"); }
        root = canonical; path = canonical.resolve("transactions.wal");
        routesHash = java.util.HexFormat.of().parseHex(WalCodec.digest(
                String.join("\u0000", shards.stream().sorted().toList()).getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        FileChannel openedLock = null;
        FileChannel openedData = null;
        FileLock acquired = null;
        try {
            // 锁单独保存在另一 inode 上，关闭日志读通道不能释放写者的系统级独占锁。
            openedLock = FileChannel.open(canonical.resolve("writer.lock"), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
            acquired = openedLock.tryLock();
            if (acquired == null) { throw new IOException("WAL directory already has a writer"); }
            boolean replicaExists = Files.exists(root.resolve("REPLICA"), java.nio.file.LinkOption.NOFOLLOW_LINKS);
            if (replica == null && replicaExists) { throw new IOException("replica directory requires consensus authority"); }
            if (replica != null) {
                if (replicaExists) {
                    if (!replica.equals(WalReplicaIdentity.read(root))) { throw new IOException("replica identity mismatch"); }
                } else {
                    try (var paths = Files.list(root)) {
                        if (paths.anyMatch(file -> !file.getFileName().toString().equals("writer.lock"))) { throw new IOException("cannot adopt an existing directory as replica"); }
                    }
                }
            }
            var current = WalCheckpointFiles.read(root);
            if (current == null && (!Files.exists(root.resolve("transactions.wal"))
                    || Files.size(root.resolve("transactions.wal")) == 0) && hasRecoveryEvidence()) {
                // 已有持久身份或检查点却失去入口时，只能拒绝启动，不能创建新 UUID 后清理旧数据。
                throw new IOException("WAL or CURRENT missing from an initialized engine directory");
            }
            if (current != null) {
                snapshot = current.state(); generation = current.generation(); path = generation.resolve("tail.wal");
                if (!snapshot.shards().equals(shards)) { throw new IOException("checkpoint routes mismatch"); }
                baseBoundary = snapshot.boundary(); baseSequence = snapshot.sequence(); retiredThrough = snapshot.retiredThrough(); headerSize = 100;
                archives.addAll(snapshot.archives()); archivedBloom = new WalBloom(snapshot.archiveBloom());
            }
            openedData = current == null
                    ? FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE)
                    : FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
            lockChannel = openedLock;
            channel = openedData;
            lock = acquired;
            if (current == null && channel.size() == 0) {
                engineId = replica == null ? UUID.randomUUID() : replica.engineId(); writeFully(ByteBuffer.wrap(header(0, null))); channel.force(true);
                WalFiles.forceDirectory(root);
                for (Path parent = root.getParent(); parent != null; parent = parent.getParent()) { WalFiles.forceDirectory(parent); }
            } else {
                byte[] bytes = readBytes(0, headerSize).array(); var stored = ByteBuffer.wrap(bytes);
                int version = current == null ? 1 : 2; if (replica != null) { version += 2; }
                if (stored.getInt() != MAGIC || stored.getInt() != version) { throw new IOException("unsupported WAL header"); }
                engineId = new UUID(stored.getLong(), stored.getLong()); byte[] routes = new byte[32]; stored.get(routes);
                if (replica != null && !engineId.equals(replica.engineId())) { throw new IOException("replica engine mismatch"); }
                if (!Arrays.equals(routesHash, routes) || ByteBuffer.wrap(bytes).getInt(headerSize - 4)
                        != checksum(Arrays.copyOf(bytes, headerSize - 4))) { throw new IOException("WAL routes or header checksum mismatch"); }
                if (current != null) {
                    byte[] imageHash = new byte[32]; long base = stored.getLong(); stored.get(imageHash);
                    if (!engineId.equals(snapshot.engineId()) || base != baseSequence
                            || !java.util.HexFormat.of().formatHex(imageHash).equals(current.imageHash())) { throw new IOException("checkpoint/WAL generation mismatch"); }
                    for (var archive : archives) { WalReceiptArchive.open(root, engineId, archive); receiptArchiveBytes += Files.size(root.resolve(archive.fileName())); }
                }
            }
            Path identity = root.resolve("ENGINE"); byte[] expectedIdentity = header(0, null);
            if (Files.exists(identity, java.nio.file.LinkOption.NOFOLLOW_LINKS)
                    && (!Files.isRegularFile(identity, java.nio.file.LinkOption.NOFOLLOW_LINKS)
                    || !Arrays.equals(expectedIdentity, WalFiles.readBounded(identity, HEADER_SIZE)))) {
                throw new IOException("durable engine identity mismatch");
            }
            recover();
            if (channel.size() > maxBytes) { throw new IOException("existing WAL exceeds configured disk budget"); }
            if (!Files.exists(identity)) {
                // 标记原子创建且先于任何业务 ACK；旧格式有完整 WAL 时允许安全补写这个身份文件。
                Path temporary = root.resolve("ENGINE-" + UUID.randomUUID() + ".tmp");
                WalFiles.writeNew(temporary, expectedIdentity);
                Files.move(temporary, identity, java.nio.file.StandardCopyOption.ATOMIC_MOVE);
                WalFiles.forceDirectory(root);
            }
            if (replica != null && !replicaExists) { replica.persist(root); }
            this.ioHook = java.util.Objects.requireNonNull(ioHook);
            WalCheckpointFiles.cleanup(root, generation, archives.stream().map(WalReceiptArchive.Descriptor::fileName).collect(java.util.stream.Collectors.toSet()));
        } catch (Throwable failure) {
            try { if (openedData != null) { openedData.close(); } } catch (Throwable cleanup) { failure.addSuppressed(cleanup); }
            try { if (acquired != null && acquired.isValid()) { acquired.release(); } } catch (Throwable cleanup) { failure.addSuppressed(cleanup); }
            try { if (openedLock != null) { openedLock.close(); } } catch (Throwable cleanup) { failure.addSuppressed(cleanup); }
            LOCAL_WRITERS.remove(directoryIdentity);
            throw failure;
        }
    }

    /** 缺少恢复入口时识别已初始化目录，兼容尚未补写 ENGINE 标记的旧检查点代。 */
    private boolean hasRecoveryEvidence() throws IOException {
        if (Files.exists(root.resolve("REPLICA"), java.nio.file.LinkOption.NOFOLLOW_LINKS)) { return true; }
        if (Files.exists(root.resolve("ENGINE"), java.nio.file.LinkOption.NOFOLLOW_LINKS)) { return true; }
        try (var paths = Files.list(root)) {
            return paths.anyMatch(path -> {
                String name = path.getFileName().toString();
                return name.matches("checkpoint-[a-f0-9-]{36}") || name.matches("receipts-[a-f0-9-]{36}\\.sst")
                        || name.matches("ENGINE-[a-f0-9-]{36}\\.tmp");
            });
        }
    }

    /** 扫描连续完整事务，仅修复不完整尾部，完整记录校验失败则拒绝启动。 */
    private void recover() throws IOException {
        long offset = headerSize;
        long size = channel.size();
        while (offset < size) {
            if (size - offset < FRAME_HEADER) { break; }
            ByteBuffer prefix = readBytes(offset, FRAME_HEADER);
            if (prefix.getInt() != MAGIC || checksum(Arrays.copyOf(prefix.array(), 12)) != prefix.getInt(12)) {
                throw new IOException("WAL frame header checksum mismatch at " + offset);
            }
            int length = prefix.getInt();
            int crc = prefix.getInt();
            if (length < 36 || length > WalCodec.MAX_FRAME) { throw new IOException("invalid WAL frame length"); }
            if (size - offset - FRAME_HEADER < length) { break; }
            byte[] data = readBytes(offset + FRAME_HEADER, length).array();
            if (checksum(data) != crc) { throw new IOException("WAL checksum mismatch at " + offset); }
            WalCommit commit = WalCodec.decode(data);
            index(offset, data, commit);
            offset += FRAME_HEADER + (long) length;
        }
        if (offset != size) { channel.truncate(offset); }
        // 完整但未获调用方确认的尾部可在恢复时成为提交结果，恢复后重新确保持久性。
        channel.force(true);
        channel.position(offset);
        fileBytes = offset;
    }

    /** 原子事务逐项追加、整批刷盘；刷盘异常后调用方必须停止当前运行时。 */
    synchronized void append(List<WalCommit> commits) throws IOException {
        List<byte[]> encoded = commits.stream().map(WalCodec::encode).toList();
        long required = encoded.stream().mapToLong(bytes -> FRAME_HEADER + (long) bytes.length).sum();
        if (fileBytes + required > maxBytes) {
            throw new WalMvccException(ErrorCode.RESOURCE_EXHAUSTED, "WAL disk budget exhausted", null, null);
        }
        if ((long) entries.size() + commits.size() > retention.maxPendingReceipts()) {
            throw new WalMvccException(ErrorCode.RESOURCE_EXHAUSTED, "pending receipt budget exhausted; checkpoint required", null, null);
        }
        long expected = lastSequence() + 1;
        var transactionIds = new java.util.HashSet<UUID>();
        for (int i = 0; i < commits.size(); i++) {
            if (commits.get(i).sequence() != expected++ || receipt(commits.get(i).transactionId()).isPresent()
                    || !transactionIds.add(commits.get(i).transactionId())
                    || encoded.get(i).length > WalCodec.MAX_FRAME) { throw new IOException("invalid append sequence"); }
        }
        long offset = fileBytes;
        channel.position(offset);
        for (byte[] data : encoded) {
            ByteBuffer prefix = ByteBuffer.allocate(FRAME_HEADER).putInt(MAGIC).putInt(data.length).putInt(checksum(data));
            prefix.putInt(checksum(Arrays.copyOf(prefix.array(), 12))).flip();
            writeFully(prefix);
            writeFully(ByteBuffer.wrap(data));
        }
        ioHook.beforeForce();
        channel.force(true);
        ioHook.afterForce();
        forces++;
        for (int i = 0; i < commits.size(); i++) {
            index(offset, encoded.get(i), commits.get(i));
            offset += FRAME_HEADER + (long) encoded.get(i).length;
        }
    }

    /** 登记严格连续的持久事务索引并拒绝重复事务号。 */
    private void index(long offset, byte[] bytes, WalCommit commit) throws IOException {
        if (commit.sequence() != baseSequence + entries.size() + 1L || receipts.containsKey(commit.transactionId())) {
            throw new IOException("non-contiguous WAL or duplicate transaction id");
        }
        WalReceipt receipt = new WalReceipt(commit.transactionId(), commit.sequence(), WalCodec.digest(bytes));
        entries.add(new Entry(offset, bytes.length, receipt));
        receipts.put(commit.transactionId(), receipt);
    }

    /** 按提交序号读取有限日志批次，同时复核磁盘内容。 */
    synchronized List<WalCommit> read(long from, int limit) throws IOException {
        if (closed) { throw new java.nio.channels.ClosedChannelException(); }
        var result = new ArrayList<WalCommit>();
        // 读线程的 interrupt 会关闭其 FileChannel，必须与持有独占锁的写通道隔离。
        try (var reader = FileChannel.open(path, StandardOpenOption.READ)) {
            if (from <= baseSequence) { throw new IOException("requested WAL prefix has been checkpointed"); }
            for (long sequence = from; sequence <= lastSequence() && result.size() < limit; sequence++) {
                if (sequence < 1) { throw new IOException("invalid read sequence"); }
                Entry entry = entries.get(Math.toIntExact(sequence - baseSequence - 1));
                byte[] data = readBytes(reader, entry.offset + FRAME_HEADER, entry.length).array();
                if (!entry.receipt.digest().equals(WalCodec.digest(data))) { throw new IOException("WAL changed after recovery"); }
                result.add(WalCodec.decode(data));
            }
        }
        return result;
    }

    /** 返回持久日志身份。 */
    UUID engineId() { return engineId; }

    /** 返回已确认刷盘或已恢复的最后事务序号。 */
    synchronized long lastSequence() { return baseSequence + entries.size(); }

    /** 查询持久提交凭据，缺失仅表示没有已确认的提交记录。 */
    synchronized Optional<WalReceipt> receipt(UUID id) throws IOException {
        WalReceipt active = receipts.get(id); if (active != null) { return Optional.of(active); }
        if (!archivedBloom.mightContain(id)) { return Optional.empty(); }
        for (int i = archives.size() - 1; i >= 0; i--) {
            var found = archive(archives.get(i)).find(id); if (found.isPresent()) { return found; }
        }
        return Optional.empty();
    }

    /** 返回指定序号的稳定凭据，供检查点核对。 */
    synchronized WalReceipt receipt(long sequence) {
        if (sequence == baseSequence && baseBoundary != null) { return baseBoundary; }
        return entries.get(Math.toIntExact(sequence - baseSequence - 1)).receipt;
    }

    /** 返回当前恢复基线，低于该边界的日志只能通过快照恢复。 */
    synchronized long baseSequence() { return baseSequence; }

    /** 返回不可变恢复镜像，运行时仅在启动阶段使用。 */
    synchronized WalCheckpointState snapshot() { return snapshot; }

    /** 运行时恢复完成后释放镜像对象，旧快照不能长期额外占用一份完整版本内存。 */
    synchronized void releaseRecoveryImage() { snapshot = null; }

    /** 返回已回收凭据的最高序号，缺失 UUID 只能被解释为未知或已过窗口。 */
    synchronized long retiredThrough() { return retiredThrough; }

    /** 返回尚未从活动索引迁移到磁盘归档的凭据数量。 */
    synchronized int pendingReceipts() { return entries.size(); }

    /** 返回当前被检查点引用的凭据归档数量。 */
    synchronized int receiptArchives() { return archives.size(); }

    /** 在提交决议暂停且物化完全追平后，先持久化完整新代，再原子发布并删除旧代。 */
    synchronized void checkpoint(Set<String> shards, Set<String> storedShards, long safePoint,
                                 List<WalCheckpointState.Version> versions, long now,
                                 java.util.function.Consumer<WalMvccRuntime.Stage> hook) throws IOException {
        if (closed || lastSequence() == 0) { throw new IOException("checkpoint requires an open committed prefix"); }
        long cut = lastSequence(); WalReceipt boundary = receipt(cut);
        var retained = new ArrayList<WalReceiptArchive.Descriptor>(); long retired = retiredThrough; long diskBytes = 0;
        for (var descriptor : archives) {
            if (descriptor.expiresAtMillis() <= now) { retired = Math.max(retired, descriptor.maxSequence()); }
            else { retained.add(descriptor); diskBytes += java.nio.file.Files.size(root.resolve(descriptor.fileName())); }
        }
        for (int first = 0; first < entries.size(); first += 100_000) {
            diskBytes += WalReceiptArchive.estimatedBytes(Math.min(100_000, entries.size() - first));
        }
        if (diskBytes > retention.maxReceiptArchiveBytes() || retained.size() + ((long) entries.size() + 99_999) / 100_000 > 100_000) {
            throw new WalMvccException(ErrorCode.RESOURCE_EXHAUSTED, "receipt archive disk budget exhausted", null, null);
        }
        long plannedReceipts = retained.stream().mapToLong(WalReceiptArchive.Descriptor::count).sum() + entries.size();
        int plannedArchives = retained.size() + (entries.size() + 99_999) / 100_000;
        WalCheckpointState.checkEncodingBudget(shards, storedShards, versions, plannedArchives, WalBloom.capacity(plannedReceipts, 16 * 1024 * 1024));
        hook.accept(WalMvccRuntime.Stage.BEFORE_CHECKPOINT_FILES);
        long expires = retention.receiptRetentionMillis() == 0 || now > Long.MAX_VALUE - retention.receiptRetentionMillis()
                ? Long.MAX_VALUE : now + retention.receiptRetentionMillis();
        var recent = entries.stream().map(Entry::receipt).toList();
        for (int first = 0; first < recent.size(); first += 100_000) {
            retained.add(WalReceiptArchive.create(root, engineId, recent.subList(first, Math.min(first + 100_000, recent.size())), expires, ioHook));
        }
        hook.accept(WalMvccRuntime.Stage.AFTER_RECEIPTS_FORCE);
        long count = retained.stream().mapToLong(WalReceiptArchive.Descriptor::count).sum();
        var filter = WalBloom.allocate(count, 16 * 1024 * 1024);
        if (retired == retiredThrough && filter.byteSize() == archivedBloom.byteSize()) {
            filter = new WalBloom(archivedBloom.bytes()); for (var receipt : recent) { filter.add(receipt.transactionId()); }
        } else {
            WalBloom target = filter;
            for (var descriptor : retained) { archive(descriptor).forEach(receipt -> target.add(receipt.transactionId())); }
        }
        var image = new WalCheckpointState(engineId, shards, storedShards, cut, safePoint, boundary,
                versions, retained, filter.bytes(), retired);
        byte[] encoded = image.encode(); String digest = WalCodec.digest(encoded);
        UUID identity = UUID.randomUUID(); Path next = root.resolve("checkpoint-" + identity); Files.createDirectory(next);
        WalFiles.writeNew(next.resolve("snapshot.bin"), encoded, ioHook); hook.accept(WalMvccRuntime.Stage.AFTER_SNAPSHOT_FORCE);
        WalFiles.writeNew(next.resolve("tail.wal"), header(cut, digest), ioHook); WalFiles.forceDirectory(next); WalFiles.forceDirectory(root);
        Path pointer = WalCheckpointFiles.writePointer(root, identity, digest, ioHook);
        hook.accept(WalMvccRuntime.Stage.BEFORE_CURRENT_MOVE);
        Files.move(pointer, root.resolve("CURRENT"), java.nio.file.StandardCopyOption.ATOMIC_MOVE, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        hook.accept(WalMvccRuntime.Stage.AFTER_CURRENT_MOVE); WalFiles.forceDirectory(root);
        hook.accept(WalMvccRuntime.Stage.AFTER_CURRENT_FORCE);
        // 新代发布持久化之前，旧代和全部旧归档始终保留；失败必须由运行时停止写入。
        channel.close(); generation = next; path = next.resolve("tail.wal");
        channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE); headerSize = 100; fileBytes = headerSize;
        snapshot = null; baseBoundary = boundary; baseSequence = cut; retiredThrough = retired; archives.clear(); archives.addAll(retained);
        archivedBloom = filter; entries.clear(); receipts.clear(); archiveCache.clear(); archiveCacheBytes = 0;
        receiptArchiveBytes = diskBytes;
        hook.accept(WalMvccRuntime.Stage.BEFORE_OLD_GENERATION_DELETE);
        WalCheckpointFiles.cleanup(root, generation, retained.stream().map(WalReceiptArchive.Descriptor::fileName).collect(java.util.stream.Collectors.toSet()));
        hook.accept(WalMvccRuntime.Stage.AFTER_OLD_GENERATION_DELETE);
    }

    /** 过滤器缓存有固定上限，历史事务不会全部重新进入内存。 */
    private WalReceiptArchive archive(WalReceiptArchive.Descriptor descriptor) throws IOException {
        var result = archiveCache.get(descriptor.fileName());
        if (result == null) {
            result = WalReceiptArchive.open(root, engineId, descriptor); archiveCache.put(descriptor.fileName(), result);
            archiveCacheBytes += result.memoryBytes();
            var iterator = archiveCache.entrySet().iterator();
            while (archiveCacheBytes > 8L * 1024 * 1024 && iterator.hasNext()) {
                var oldest = iterator.next(); archiveCacheBytes -= oldest.getValue().memoryBytes(); iterator.remove();
            }
        }
        return result;
    }

    /** 旧格式保留 60 字节头，新代将恢复边界和镜像摘要写入 100 字节头。 */
    private byte[] header(long base, String imageHash) {
        int version = imageHash == null ? 1 : 2; if (replica != null) { version += 2; }
        var result = ByteBuffer.allocate(imageHash == null ? HEADER_SIZE : 100).putInt(MAGIC).putInt(version)
                .putLong(engineId.getMostSignificantBits()).putLong(engineId.getLeastSignificantBits()).put(routesHash);
        if (imageHash != null) { result.putLong(base).put(java.util.HexFormat.of().parseHex(imageHash)); }
        result.putInt(checksum(Arrays.copyOf(result.array(), result.capacity() - 4))); return result.array();
    }

    /** 返回当前追加批次的刷盘次数，不包含初始化与恢复刷盘。 */
    synchronized long forces() { return forces; }

    /** 返回包含文件头的磁盘使用量。 */
    synchronized long bytes() throws IOException { return fileBytes; }

    /** 返回已验证的归档物理字节，健康查询不重新扫描文件系统。 */
    synchronized long archiveBytes() { return receiptArchiveBytes; }

    /** 在暂停提交和检查点期间列出恢复必需的稳定文件，不复制锁文件或未知用户文件。 */
    synchronized Map<String, Path> backupFiles() throws IOException {
        if (closed || !entries.isEmpty() || generation == null && lastSequence() != 0) { throw new IOException("backup requires a checkpointed prefix"); }
        var files = new java.util.LinkedHashMap<String, Path>(); files.put("local/ENGINE", root.resolve("ENGINE"));
        if (replica != null) { files.put("local/REPLICA", root.resolve("REPLICA")); }
        if (generation == null) { files.put("local/transactions.wal", path); }
        else {
            files.put("local/CURRENT", root.resolve("CURRENT"));
            String prefix = "local/" + generation.getFileName() + "/";
            files.put(prefix + "snapshot.bin", generation.resolve("snapshot.bin")); files.put(prefix + "tail.wal", generation.resolve("tail.wal"));
            for (var archive : archives) { files.put("local/" + archive.fileName(), root.resolve(archive.fileName())); }
        }
        return Map.copyOf(files);
    }

    /** 返回独占的规范目录，备份目标不得位于正在运行的恢复文件树中。 */
    Path directory() { return root; }

    /** 完整快照清单绑定源副本身份，不能将复制日志误标为普通单机备份。 */
    WalReplicaIdentity replicaIdentity() { return replica; }

    /** 读取指定位置的完整字段。 */
    private ByteBuffer readBytes(long offset, int length) throws IOException {
        return readBytes(channel, offset, length);
    }

    /** 在指定独立通道读取完整字段，读线程取消不能关闭持锁写通道。 */
    private static ByteBuffer readBytes(FileChannel input, long offset, int length) throws IOException {
        ByteBuffer bytes = ByteBuffer.allocate(length);
        while (bytes.hasRemaining()) {
            int read = input.read(bytes, offset + bytes.position());
            if (read <= 0) { throw new IOException("truncated WAL"); }
        }
        return bytes.flip();
    }

    /** 写完整个缓冲区，拒绝无进展写入。 */
    private void writeFully(ByteBuffer bytes) throws IOException {
        while (bytes.hasRemaining()) {
            long position = channel.position();
            int limit = ioHook.writeLimit(position, bytes.remaining());
            if (limit < 1 || limit > bytes.remaining()) { throw new IOException("invalid I/O write limit"); }
            int originalLimit = bytes.limit();
            bytes.limit(bytes.position() + limit);
            try {
                int written = channel.write(bytes);
                if (written <= 0) { throw new IOException("WAL write made no progress"); }
                fileBytes = Math.max(fileBytes, position + written);
            } finally { bytes.limit(originalLimit); }
            ioHook.afterWrite(fileBytes);
        }
    }

    /** 计算帧和文件头的校验值。 */
    private static int checksum(byte[] data) {
        var crc = new CRC32C();
        crc.update(data);
        return (int) crc.getValue();
    }

    /** 在工作线程退出后释放文件锁和文件描述符。 */
    @Override public synchronized void close() throws IOException {
        if (closed) { return; }
        closed = true;
        try { channel.close(); }
        finally {
            try { if (lock.isValid()) { lock.release(); } }
            finally { try { lockChannel.close(); } finally { LOCAL_WRITERS.remove(directoryIdentity); } }
        }
    }
}
