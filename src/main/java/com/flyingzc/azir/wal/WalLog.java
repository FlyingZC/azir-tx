/*
 * Copyright 2026 Azir TX contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

/**
 * Owns the local durable transaction log, record validation and exclusive directory access.
 */
final class WalLog implements AutoCloseable {

    private static final Set<Object> LOCAL_WRITERS = java.util.concurrent.ConcurrentHashMap.newKeySet();

    static final IoHook NORMAL_IO = new IoHook() {
    };

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

    /**
     * Monochrome log files, create and sync identity headers, or strict recovery already exists.
     *
     * @param directory directory
     * @param shards shards
     * @param maxBytes max bytes
     * @throws IOException if the operation cannot complete
     */
    WalLog(final Path directory, final Set<String> shards, final long maxBytes) throws IOException {
        this(directory, shards, maxBytes, NORMAL_IO);
    }

    /**
     * Enables injection after normal identity initialization and recovery is completed, without mixing test failure into initialisation.
     *
     * @param directory directory
     * @param shards shards
     * @param maxBytes max bytes
     * @param ioHook io hook
     * @throws IOException if the operation cannot complete
     */
    WalLog(final Path directory, final Set<String> shards, final long maxBytes, final IoHook ioHook) throws IOException {
        this(directory, shards, maxBytes, ioHook, WalRetentionOptions.preserveAll());
    }

    /**
     * In the exclusive directory recovery checkpoint generation and the remaining logs, the old format is compatible until the first security check point.
     *
     * @param directory directory
     * @param shards shards
     * @param maxBytes max bytes
     * @param ioHook io hook
     * @param retention retention
     * @throws IOException if the operation cannot complete
     */
    WalLog(final Path directory, final Set<String> shards, final long maxBytes, final IoHook ioHook, final WalRetentionOptions retention) throws IOException {
        this(directory, shards, maxBytes, ioHook, retention, null);
    }

    /**
     * The copy mode uses a fixed engine and version 3/4 file header, and the old program cannot be taken over by local transaction.
     *
     * @param directory directory
     * @param shards shards
     * @param maxBytes max bytes
     * @param ioHook io hook
     * @param retention retention
     * @param replica replica
     * @throws IOException if the operation cannot complete
     */
    WalLog(final Path directory, final Set<String> shards, final long maxBytes, final IoHook ioHook, final WalRetentionOptions retention, final WalReplicaIdentity replica) throws IOException {
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
        // The POSIX lock may also be released with JVM, so it is important to prevent the re-opening of this process.
        if (!LOCAL_WRITERS.add(directoryIdentity)) {
            throw new IOException("WAL directory already has a local writer");
        }
        root = canonical;
        path = canonical.resolve("transactions.wal");
        routesHash = java.util.HexFormat.of().parseHex(WalCodec.digest(
                String.join("\u0000", shards.stream().sorted().toList()).getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        FileChannel openedLock = null;
        FileChannel openedData = null;
        FileLock acquired = null;
        try {
            // The lock is kept on another inode, and the system-level lock that closes log reading channels does not release the writer.
            openedLock = FileChannel.open(canonical.resolve("writer.lock"), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
            acquired = openedLock.tryLock();
            if (acquired == null) {
                throw new IOException("WAL directory already has a writer");
            }
            boolean replicaExists = Files.exists(root.resolve("REPLICA"), java.nio.file.LinkOption.NOFOLLOW_LINKS);
            if (replica == null && replicaExists) {
                throw new IOException("replica directory requires consensus authority");
            }
            if (replica != null) {
                if (replicaExists) {
                    if (!replica.equals(WalReplicaIdentity.read(root))) {
                        throw new IOException("replica identity mismatch");
                    }
                } else {
                    try (var paths = Files.list(root)) {
                        if (paths.anyMatch(file -> !file.getFileName().toString().equals("writer.lock"))) {
                            throw new IOException("cannot adopt an existing directory as replica");
                        }
                    }
                }
            }
            var current = WalCheckpointFiles.read(root);
            if (current == null && (!Files.exists(root.resolve("transactions.wal"))
                    || Files.size(root.resolve("transactions.wal")) == 0) && hasRecoveryEvidence()) {
                // When durable identity or checkpoints have lost access, you can only refuse to start and not clean old data after creating a new UUID.
                throw new IOException("WAL or CURRENT missing from an initialized engine directory");
            }
            if (current != null) {
                snapshot = current.state();
                generation = current.generation();
                path = generation.resolve("tail.wal");
                if (!snapshot.shards().equals(shards)) {
                    throw new IOException("checkpoint routes mismatch");
                }
                baseBoundary = snapshot.boundary();
                baseSequence = snapshot.sequence();
                retiredThrough = snapshot.retiredThrough();
                headerSize = 100;
                archives.addAll(snapshot.archives());
                archivedBloom = new WalBloom(snapshot.archiveBloom());
            }
            openedData = current == null
                    ? FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE)
                    : FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
            lockChannel = openedLock;
            channel = openedData;
            lock = acquired;
            if (current == null && channel.size() == 0) {
                engineId = replica == null ? UUID.randomUUID() : replica.engineId();
                writeFully(ByteBuffer.wrap(header(0, null)));
                channel.force(true);
                WalFiles.forceDirectory(root);
                for (Path parent = root.getParent(); parent != null; parent = parent.getParent()) {
                    WalFiles.forceDirectory(parent);
                }
            } else {
                byte[] bytes = readBytes(0, headerSize).array();
                var stored = ByteBuffer.wrap(bytes);
                int version = current == null ? 1 : 2;
                if (replica != null) {
                    version += 2;
                }
                if (stored.getInt() != MAGIC || stored.getInt() != version) {
                    throw new IOException("unsupported WAL header");
                }
                engineId = new UUID(stored.getLong(), stored.getLong());
                byte[] routes = new byte[32];
                stored.get(routes);
                if (replica != null && !engineId.equals(replica.engineId())) {
                    throw new IOException("replica engine mismatch");
                }
                if (!Arrays.equals(routesHash, routes) || ByteBuffer.wrap(bytes).getInt(headerSize - 4) != checksum(Arrays.copyOf(bytes, headerSize - 4))) {
                    throw new IOException("WAL routes or header checksum mismatch");
                }
                if (current != null) {
                    byte[] imageHash = new byte[32];
                    long base = stored.getLong();
                    stored.get(imageHash);
                    if (!engineId.equals(snapshot.engineId()) || base != baseSequence
                            || !java.util.HexFormat.of().formatHex(imageHash).equals(current.imageHash())) {
                        throw new IOException("checkpoint/WAL generation mismatch");
                    }
                    for (var archive : archives) {
                        WalReceiptArchive.open(root, engineId, archive);
                        receiptArchiveBytes += Files.size(root.resolve(archive.fileName()));
                    }
                }
            }
            Path identity = root.resolve("ENGINE");
            byte[] expectedIdentity = header(0, null);
            if (Files.exists(identity, java.nio.file.LinkOption.NOFOLLOW_LINKS)
                    && (!Files.isRegularFile(identity, java.nio.file.LinkOption.NOFOLLOW_LINKS)
                            || !Arrays.equals(expectedIdentity, WalFiles.readBounded(identity, HEADER_SIZE)))) {
                throw new IOException("durable engine identity mismatch");
            }
            recover();
            if (channel.size() > maxBytes) {
                throw new IOException("existing WAL exceeds configured disk budget");
            }
            if (!Files.exists(identity)) {
                // Mark atomic creates and predates any business ACK; securely write this identity file when the old format has a complete WAL.
                Path temporary = root.resolve("ENGINE-" + UUID.randomUUID() + ".tmp");
                WalFiles.writeNew(temporary, expectedIdentity);
                Files.move(temporary, identity, java.nio.file.StandardCopyOption.ATOMIC_MOVE);
                WalFiles.forceDirectory(root);
            }
            if (replica != null && !replicaExists) {
                replica.persist(root);
            }
            this.ioHook = java.util.Objects.requireNonNull(ioHook);
            WalCheckpointFiles.cleanup(root, generation, archives.stream().map(WalReceiptArchive.Descriptor::fileName).collect(java.util.stream.Collectors.toSet()));
        } catch (final Throwable failure) {
            try {
                if (openedData != null) {
                    openedData.close();
                }
            } catch (final Throwable cleanup) {
                failure.addSuppressed(cleanup);
            }
            try {
                if (acquired != null && acquired.isValid()) {
                    acquired.release();
                }
            } catch (final Throwable cleanup) {
                failure.addSuppressed(cleanup);
            }
            try {
                if (openedLock != null) {
                    openedLock.close();
                }
            } catch (final Throwable cleanup) {
                failure.addSuppressed(cleanup);
            }
            LOCAL_WRITERS.remove(directoryIdentity);
            throw failure;
        }
    }

    /**
     * Identification of initialised directories when recovery access points are missing and compatible with old checkpoints that have not completed EGNINE tags.
     *
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    private boolean hasRecoveryEvidence() throws IOException {
        if (Files.exists(root.resolve("REPLICA"), java.nio.file.LinkOption.NOFOLLOW_LINKS)) {
            return true;
        }
        if (Files.exists(root.resolve("ENGINE"), java.nio.file.LinkOption.NOFOLLOW_LINKS)) {
            return true;
        }
        try (var paths = Files.list(root)) {
            return paths.anyMatch(path -> {
                String name = path.getFileName().toString();
                return name.matches("checkpoint-[a-f0-9-]{36}") || name.matches("receipts-[a-f0-9-]{36}\\.sst")
                        || name.matches("ENGINE-[a-f0-9-]{36}\\.tmp");
            });
        }
    }

    /**
     * Scan continuous transaction complete, only fix incomplete tails, and full record validate failed to start.
     *
     * @throws IOException if the operation cannot complete
     */
    private void recover() throws IOException {
        long offset = headerSize;
        long size = channel.size();
        while (offset < size) {
            if (size - offset < FRAME_HEADER) {
                break;
            }
            ByteBuffer prefix = readBytes(offset, FRAME_HEADER);
            if (prefix.getInt() != MAGIC || checksum(Arrays.copyOf(prefix.array(), 12)) != prefix.getInt(12)) {
                throw new IOException("WAL frame header checksum mismatch at " + offset);
            }
            int length = prefix.getInt();
            int crc = prefix.getInt();
            if (length < 36 || length > WalCodec.MAX_FRAME) {
                throw new IOException("invalid WAL frame length");
            }
            if (size - offset - FRAME_HEADER < length) {
                break;
            }
            byte[] data = readBytes(offset + FRAME_HEADER, length).array();
            if (checksum(data) != crc) {
                throw new IOException("WAL checksum mismatch at " + offset);
            }
            WalCommit commit = WalCodec.decode(data);
            index(offset, data, commit);
            offset += FRAME_HEADER + (long) length;
        }
        if (offset != size) {
            channel.truncate(offset);
        }
        // Full but unconfirmed end of the caller can be commit at recovery, and recovery will re-establish durable sex.
        channel.force(true);
        channel.position(offset);
        fileBytes = offset;
    }

    /**
     * Atomic transaction Adds, batches and brushes item by item; the caller must stop the current run after the brush is abnormal.
     *
     * @param commits commits
     * @throws IOException if the operation cannot complete
     * @throws WalMvccException if the operation cannot complete
     */
    synchronized void append(final List<WalCommit> commits) throws IOException {
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
                    || encoded.get(i).length > WalCodec.MAX_FRAME) {
                throw new IOException("invalid append sequence");
            }
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

    /**
     * Register a strictly continuous durable transaction index and refuse to repeat transaction ID.
     *
     * @param offset offset
     * @param bytes bytes
     * @param commit commit
     * @throws IOException if the operation cannot complete
     */
    private void index(final long offset, final byte[] bytes, final WalCommit commit) throws IOException {
        if (commit.sequence() != baseSequence + entries.size() + 1L || receipts.containsKey(commit.transactionId())) {
            throw new IOException("non-contiguous WAL or duplicate transaction id");
        }
        WalReceipt receipt = new WalReceipt(commit.transactionId(), commit.sequence(), WalCodec.digest(bytes));
        entries.add(new Entry(offset, bytes.length, receipt));
        receipts.put(commit.transactionId(), receipt);
    }

    /**
     * Read the limited log batch with a commit serial number while reviewing the disk contents.
     *
     * @param from from
     * @param limit maximum number of items
     * @return the operation result
     * @throws IOException if the operation cannot complete
     * @throws java.nio.channels.ClosedChannelException if the operation cannot complete
     */
    synchronized List<WalCommit> read(final long from, final int limit) throws IOException {
        if (closed) {
            throw new java.nio.channels.ClosedChannelException();
        }
        var result = new ArrayList<WalCommit>();
        // Read the threads, interrupt will shut it off at File Channel and must be isolated from a writing channel that holds an exclusive lock.
        try (var reader = FileChannel.open(path, StandardOpenOption.READ)) {
            if (from <= baseSequence) {
                throw new IOException("requested WAL prefix has been checkpointed");
            }
            for (long sequence = from; sequence <= lastSequence() && result.size() < limit; sequence++) {
                if (sequence < 1) {
                    throw new IOException("invalid read sequence");
                }
                Entry entry = entries.get(Math.toIntExact(sequence - baseSequence - 1));
                byte[] data = readBytes(reader, entry.offset + FRAME_HEADER, entry.length).array();
                if (!entry.receipt.digest().equals(WalCodec.digest(data))) {
                    throw new IOException("WAL changed after recovery");
                }
                result.add(WalCodec.decode(data));
            }
        }
        return result;
    }

    /**
     * Returns durable log identity.
     *
     * @return the operation result
     */
    UUID engineId() {
        return engineId;
    }

    /**
     * Returns the last transaction serial number of a brushed or confirmed recovery.
     *
     * @return the operation result
     */
    synchronized long lastSequence() {
        return baseSequence + entries.size();
    }

    /**
     * Query durable commit receipt, missing only means that there are no confirmed commit records.
     *
     * @param id id
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    synchronized Optional<WalReceipt> receipt(final UUID id) throws IOException {
        WalReceipt active = receipts.get(id);
        if (active != null) {
            return Optional.of(active);
        }
        if (!archivedBloom.mightContain(id)) {
            return Optional.empty();
        }
        for (int i = archives.size() - 1; i >= 0; i--) {
            var found = archive(archives.get(i)).find(id);
            if (found.isPresent()) {
                return found;
            }
        }
        return Optional.empty();
    }

    /**
     * Returns the stability of a given serial number receipt for checking points.
     *
     * @param sequence sequence
     * @return the operation result
     */
    synchronized WalReceipt receipt(final long sequence) {
        if (sequence == baseSequence && baseBoundary != null) {
            return baseBoundary;
        }
        return entries.get(Math.toIntExact(sequence - baseSequence - 1)).receipt;
    }

    /**
     * Returns the current recovery baseline, and only logs below this boundary can pass snapshot recovery.
     *
     * @return the operation result
     */
    synchronized long baseSequence() {
        return baseSequence;
    }

    /**
     * Returns the immutable recovery mirror, which is only used at start-up.
     *
     * @return the operation result
     */
    synchronized WalCheckpointState snapshot() {
        return snapshot;
    }

    /**
     * Releases the mirror object when running recovery is finished. The old snapshot cannot permanently occupy a full version memory.
     */
    synchronized void releaseRecoveryImage() {
        snapshot = null;
    }

    /**
     * Returns the highest serial number of collection receipt, the missing UUID can only be interpreted as unknown or pasted window.
     *
     * @return the operation result
     */
    synchronized long retiredThrough() {
        return retiredThrough;
    }

    /**
     * Returns the number of receipt s that have not been migrated from active index to disk archive.
     *
     * @return the operation result
     */
    synchronized int pendingReceipts() {
        return entries.size();
    }

    /**
     * Returns the number of receipt archives currently cited at the checkpoint.
     *
     * @return the operation result
     */
    synchronized int receiptArchives() {
        return archives.size();
    }

    /**
     * After the commit decision pause and materialization is fully recovered, the durable storage complete new generation is followed by the atomic release and deletion of the old
     * generation.
     *
     * @param shards shards
     * @param storedShards stored shards
     * @param safePoint oldest retained snapshot timestamp
     * @param versions versions
     * @param now now
     * @param hook hook
     * @throws IOException if the operation cannot complete
     * @throws WalMvccException if the operation cannot complete
     */
    synchronized void checkpoint(final Set<String> shards, final Set<String> storedShards, final long safePoint,
                                 final List<WalCheckpointState.Version> versions, final long now,
                                 final java.util.function.Consumer<WalMvccRuntime.Stage> hook) throws IOException {
        if (closed || lastSequence() == 0) {
            throw new IOException("checkpoint requires an open committed prefix");
        }
        long cut = lastSequence();
        WalReceipt boundary = receipt(cut);
        var retained = new ArrayList<WalReceiptArchive.Descriptor>();
        long retired = retiredThrough;
        long diskBytes = 0;
        for (var descriptor : archives) {
            if (descriptor.expiresAtMillis() <= now) {
                retired = Math.max(retired, descriptor.maxSequence());
            } else {
                retained.add(descriptor);
                diskBytes += java.nio.file.Files.size(root.resolve(descriptor.fileName()));
            }
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
                ? Long.MAX_VALUE
                : now + retention.receiptRetentionMillis();
        var recent = entries.stream().map(Entry::receipt).toList();
        for (int first = 0; first < recent.size(); first += 100_000) {
            retained.add(WalReceiptArchive.create(root, engineId, recent.subList(first, Math.min(first + 100_000, recent.size())), expires, ioHook));
        }
        hook.accept(WalMvccRuntime.Stage.AFTER_RECEIPTS_FORCE);
        long count = retained.stream().mapToLong(WalReceiptArchive.Descriptor::count).sum();
        var filter = WalBloom.allocate(count, 16 * 1024 * 1024);
        if (retired == retiredThrough && filter.byteSize() == archivedBloom.byteSize()) {
            filter = new WalBloom(archivedBloom.bytes());
            for (var receipt : recent) {
                filter.add(receipt.transactionId());
            }
        } else {
            WalBloom target = filter;
            for (var descriptor : retained) {
                archive(descriptor).forEach(receipt -> target.add(receipt.transactionId()));
            }
        }
        var image = new WalCheckpointState(engineId, shards, storedShards, cut, safePoint, boundary,
                versions, retained, filter.bytes(), retired);
        byte[] encoded = image.encode();
        String digest = WalCodec.digest(encoded);
        UUID identity = UUID.randomUUID();
        Path next = root.resolve("checkpoint-" + identity);
        Files.createDirectory(next);
        WalFiles.writeNew(next.resolve("snapshot.bin"), encoded, ioHook);
        hook.accept(WalMvccRuntime.Stage.AFTER_SNAPSHOT_FORCE);
        WalFiles.writeNew(next.resolve("tail.wal"), header(cut, digest), ioHook);
        WalFiles.forceDirectory(next);
        WalFiles.forceDirectory(root);
        Path pointer = WalCheckpointFiles.writePointer(root, identity, digest, ioHook);
        hook.accept(WalMvccRuntime.Stage.BEFORE_CURRENT_MOVE);
        Files.move(pointer, root.resolve("CURRENT"), java.nio.file.StandardCopyOption.ATOMIC_MOVE, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        hook.accept(WalMvccRuntime.Stage.AFTER_CURRENT_MOVE);
        WalFiles.forceDirectory(root);
        hook.accept(WalMvccRuntime.Stage.AFTER_CURRENT_FORCE);
        // The old and all older archives are kept until durable storage is released by the new generation; failure must be stopped while running.
        channel.close();
        generation = next;
        path = next.resolve("tail.wal");
        channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
        headerSize = 100;
        fileBytes = headerSize;
        snapshot = null;
        baseBoundary = boundary;
        baseSequence = cut;
        retiredThrough = retired;
        archives.clear();
        archives.addAll(retained);
        archivedBloom = filter;
        entries.clear();
        receipts.clear();
        archiveCache.clear();
        archiveCacheBytes = 0;
        receiptArchiveBytes = diskBytes;
        hook.accept(WalMvccRuntime.Stage.BEFORE_OLD_GENERATION_DELETE);
        WalCheckpointFiles.cleanup(root, generation, retained.stream().map(WalReceiptArchive.Descriptor::fileName).collect(java.util.stream.Collectors.toSet()));
        hook.accept(WalMvccRuntime.Stage.AFTER_OLD_GENERATION_DELETE);
    }

    /**
     * Filter cache has a fixed limit, history transaction will not re-enter the memory.
     *
     * @param descriptor descriptor
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    private WalReceiptArchive archive(final WalReceiptArchive.Descriptor descriptor) throws IOException {
        var result = archiveCache.get(descriptor.fileName());
        if (result == null) {
            result = WalReceiptArchive.open(root, engineId, descriptor);
            archiveCache.put(descriptor.fileName(), result);
            archiveCacheBytes += result.memoryBytes();
            var iterator = archiveCache.entrySet().iterator();
            while (archiveCacheBytes > 8L * 1024 * 1024 && iterator.hasNext()) {
                var oldest = iterator.next();
                archiveCacheBytes -= oldest.getValue().memoryBytes();
                iterator.remove();
            }
        }
        return result;
    }

    /**
     * The old format retains 60 byte headers, and the new generation writes recovery boundaries and mirror summaries to 100 bytes.
     *
     * @param base base
     * @param imageHash image hash
     * @return the operation result
     */
    private byte[] header(final long base, final String imageHash) {
        int version = imageHash == null ? 1 : 2;
        if (replica != null) {
            version += 2;
        }
        var result = ByteBuffer.allocate(imageHash == null ? HEADER_SIZE : 100).putInt(MAGIC).putInt(version)
                .putLong(engineId.getMostSignificantBits()).putLong(engineId.getLeastSignificantBits()).put(routesHash);
        if (imageHash != null) {
            result.putLong(base).put(java.util.HexFormat.of().parseHex(imageHash));
        }
        result.putInt(checksum(Arrays.copyOf(result.array(), result.capacity() - 4)));
        return result.array();
    }

    /**
     * Returns the current number of brushes added to batch, without initialization and recovery brushes.
     *
     * @return the operation result
     */
    synchronized long forces() {
        return forces;
    }

    /**
     * Returns the amount of disk that contains the file header.
     *
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    synchronized long bytes() throws IOException {
        return fileBytes;
    }

    /**
     * Returns an archived physical byte of verify, and health queries do not rescan the file system.
     *
     * @return the operation result
     */
    synchronized long archiveBytes() {
        return receiptArchiveBytes;
    }

    /**
     * Lists the recovery required stability files during pause commit and checkpoint, without copying lock files or unknown user files.
     *
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    synchronized Map<String, Path> backupFiles() throws IOException {
        if (closed || !entries.isEmpty() || generation == null && lastSequence() != 0) {
            throw new IOException("backup requires a checkpointed prefix");
        }
        var files = new java.util.LinkedHashMap<String, Path>();
        files.put("local/ENGINE", root.resolve("ENGINE"));
        if (replica != null) {
            files.put("local/REPLICA", root.resolve("REPLICA"));
        }
        if (generation == null) {
            files.put("local/transactions.wal", path);
        } else {
            files.put("local/CURRENT", root.resolve("CURRENT"));
            String prefix = "local/" + generation.getFileName() + "/";
            files.put(prefix + "snapshot.bin", generation.resolve("snapshot.bin"));
            files.put(prefix + "tail.wal", generation.resolve("tail.wal"));
            for (var archive : archives) {
                files.put("local/" + archive.fileName(), root.resolve(archive.fileName()));
            }
        }
        return Map.copyOf(files);
    }

    /**
     * Returns a unique directory, and the backup target cannot be located in a running recovery file tree.
     *
     * @return the operation result
     */
    Path directory() {
        return root;
    }

    /**
     * The complete snapshot list binds the source replica identity and cannot mislabel a copy log as an ordinary stand-alone backup.
     *
     * @return the operation result
     */
    WalReplicaIdentity replicaIdentity() {
        return replica;
    }

    /**
     * Reads the full field of the given location.
     *
     * @param offset offset
     * @param length length
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    private ByteBuffer readBytes(final long offset, final int length) throws IOException {
        return readBytes(channel, offset, length);
    }

    /**
     * Reads the full field at the specified independent channel. The read thread cancels to close the locked writing channel.
     *
     * @param input input
     * @param offset offset
     * @param length length
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    private static ByteBuffer readBytes(final FileChannel input, final long offset, final int length) throws IOException {
        ByteBuffer bytes = ByteBuffer.allocate(length);
        while (bytes.hasRemaining()) {
            int read = input.read(bytes, offset + bytes.position());
            if (read <= 0) {
                throw new IOException("truncated WAL");
            }
        }
        return bytes.flip();
    }

    /**
     * Write a complete buffer area and refuse to write without progress.
     *
     * @param bytes bytes
     * @throws IOException if the operation cannot complete
     */
    private void writeFully(final ByteBuffer bytes) throws IOException {
        while (bytes.hasRemaining()) {
            long position = channel.position();
            int limit = ioHook.writeLimit(position, bytes.remaining());
            if (limit < 1 || limit > bytes.remaining()) {
                throw new IOException("invalid I/O write limit");
            }
            int originalLimit = bytes.limit();
            bytes.limit(bytes.position() + limit);
            try {
                int written = channel.write(bytes);
                if (written <= 0) {
                    throw new IOException("WAL write made no progress");
                }
                fileBytes = Math.max(fileBytes, position + written);
            } finally {
                bytes.limit(originalLimit);
            }
            ioHook.afterWrite(fileBytes);
        }
    }

    /**
     * Calculates the validate value for frames and file headers.
     *
     * @param data data
     * @return the operation result
     */
    private static int checksum(final byte[] data) {
        var crc = new CRC32C();
        crc.update(data);
        return (int) crc.getValue();
    }

    /**
     * Releases the file lock and file description after the workspace exit.
     *
     * @throws IOException if the operation cannot complete
     */
    @Override
    public synchronized void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        try {
            channel.close();
        } finally {
            try {
                if (lock.isValid()) {
                    lock.release();
                }
            } finally {
                try {
                    lockChannel.close();
                } finally {
                    LOCAL_WRITERS.remove(directoryIdentity);
                }
            }
        }
    }

    /**
     * The real I/O border for the failure acceptance only in the package, which is fully passed through the normal path.
     */
    interface IoHook {

        /**
         * Limits the actual length of a single time to construct the true partial frame.
         *
         * @param position position
         * @param remaining remaining
         * @return the operation result
         */
        default int writeLimit(final long position, final int remaining) {
            return remaining;
        }

        /**
         * Simulates a disk full or device error after the actual writing.
         *
         * @param position position
         * @throws IOException if the operation cannot complete
         */
        default void afterWrite(final long position) throws IOException {
        }

        /**
         * The injection failed before the actual brush.
         *
         * @throws IOException if the operation cannot complete
         */
        default void beforeForce() throws IOException {
        }

        /**
         * After the actual brush was successfully injected to confirm loss.
         *
         * @throws IOException if the operation cannot complete
         */
        default void afterForce() throws IOException {
        }

        /**
         * Limits the part of a check point file that is written in real time, to construct a mirror, receipt or pointer.
         *
         * @param fileName file name
         * @param position position
         * @param remaining remaining
         * @return the operation result
         */
        default int checkpointWriteLimit(final String fileName, final long position, final int remaining) {
            return remaining;
        }

        /**
         * Checkpoint file is actually written and injected with device error.
         *
         * @param fileName file name
         * @param position position
         * @throws IOException if the operation cannot complete
         */
        default void afterCheckpointWrite(final String fileName, final long position) throws IOException {
        }

        /**
         * Could not close temporary folder: %s.
         *
         * @param fileName file name
         * @throws IOException if the operation cannot complete
         */
        default void beforeCheckpointForce(final String fileName) throws IOException {
        }

        /**
         * Simulates the loss after the check point file brush has been successfully completed.
         *
         * @param fileName file name
         * @throws IOException if the operation cannot complete
         */
        default void afterCheckpointForce(final String fileName) throws IOException {
        }
    }

    /**
     * Saves the lightweight log index, write set read as needed to avoid memory retirement being held in the log index.
     *
     * @param offset offset
     * @param length length
     * @param receipt receipt
     */
    private record Entry(long offset, int length, WalReceipt receipt) {
    }
}
