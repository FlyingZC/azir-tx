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

package com.flyingzc.azir.wal.ha;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import com.flyingzc.azir.wal.WalBackup;
import com.flyingzc.azir.wal.WalBackupOptions;
import com.flyingzc.azir.wal.WalCheckpoint;
import org.apache.ratis.proto.RaftProtos.RaftConfigurationProto;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.server.protocol.TermIndex;

import java.nio.channels.FileChannel;

import java.security.DigestInputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.util.HashSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Single file transfer format for full replica mirrors, binding Raft position, member configuration and operational boundaries without copying voting metadata.
 */
final class RatisSnapshotBundle {

    private static final int MAGIC = 0x415A5253;

    private static final int END = 0x415A5245;

    private static final int MAX_FILES = 102_001;

    /**
     * Tools are only installed in state machines and do not clean up to the business exposure log or write any access path.
     */
    private RatisSnapshotBundle() {
    }

    /**
     * Write new files and force; the caller can only publish Ratis snapshot file names after completion.
     *
     * @param backup backup
     * @param destination destination
     * @param group group
     * @param position position
     * @param configurationIndex configuration index
     * @param configuration configuration
     * @param options options
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    static Header pack(final Path backup, final Path destination, final RaftGroupId group, final TermIndex position,
                       final long configurationIndex, final RaftConfigurationProto configuration, final WalBackupOptions options) throws IOException {
        var budget = new Budget(options);
        Path root = backup.toRealPath();
        var info = WalBackup.inspect(root, options);
        if (!info.engineId().equals(group.getUuid()) || !Files.isRegularFile(root.resolve("local/REPLICA"), LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("snapshot requires this Raft group's replica backup");
        }
        var header = new Header(group.getUuid(), position, configurationIndex, configuration, info.backupId(), new WalCheckpoint(info.sequence(), info.digest()));
        var files = new java.util.ArrayList<Path>();
        int inspected = 0;
        try (var stream = Files.walk(root, 3)) {
            var iterator = stream.iterator();
            while (iterator.hasNext()) {
                Path path = iterator.next();
                if (path.equals(root)) {
                    continue;
                }
                budget.check();
                if (++inspected > MAX_FILES + 103_000 || Files.isSymbolicLink(path)) {
                    throw new IOException("snapshot tree exceeds budget or contains symbolic link");
                }
                String name = root.relativize(path).toString();
                if (Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS)) {
                    if (!name.matches("local|data|local/checkpoint-[a-f0-9-]{36}")) {
                        throw new IOException("snapshot contains unknown directory");
                    }
                } else {
                    if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS) || !allowed(name) || files.size() >= MAX_FILES) {
                        throw new IOException("snapshot contains unknown or too many files");
                    }
                    files.add(path);
                }
            }
        }
        files.sort(java.util.Comparator.comparing(path -> root.relativize(path).toString()));
        if (files.isEmpty() || files.size() > MAX_FILES) {
            throw new IOException("snapshot file count exceeds budget");
        }
        var hash = sha();
        try (
                var channel = FileChannel.open(destination, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS);
                var buffered = new BufferedOutputStream(java.nio.channels.Channels.newOutputStream(channel), 65_536);
                var digest = new DigestOutputStream(new LimitedOutput(buffered, budget), hash);
                var out = new DataOutputStream(digest)) {
            writeHeader(out, header);
            out.writeInt(files.size());
            var buffer = new byte[65_536];
            for (Path file : files) {
                out.writeUTF(root.relativize(file).toString());
                long expected = Files.size(file);
                out.writeLong(expected);
                long count = 0;
                try (var input = Files.newInputStream(file, LinkOption.NOFOLLOW_LINKS)) {
                    for (int read; (read = input.read(buffer)) != -1;) {
                        if (read == 0) {
                            throw new IOException("snapshot read made no progress");
                        }
                        out.write(buffer, 0, read);
                        count += read;
                    }
                }
                if (count != expected) {
                    throw new IOException("snapshot source changed during copy");
                }
            }
            out.writeInt(END);
            out.flush();
            byte[] checksum = hash.digest();
            digest.on(false);
            out.write(checksum);
            out.flush();
            channel.force(true);
        }
        forceDirectory(destination.toAbsolutePath().getParent());
        return header;
    }

    /**
     * Verify envelope, metadata and full file abstract only; business mirror content must also go through the complete verify format of extract.
     *
     * @param bundle bundle
     * @param group group
     * @param position position
     * @param options options
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    static Header inspect(final Path bundle, final RaftGroupId group, final TermIndex position, final WalBackupOptions options) throws IOException {
        return scan(bundle, null, group, position, new Budget(options));
    }

    /**
     * Checks the entire transfer file and then unpacks it to a private new directory, complete mirror verify before posting the target directory.
     *
     * @param bundle bundle
     * @param destination destination
     * @param group group
     * @param position position
     * @param options options
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    static Header extract(final Path bundle, final Path destination, final RaftGroupId group, final TermIndex position, final WalBackupOptions options) throws IOException {
        var budget = new Budget(options);
        Path target = destination.toAbsolutePath().normalize();
        Path parent = target.getParent();
        if (parent == null || !Files.isDirectory(parent, LinkOption.NOFOLLOW_LINKS) || Files.exists(target, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("snapshot extraction requires a new target");
        }
        Header inspected = scan(bundle, null, group, position, budget);
        Path stage = parent.resolve(".azir-unpack-" + UUID.randomUUID());
        Files.createDirectory(stage);
        forceDirectory(parent);
        try {
            Header extracted = scan(bundle, stage, group, position, budget);
            if (!inspected.equals(extracted)) {
                throw new IOException("snapshot metadata changed during extraction");
            }
            var info = WalBackup.inspect(stage, options);
            if (!info.engineId().equals(extracted.group()) || !info.backupId().equals(extracted.backupId())
                    || info.sequence() != extracted.boundary().sequence() || !info.digest().equals(extracted.boundary().digest())
                    || !Files.isRegularFile(stage.resolve("local/REPLICA"), LinkOption.NOFOLLOW_LINKS)) {
                throw new IOException("snapshot business boundary mismatch");
            }
            budget.check();
            try (var paths = Files.walk(stage, 3)) {
                for (Path path : paths.filter(Files::isDirectory).sorted(java.util.Comparator.reverseOrder()).toList()) {
                    forceDirectory(path);
                }
            }
            if (Files.exists(target, LinkOption.NOFOLLOW_LINKS)) {
                throw new IOException("snapshot extraction target appeared");
            }
            Files.move(stage, target, StandardCopyOption.ATOMIC_MOVE);
            forceDirectory(parent);
            return extracted;
        } catch (final IOException | RuntimeException | Error failure) {
            try {
                discardOwnedStage(stage);
            } catch (final IOException cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }

    /**
     * Only the random private temporary directory that this method has just created does not process a caller with a target or follow a symbol link.
     *
     * @param stage stage
     * @throws IOException if the operation cannot complete
     */
    private static void discardOwnedStage(final Path stage) throws IOException {
        if (!Files.exists(stage, LinkOption.NOFOLLOW_LINKS)) {
            return;
        }
        try (var paths = Files.walk(stage)) {
            for (Path path : paths.sorted(java.util.Comparator.reverseOrder()).toList()) {
                Files.delete(path);
            }
        }
        forceDirectory(stage.getParent());
    }

    /**
     * All lengths, paths and total bytes are validate before writing. They cannot follow a remote file name to escape the private recovery directory.
     *
     * @param bundle bundle
     * @param stage stage
     * @param group group
     * @param position position
     * @param budget budget
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    private static Header scan(final Path bundle, final Path stage, final RaftGroupId group, final TermIndex position, final Budget budget) throws IOException {
        if (!Files.isRegularFile(bundle, LinkOption.NOFOLLOW_LINKS) || Files.size(bundle) > budget.options.maxBytes()) {
            throw new IOException("snapshot file missing or exceeds byte budget");
        }
        var hash = sha();
        long bytes = 0;
        try (
                var input = new BufferedInputStream(Files.newInputStream(bundle, LinkOption.NOFOLLOW_LINKS), 65_536);
                var digest = new DigestInputStream(input, hash); var in = new DataInputStream(digest)) {
            Header header = readHeader(in);
            if (!header.group().equals(group.getUuid()) || !header.position().equals(position)) {
                throw new IOException("snapshot Raft group or position mismatch");
            }
            int count = in.readInt();
            if (count < 1 || count > MAX_FILES) {
                throw new IOException("snapshot file count exceeds budget");
            }
            var names = new HashSet<String>();
            var buffer = new byte[65_536];
            for (int i = 0; i < count; i++) {
                budget.check();
                String name = in.readUTF();
                long size = in.readLong();
                if (!allowed(name) || !names.add(name) || size < 1 || size > budget.options.maxBytes() - bytes) {
                    throw new IOException("invalid snapshot file entry");
                }
                bytes += size;
                Path target = stage == null ? null : stage.resolve(name);
                if (target != null) {
                    Files.createDirectories(target.getParent());
                }
                try (var out = target == null ? OutputStream.nullOutputStream() : Files.newOutputStream(target, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS)) {
                    long left = size;
                    while (left > 0) {
                        budget.check();
                        int read = in.read(buffer, 0, (int) Math.min(left, buffer.length));
                        if (read < 1) {
                            throw new IOException("truncated snapshot file");
                        }
                        out.write(buffer, 0, read);
                        left -= read;
                    }
                }
                if (target != null) {
                    try (var channel = FileChannel.open(target, StandardOpenOption.WRITE)) {
                        channel.force(true);
                    }
                }
            }
            if (!names.contains("COMPLETE") || !names.contains("local/REPLICA") || in.readInt() != END) {
                throw new IOException("incomplete snapshot file catalog");
            }
            byte[] checksum = hash.digest();
            digest.on(false);
            if (!MessageDigest.isEqual(checksum, in.readNBytes(32)) || in.read() != -1) {
                throw new IOException("snapshot checksum or length mismatch");
            }
            budget.check();
            return header;
        } catch (final IllegalArgumentException failure) {
            throw new IOException("invalid snapshot metadata", failure);
        }
    }

    /**
     * Snapshot allows only a fixed file collection of backup protocols, and path separator and filenames do not accept user-defined.
     *
     * @param name name
     * @return the operation result
     */
    private static boolean allowed(final String name) {
        return name.matches("COMPLETE|local/(ENGINE|REPLICA|CURRENT|transactions\\.wal|receipts-[a-f0-9-]{36}\\.sst|checkpoint-[a-f0-9-]{36}/(snapshot\\.bin|tail\\.wal))|data/[0-9]{4}\\.bin");
    }

    /**
     * Stable code Raft location and its membership configuration, with content participating in the summary together with business documents.
     *
     * @param out out
     * @param header header
     * @throws IOException if the operation cannot complete
     */
    private static void writeHeader(final DataOutputStream out, final Header header) throws IOException {
        out.writeInt(MAGIC);
        out.writeInt(1);
        uuid(out, header.group());
        out.writeLong(header.position().getTerm());
        out.writeLong(header.position().getIndex());
        out.writeLong(header.configurationIndex());
        byte[] configuration = header.configuration().toByteArray();
        out.writeInt(configuration.length);
        out.write(configuration);
        uuid(out, header.backupId());
        out.writeLong(header.boundary().sequence());
        out.writeUTF(header.boundary().digest());
    }

    /**
     * Configure the message to limit the length of the message before it is distributed. The anti-serialization anomaly cannot be converted into a default empty configuration.
     *
     * @param in in
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    private static Header readHeader(final DataInputStream in) throws IOException {
        if (in.readInt() != MAGIC || in.readInt() != 1) {
            throw new IOException("unsupported snapshot bundle");
        }
        UUID group = uuid(in);
        TermIndex position = TermIndex.valueOf(in.readLong(), in.readLong());
        long configurationIndex = in.readLong();
        int length = in.readInt();
        if (length < 1 || length > 1_048_576) {
            throw new IOException("snapshot configuration exceeds budget");
        }
        byte[] bytes = in.readNBytes(length);
        if (bytes.length != length) {
            throw new IOException("truncated snapshot configuration");
        }
        var configuration = RaftConfigurationProto.parseFrom(bytes);
        return new Header(group, position, configurationIndex, configuration, uuid(in), new WalCheckpoint(in.readLong(), in.readUTF()));
    }

    /**
     * Fixed length UUID does not guess through directory or string.
     *
     * @param out out
     * @param value value to write
     * @throws IOException if the operation cannot complete
     */
    private static void uuid(final DataOutputStream out, final UUID value) throws IOException {
        out.writeLong(value.getMostSignificantBits());
        out.writeLong(value.getLeastSignificantBits());
    }

    /**
     * Reads a fixed length UUID.
     *
     * @param in in
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    private static UUID uuid(final DataInputStream in) throws IOException {
        return new UUID(in.readLong(), in.readLong());
    }

    /**
     * SHA256's missing is an error in the operating environment and cannot be reduced to no validate.
     *
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    private static MessageDigest sha() throws IOException {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (final java.security.NoSuchAlgorithmException failure) {
            throw new IOException(failure);
        }
    }

    /**
     * Synchronizes a directory entry after the file is synchronized. You cannot rely only on the output filename in the memory.
     *
     * @param directory directory
     * @throws IOException if the operation cannot complete
     */
    private static void forceDirectory(final Path directory) throws IOException {
        try (var channel = FileChannel.open(directory, StandardOpenOption.READ)) {
            channel.force(true);
        }
    }

    /**
     * Fixed metadata are protected by the entire file SHA256 and can not be configured more than the applied position.
     *
     * @param group group
     * @param position position
     * @param configurationIndex configuration index
     * @param configuration configuration
     * @param backupId backup id
     * @param boundary boundary
     */
    record Header(UUID group, TermIndex position, long configurationIndex, RaftConfigurationProto configuration,
            UUID backupId, WalCheckpoint boundary) {

        /**
         * The decoded verify minimum requirements for limited configuration, operational boundaries and assembly of members.
         *
         * @param group group
         * @param position position
         * @param configurationIndex configuration index
         * @param configuration configuration
         * @param backupId backup id
         * @param boundary boundary
         * @throws IllegalArgumentException if the operation cannot complete
         */
        Header {
            java.util.Objects.requireNonNull(group);
            java.util.Objects.requireNonNull(position);
            java.util.Objects.requireNonNull(backupId);
            java.util.Objects.requireNonNull(configuration);
            java.util.Objects.requireNonNull(boundary);
            if (position.getTerm() < 1 || position.getIndex() < 0 || configurationIndex < -1 || configurationIndex > position.getIndex()
                    || configuration.getPeersCount() < 1 || configuration.getSerializedSize() > 1_048_576 || configuration.getPeersCount() > 1024
                    || configuration.getOldPeersCount() > 1024 || configuration.getListenersCount() > 1024 || configuration.getOldListenersCount() > 1024
                    || boundary.sequence() < 0 || !(boundary.sequence() == 0 ? boundary.digest().isEmpty() : boundary.digest().matches("[a-f0-9]{64}"))) {
                throw new IllegalArgumentException("invalid snapshot metadata");
            }
        }
    }

    /**
     * Maintains a single-tone period, with all segments checking the total or duration before I/O.
     */
    private static final class Budget {

        final WalBackupOptions options;

        final long started = System.nanoTime();

        long written;

        /**
         * Each time a separate count is sealed or unpacked.
         *
         * @param options options
         */
        Budget(final WalBackupOptions options) {
            this.options = java.util.Objects.requireNonNull(options);
        }

        /**
         * Do not interrupt the shared engine file channel by thread.
         *
         * @throws IOException if the operation cannot complete
         */
        void check() throws IOException {
            if (Thread.currentThread().isInterrupted() || System.nanoTime() - started >= TimeUnit.MILLISECONDS.toNanos(options.timeoutMillis())) {
                throw new IOException("snapshot maintenance timed out or interrupted");
            }
        }

        /**
         * The header and member configuration are also included in the byte total budget.
         *
         * @param size size
         * @throws IOException if the operation cannot complete
         */
        void add(final int size) throws IOException {
            check();
            if (size < 0 || size > options.maxBytes() - written) {
                throw new IOException("snapshot bundle exceeds byte budget");
            }
            written += size;
        }
    }

    /**
     * Encoding is pre-encoded bytes, and the target disk cannot be filled before checking the upper limit.
     */
    private static final class LimitedOutput extends FilterOutputStream {

        final Budget budget;

        /**
         * Saves an independently operated budget.
         *
         * @param output output
         * @param budget budget
         */
        LimitedOutput(final OutputStream output, final Budget budget) {
            super(output);
            this.budget = budget;
        }

        /**
         * The single-bytes field is also subject to total volume limitations.
         *
         * @param value value to write
         * @throws IOException if the operation cannot complete
         */
        @Override
        public void write(final int value) throws IOException {
            budget.add(1);
            out.write(value);
        }

        /**
         * Large pieces are written directly to the bottom and do not degrade into a byte cycle.
         *
         * @param bytes bytes
         * @param offset offset
         * @param length length
         * @throws IOException if the operation cannot complete
         */
        @Override
        public void write(final byte[] bytes, final int offset, final int length) throws IOException {
            budget.add(length);
            out.write(bytes, offset, length);
        }
    }
}
