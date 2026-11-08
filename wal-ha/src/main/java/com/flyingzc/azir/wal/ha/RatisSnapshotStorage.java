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

import org.apache.ratis.statemachine.SnapshotInfo;
import org.apache.ratis.statemachine.SnapshotRetentionPolicy;
import org.apache.ratis.statemachine.StateMachineStorage;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import com.flyingzc.azir.wal.WalBackupOptions;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.FileInfo;
import org.apache.ratis.server.storage.RaftStorage;

import org.apache.ratis.statemachine.impl.SingleFileSnapshotInfo;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.util.MD5FileUtil;
import org.apache.ratis.io.MD5Hash;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import java.util.UUID;

/**
 * Remountable single file snapshot, do not swallow damage abnormalities and do not downgrade missing latest snapshot to an old state.
 */
final class RatisSnapshotStorage implements StateMachineStorage {

    private final RaftGroupId group;

    private final WalBackupOptions limits;

    private final Io io;

    private Path directory;

    private volatile SingleFileSnapshotInfo latest;

    private Stamp verifiedData, verifiedDigest;

    /**
     * Use clear group identity and limited maintenance budget.
     *
     * @param group group
     * @param limits limits
     */
    RatisSnapshotStorage(final RaftGroupId group, final WalBackupOptions limits) {
        this(group, limits, new Io() {
        });
    }

    /**
     * Failure Injection only works on the inner structure of the package, and the actual operation is fully consistent with normal assembly.
     *
     * @param group group
     * @param limits limits
     * @param io io
     */
    RatisSnapshotStorage(final RaftGroupId group, final WalBackupOptions limits, final Io io) {
        this.group = group;
        this.limits = limits;
        this.io = java.util.Objects.requireNonNull(io);
    }

    /**
     * Each process begins to check the top position on the disk and cannot rely on old memory caches.
     *
     * @param storage storage
     * @throws IOException if the operation cannot complete
     */
    @Override
    public void init(final RaftStorage storage) throws IOException {
        directory = storage.getStorageDir().getStateMachineDir().toPath();
        Files.createDirectories(directory);
        reload();
    }

    /**
     * Ratis replacement status directories must be re-scanned later, and the new directory anomaly directly prevents installation from being completed.
     *
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    synchronized SingleFileSnapshotInfo reload() throws IOException {
        return reload(null);
    }

    /**
     * Maintenance of scans shared with validate budget; normal start-up continues to complete the directory and content check.
     *
     * @param work work
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    private SingleFileSnapshotInfo reload(final RatisRetirementWork work) throws IOException {
        Path newest = null;
        TermIndex position = null;
        int inspected = 0;
        try (var files = Files.list(directory)) {
            var iterator = files.iterator();
            while (iterator.hasNext()) {
                Path path = iterator.next();
                if (work != null) {
                    work.scan();
                }
                if (++inspected > (work == null ? 4096 : work.options.scanLimit())) {
                    throw new IOException("snapshot catalog exceeds maintenance budget");
                }
                if (!SimpleStateMachineStorage.SNAPSHOT_REGEX.matcher(path.getFileName().toString()).matches()) {
                    continue;
                }
                if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
                    throw new IOException("snapshot path is not a regular file");
                }
                var candidate = SimpleStateMachineStorage.getTermIndexFromSnapshotFile(path.toFile());
                if (position == null || candidate.getIndex() > position.getIndex()) {
                    newest = path;
                    position = candidate;
                } else if (candidate.getIndex() == position.getIndex() && candidate.getTerm() != position.getTerm()) {
                    throw new IOException("ambiguous snapshot term at same index");
                }
            }
        }
        if (newest == null) {
            if (latest != null) {
                throw new IOException("latest snapshot disappeared");
            }
            return null;
        }
        if (latest != null && position.getIndex() < latest.getIndex()) {
            throw new IOException("latest snapshot regressed");
        }
        Path digestPath = MD5FileUtil.getDigestFileForFile(newest.toFile()).toPath();
        var dataStamp = stamp(newest);
        var digestStamp = stamp(digestPath);
        if (latest != null && position.equals(latest.getTermIndex()) && newest.equals(latest.getFile().getPath()) && dataStamp.equals(verifiedData) && digestStamp.equals(verifiedDigest)) {
            return latest;
        }
        var verificationLimits = work == null ? limits : new WalBackupOptions(limits.maxBytes(), limits.batchKeys(), limits.batchBytes(), Math.min(limits.timeoutMillis(), work.remainingMillis()));
        RatisSnapshotBundle.inspect(newest, group, position, verificationLimits);
        var expected = MD5FileUtil.readStoredMd5ForFile(newest.toFile());
        if (expected == null || !expected.equals(md5(newest, work))) {
            throw new IOException("snapshot transport digest missing or mismatched");
        }
        latest = new SingleFileSnapshotInfo(new FileInfo(newest, expected), position.getTerm(), position.getIndex());
        verifiedData = dataStamp;
        verifiedDigest = digestStamp;
        return latest;
    }

    /**
     * The complete file and transfer summary are refreshed before the file name is released, supporting the installation of the replica reconstruction status directory from stand-alone.
     *
     * @param bundle bundle
     * @param position position
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    synchronized SingleFileSnapshotInfo publish(final Path bundle, final TermIndex position) throws IOException {
        RatisSnapshotBundle.inspect(bundle, group, position, limits);
        var previous = reload();
        if (previous != null && (position.getIndex() < previous.getIndex() || position.getIndex() == previous.getIndex() && position.getTerm() != previous.getTerm())) {
            throw new IOException("snapshot publication regressed or changed term at existing index");
        }
        Path target = directory.resolve(SimpleStateMachineStorage.getSnapshotFileName(position.getTerm(), position.getIndex()));
        if (Files.exists(target, LinkOption.NOFOLLOW_LINKS)) {
            var current = reload();
            if (current == null || !current.getTermIndex().equals(position) || !sameContents(bundle, current.getFile().getPath())) {
                throw new IOException("snapshot publication collision");
            }
            force(current);
            return current;
        }
        Path temporary = directory.resolve(".bundle-" + UUID.randomUUID());
        try (
                var input = FileChannel.open(bundle, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS);
                var output = FileChannel.open(temporary, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS)) {
            var buffer = ByteBuffer.allocate(65_536);
            long bytes = 0;
            long started = System.nanoTime();
            for (int read; (read = input.read(buffer)) != -1;) {
                if (read < 1 || (bytes += read) > limits.maxBytes() || System.nanoTime() - started > java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(limits.timeoutMillis())) {
                    throw new IOException("snapshot publication exceeds budget");
                }
                buffer.flip();
                io.write(output, buffer);
                buffer.clear();
            }
            io.forceFile(temporary, output);
        }
        RatisSnapshotBundle.inspect(temporary, group, position, limits);
        var checksum = md5(temporary);
        io.saveDigest(target, checksum);
        Path digest = MD5FileUtil.getDigestFileForFile(target.toFile()).toPath();
        try (var metadata = FileChannel.open(digest, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS)) {
            io.forceFile(digest, metadata);
        }
        io.forceDirectory(directory);
        io.move(temporary, target);
        io.forceDirectory(directory);
        io.forceDirectory(directory.getParent());
        latest = new SingleFileSnapshotInfo(new FileInfo(target, checksum), position.getTerm(), position.getIndex());
        verifiedData = stamp(target);
        verifiedDigest = stamp(MD5FileUtil.getDigestFileForFile(target.toFile()).toPath());
        return latest;
    }

    /**
     * Ratis may receive files that are closed but not force, synchronizing files, abstracts and directories before application is installed.
     *
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    synchronized SingleFileSnapshotInfo syncReceived() throws IOException {
        var snapshot = reload();
        if (snapshot == null) {
            throw new IOException("received snapshot missing");
        }
        force(snapshot);
        return snapshot;
    }

    /**
     * The verified files require only synchronization of directories and contents without rereading the entire large mirror.
     *
     * @param snapshot snapshot
     * @throws IOException if the operation cannot complete
     */
    private void force(final SingleFileSnapshotInfo snapshot) throws IOException {
        Path file = snapshot.getFile().getPath();
        for (Path path : java.util.List.of(file, MD5FileUtil.getDigestFileForFile(file.toFile()).toPath())) {
            try (var channel = FileChannel.open(path, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS)) {
                io.forceFile(path, channel);
            }
        }
        io.forceDirectory(directory);
        io.forceDirectory(directory.getParent());
    }

    /**
     * The official mirror of the same location cannot be impersonated by another valid but different content, comparing the workload to a whole budget.
     *
     * @param requested requested
     * @param existing existing
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    private boolean sameContents(final Path requested, final Path existing) throws IOException {
        if (Files.size(requested) != Files.size(existing)) {
            return false;
        }
        long bytes = 0, began = System.nanoTime();
        byte[] a = new byte[65_536], b = new byte[65_536];
        try (var first = Files.newInputStream(requested, LinkOption.NOFOLLOW_LINKS); var second = Files.newInputStream(existing, LinkOption.NOFOLLOW_LINKS)) {
            for (int count; (count = first.read(a)) != -1;) {
                if (count < 1 || (bytes += count) > limits.maxBytes() || System.nanoTime() - began > java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(limits.timeoutMillis())) {
                    throw new IOException("snapshot collision comparison exceeds budget");
                }
                int read = second.readNBytes(b, 0, count);
                if (read != count || java.util.Arrays.mismatch(a, 0, count, b, 0, read) >= 0) {
                    return false;
                }
            }
            return second.read() == -1;
        }
    }

    /**
     * Only durable mirrors of verify returned, and the loading failed to be empty.
     *
     * @return the operation result
     */
    @Override
    public SnapshotInfo getLatestSnapshot() {
        return latest;
    }

    /**
     * The transfer path relative to this directory avoids exposing the business root directory to the Ratis file.
     *
     * @return the operation result
     */
    @Override
    public java.io.File getSnapshotDir() {
        return directory == null ? null : directory.toFile();
    }

    /**
     * This does not format an existing business status; only new directories without mirrors can be accepted.
     *
     * @throws IOException if the operation cannot complete
     */
    @Override
    public void format() throws IOException {
        if (latest != null) {
            throw new IOException("cannot format initialized snapshot storage");
        }
    }

    /**
     * Auto-strategic visible configurations are only cleared when the number is retained; nodal maintenance will also feed into activities and install border protection.
     *
     * @param policy policy
     * @throws IOException if the operation cannot complete
     */
    @Override
    public void cleanupOldSnapshots(final SnapshotRetentionPolicy policy) throws IOException {
        if (policy.getNumSnapshotsRetained() <= 0) {
            return;
        }
        cleanup(new RatisRetirementWork(new RatisRetirementOptions(1, 128, 4096, 1000, Math.max(2, Math.min(32, policy.getNumSnapshotsRetained())))), java.util.Set.of());
    }

    /**
     * The latest mirror is complete validate and synchronized, while the old file has failed to delete only the retry trash and never removes the latest or protected boundary.
     *
     * @param work work
     * @param protectedIndices protected indices
     * @throws IOException if the operation cannot complete
     */
    synchronized void cleanup(final RatisRetirementWork work, final java.util.Set<Long> protectedIndices) throws IOException {
        try {
            work.check();
            var newest = reload(work);
            if (newest != null) {
                force(newest);
            }
            RatisRetirementFiles.scratch(directory, true, work);
            if (newest == null) {
                return;
            }
            var snapshots = new java.util.ArrayList<Path>();
            var orphanDigests = new java.util.ArrayList<Path>();
            try (var entries = Files.list(directory)) {
                var iterator = entries.iterator();
                while (iterator.hasNext()) {
                    work.scan();
                    Path path = iterator.next();
                    String name = path.getFileName().toString();
                    if (SimpleStateMachineStorage.SNAPSHOT_REGEX.matcher(name).matches()) {
                        snapshots.add(path);
                    } else if (SimpleStateMachineStorage.SNAPSHOT_MD5_REGEX.matcher(name).matches()) {
                        Path base = path.resolveSibling(name.substring(0, name.length() - 4));
                        if (!Files.exists(base, LinkOption.NOFOLLOW_LINKS)) {
                            orphanDigests.add(path);
                        }
                    }
                }
            }
            snapshots.sort(java.util.Comparator.comparingLong((Path path) -> SimpleStateMachineStorage.getTermIndexFromSnapshotFile(path.toFile()).getIndex()).reversed());
            for (int i = work.options.retainedSnapshots(); i < snapshots.size(); i++) {
                Path path = snapshots.get(i);
                long index = SimpleStateMachineStorage.getTermIndexFromSnapshotFile(path.toFile()).getIndex();
                if (index == newest.getIndex() || protectedIndices.contains(index)) {
                    continue;
                }
                deleteOld(path, work);
                deleteOld(MD5FileUtil.getDigestFileForFile(path.toFile()).toPath(), work);
            }
            for (Path path : orphanDigests) {
                String name = path.getFileName().toString();
                Path base = path.resolveSibling(name.substring(0, name.length() - 4));
                if (!protectedIndices.contains(SimpleStateMachineStorage.getTermIndexFromSnapshotFile(base.toFile()).getIndex())) {
                    deleteOld(path, work);
                }
            }
        } catch (final RatisRetirementWork.Limit exhausted) {
            work.exhausted = true;
        }
    }

    /**
     * Only removes a fixed mirror file or summary from the list, and links and special files are rejected.
     *
     * @param path path
     * @param work work
     * @throws IOException if the operation cannot complete
     */
    private static void deleteOld(final Path path, final RatisRetirementWork work) throws IOException {
        if (!Files.exists(path, LinkOption.NOFOLLOW_LINKS)) {
            return;
        }
        if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("old snapshot path is not regular");
        }
        work.deleting();
        Files.delete(path);
        work.removed++;
        RatisGenerationRecord.forceDirectory(path.getParent());
    }

    /**
     * Transfer MD5 is only compatible with Ratis, with SHA256 and full mirror verify.
     *
     * @param path path
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    private MD5Hash md5(final Path path) throws IOException {
        return md5(path, null);
    }

    /**
     * Maintenance of a hash-by-block check sharing period and regular release of budget for the mirror itself.
     *
     * @param path path
     * @param work work
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    private MD5Hash md5(final Path path, final RatisRetirementWork work) throws IOException {
        try {
            var hash = java.security.MessageDigest.getInstance("MD5");
            var buffer = new byte[65_536];
            long bytes = 0;
            long started = System.nanoTime();
            try (var input = Files.newInputStream(path, LinkOption.NOFOLLOW_LINKS)) {
                for (int read; (read = input.read(buffer)) != -1;) {
                    if (work != null) {
                        work.check();
                    }
                    if (read < 1 || (bytes += read) > limits.maxBytes() || System.nanoTime() - started > java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(limits.timeoutMillis())) {
                        throw new IOException("snapshot transport checksum exceeds budget");
                    }
                    hash.update(buffer, 0, read);
                }
            }
            return MD5Hash.newInstance(hash.digest());
        } catch (final java.security.NoSuchAlgorithmException failure) {
            throw new IOException(failure);
        }
    }

    /**
     * Symbolic links and special files cannot participate in verify caches; signatures do not contain business content or receipt.
     *
     * @param path path
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    private static Stamp stamp(final Path path) throws IOException {
        var attributes = Files.readAttributes(path, java.nio.file.attribute.BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        if (!attributes.isRegularFile()) {
            throw new IOException("snapshot verification path is not regular");
        }
        return new Stamp(attributes.fileKey(), attributes.size(), attributes.lastModifiedTime());
    }

    /**
     * The signature cache of the verify immutable file, to recheck it when it changes or restarts.
     *
     * @param fileKey file key
     * @param bytes bytes
     * @param modified modified
     */
    private record Stamp(Object fileKey, long bytes, java.nio.file.attribute.FileTime modified) {
    }

    /**
     * The I/ O boundary of the mirror large file and transfer abstract is not allowed to test hooks instead of content validate.
     */
    interface Io {

        /**
         * Large files are written on a block-by-block basis, and part of the file fails to leave private input.
         *
         * @param channel channel
         * @param bytes bytes
         * @throws IOException if the operation cannot complete
         */
        default void write(final FileChannel channel, final ByteBuffer bytes) throws IOException {
            RatisGenerationRecord.write(channel, bytes);
        }

        /**
         * Path is used only to distinguish the failure boundary in a package between mirrors and abstracts.
         *
         * @param path path
         * @param channel channel
         * @throws IOException if the operation cannot complete
         */
        default void forceFile(final Path path, final FileChannel channel) throws IOException {
            channel.force(true);
        }

        /**
         * The transmission summary follows the name and format specified by Ratis.
         *
         * @param target target
         * @param digest digest
         * @throws IOException if the operation cannot complete
         */
        default void saveDigest(final Path target, final MD5Hash digest) throws IOException {
            MD5FileUtil.saveMD5File(target.toFile(), digest);
        }

        /**
         * Verify More key direct committee check to use a single bar in query.
         *
         * @param source database connection source
         * @param target target
         * @throws IOException if the operation cannot complete
         */
        default void move(final Path source, final Path target) throws IOException {
            Files.move(source, target, StandardCopyOption.ATOMIC_MOVE);
        }

        /**
         * Both the official name and the parent directory require durable confirmation.
         *
         * @param directory directory
         * @throws IOException if the operation cannot complete
         */
        default void forceDirectory(final Path directory) throws IOException {
            RatisGenerationRecord.forceDirectory(directory);
        }
    }
}
