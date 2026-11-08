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

import com.flyingzc.azir.wal.WalBackupOptions;
import com.flyingzc.azir.wal.WalCommit;
import com.flyingzc.azir.wal.WalCommitAuthority;
import com.flyingzc.azir.wal.WalMvccRuntime;
import com.flyingzc.azir.wal.WalOptions;
import com.flyingzc.azir.wal.WalReplicaIdentity;
import com.flyingzc.azir.wal.WalRetentionOptions;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.Assertions;

import com.flyingzc.azir.common.IsolationLevel;

import org.apache.ratis.proto.RaftProtos.RaftConfigurationProto;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.io.MD5Hash;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Full mirror and transfer summary real I/O failed, existing file retry conflicted with co-location.
 */
class RatisSnapshotIoTest {

    @TempDir
    Path directory;

    private final RaftGroupId group = RaftGroupId.randomId();

    /**
     * Failure does not destroy the old full mirror, retry will succeed only if the same input is synchronized with mirrors, summaries, directories.
     *
     * @param failure failure
     * @throws Exception if the operation cannot complete
     */
    @ParameterizedTest
    @EnumSource(Failure.class)
    void incompletePublicationNeverBecomesSuccessfulUntilDurablyRetried(final Failure failure) throws Exception {
        Path oldBundle = bundle(9, 1), requested = bundle(10, 2);
        var position = TermIndex.valueOf(3, 10);
        try (var raft = raft()) {
            var original = new RatisSnapshotStorage(group, limits());
            original.init(raft);
            var old = original.publish(oldBundle, TermIndex.valueOf(3, 9));
            byte[] oldBytes = Files.readAllBytes(old.getFile().getPath());
            var broken = new Fault(failure);
            var shelf = new RatisSnapshotStorage(group, limits(), broken);
            shelf.init(raft);
            assertThrows(IOException.class, () -> shelf.publish(requested, position));
            assertTrue(broken.fired);
            assertArrayEquals(oldBytes, Files.readAllBytes(old.getFile().getPath()));
            Path target = shelf.getSnapshotDir().toPath().resolve("snapshot.3_10");
            boolean present = Files.exists(target);
            Assertions.assertEquals(afterMove(failure), present);
            var retry = new Fault(null);
            var recovered = new RatisSnapshotStorage(group, limits(), retry);
            recovered.init(raft);
            var result = recovered.publish(requested, position);
            Assertions.assertEquals(position, result.getTermIndex());
            assertArrayEquals(Files.readAllBytes(requested), Files.readAllBytes(result.getFile().getPath()));
            assertTrue(retry.operations.contains("FORCE_DATA"));
            assertTrue(retry.operations.contains("FORCE_DIGEST"));
            assertTrue(retry.directories >= 2);
            if (present) {
                assertFalse(retry.operations.contains("WRITE"));
                assertFalse(retry.operations.contains("MOVE"));
            }
            var reopened = new RatisSnapshotStorage(group, limits());
            reopened.init(raft);
            Assertions.assertEquals(position, reopened.syncReceived().getTermIndex());
        }
    }

    /**
     * Synchronising again when a complete mirror exists failed, and reload caches are not allowed to replace force.
     *
     * @param failure failure
     * @throws Exception if the operation cannot complete
     */
    @ParameterizedTest
    @EnumSource(value = Failure.class,
            names = {"BEFORE_DATA_FORCE", "AFTER_DATA_FORCE", "BEFORE_DIGEST_FORCE", "AFTER_DIGEST_FORCE", "BEFORE_FIRST_DIR", "AFTER_FIRST_DIR", "BEFORE_SECOND_DIR", "AFTER_SECOND_DIR"})
    void existingSnapshotMustNotSkipAnotherSyncFailure(final Failure failure) throws Exception {
        Path requested = bundle(10, 1);
        var position = TermIndex.valueOf(3, 10);
        try (var raft = raft()) {
            var good = new RatisSnapshotStorage(group, limits());
            good.init(raft);
            good.publish(requested, position);
            var fault = new Fault(failure);
            var retry = new RatisSnapshotStorage(group, limits(), fault);
            retry.init(raft);
            assertThrows(IOException.class, () -> retry.publish(requested, position));
            assertTrue(fault.fired);
            assertArrayEquals(Files.readAllBytes(requested), Files.readAllBytes(good.getLatestSnapshot().getFiles().getFirst().getPath()));
        }
    }

    /**
     * Another valid feature at the same location, a different time with an index and a lower position cannot overwhelm or retreat to published mirrors.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void validButConflictingInputCannotReplaceSamePositionOrRegressLatest() throws Exception {
        Path first = bundle(10, 1), other = bundle(10, 2), lower = bundle(9, 1), wrongTerm = bundle(10, 3, 4);
        var position = TermIndex.valueOf(3, 10);
        try (var raft = raft()) {
            var shelf = new RatisSnapshotStorage(group, limits());
            shelf.init(raft);
            var existing = shelf.publish(first, position);
            byte[] bytes = Files.readAllBytes(existing.getFile().getPath());
            assertThrows(IOException.class, () -> shelf.publish(other, position));
            assertThrows(IOException.class, () -> shelf.publish(lower, TermIndex.valueOf(3, 9)));
            assertThrows(IOException.class, () -> shelf.publish(wrongTerm, TermIndex.valueOf(4, 10)));
            Assertions.assertEquals(position, shelf.getLatestSnapshot().getTermIndex());
            assertArrayEquals(bytes, Files.readAllBytes(existing.getFile().getPath()));
        }
    }

    /**
     * Formal Raft catalogues are initialized, but the location is given by people and does not pretend to be a majority consensus certificate.
     *
     * @return the operation result
     * @throws Exception if the operation cannot complete
     */
    private RaftStorage raft() throws Exception {
        var value = RaftStorage.newBuilder().setDirectory(directory.resolve("raft").toFile()).setOption(RaftStorage.StartupOption.FORMAT)
                .setStorageFreeSpaceMin(org.apache.ratis.util.SizeInBytes.valueOf("1MB")).build();
        value.initialize();
        return value;
    }

    /**
     * Each input comes from a real commit Root backup, and the content conflict still has a valid code and summary.
     *
     * @param index index
     * @param value value to write
     * @return the operation result
     * @throws Exception if the operation cannot complete
     */
    private Path bundle(final long index, final int value) throws Exception {
        return bundle(index, value, 3);
    }

    /**
     * The different term of office was entered to maintain a legal cover and was used to prove rejection from the location contract rather than an error in coding.
     *
     * @param index index
     * @param value value to write
     * @param term term
     * @return the operation result
     * @throws Exception if the operation cannot complete
     */
    private Path bundle(final long index, final int value, final long term) throws Exception {
        String suffix = index + "-" + value + "-" + term;
        var authority = new Authority();
        Path backup = directory.resolve("backup-" + suffix);
        var identity = new WalReplicaIdentity(group.getUuid(), group.getUuid(), "source");
        try (var core = WalMvccRuntime.openReplica(directory.resolve("source-" + suffix), Set.of("a"), Map.of(), WalOptions.defaults(), WalRetentionOptions.preserveAll(), identity, authority)) {
            authority.core = core;
            var tx = core.begin(IsolationLevel.REPEATABLE_READ);
            core.put(tx, "a", "k", new byte[]{(byte) value});
            core.commit(tx);
            core.backupReplica(authority, backup, limits());
        }
        var peers = RaftConfigurationProto.newBuilder();
        for (int i = 0; i < 3; i++) {
            peers.addPeers(RaftPeer.newBuilder().setId("n" + i).setAddress("127.0.0.1:" + (19001 + i)).build().getRaftPeerProto());
        }
        Path file = directory.resolve("bundle-" + suffix);
        RatisSnapshotBundle.pack(backup, file, group, TermIndex.valueOf(term, index), 0, peers.build(), limits());
        return file;
    }

    /**
     * A limited sample of budget, with a different size for official data.
     *
     * @return the operation result
     */
    private static WalBackupOptions limits() {
        return new WalBackupOptions(16_000_000, 8, 64_000, 60_000);
    }

    /**
     * Atomic may leave a complete official mirror image after the name has been moved.
     *
     * @param failure failure
     * @return the operation result
     */
    private static boolean afterMove(final Failure failure) {
        return Set.of(Failure.AFTER_MOVE, Failure.BEFORE_SECOND_DIR, Failure.AFTER_SECOND_DIR, Failure.BEFORE_PARENT_DIR, Failure.AFTER_PARENT_DIR).contains(failure);
    }

    /**
     * Both before and after operations may fail, with retry different before and after the official mirror name is released.
     */
    enum Failure {
        PARTIAL_WRITE, BEFORE_DATA_FORCE, AFTER_DATA_FORCE, BEFORE_DIGEST_SAVE, AFTER_DIGEST_SAVE, BEFORE_DIGEST_FORCE, AFTER_DIGEST_FORCE, BEFORE_FIRST_DIR, AFTER_FIRST_DIR, BEFORE_MOVE, AFTER_MOVE,
        BEFORE_SECOND_DIR, AFTER_SECOND_DIR, BEFORE_PARENT_DIR, AFTER_PARENT_DIR
    }

    /**
     * A controlled commit port entered by a genuine backup does not provide a distributed majority.
     */
    private static final class Authority implements WalCommitAuthority {

        WalMvccRuntime core;

        /**
         * Fixed test term.
         *
         * @return the operation result
         */
        @Override
        public long epoch() {
            return 1;
        }

        /**
         * Only for generating a valid backup.
         *
         * @param timeout timeout
         * @return the operation result
         */
        @Override
        public long readBarrier(final long timeout) {
            return 1;
        }

        /**
         * Real installation confirmed.
         *
         * @param epoch epoch
         * @param records records
         * @return the operation result
         */
        @Override
        public CompletionStage<Void> submit(final long epoch, final List<WalCommit> records) {
            core.applyCommitted(this, records);
            return CompletableFuture.completedFuture(null);
        }
    }

    /**
     * The actual operation of the package by file type and directory stage was hit only once with the expected failure.
     */
    private static final class Fault implements RatisSnapshotStorage.Io {

        final Failure failure;

        final List<String> operations = new ArrayList<>();

        boolean fired;

        int directories;

        /**
         * No failure instance record if retry is really synchronized.
         *
         * @param failure failure
         */
        Fault(final Failure failure) {
            this.failure = failure;
        }

        /**
         * The real side effects are maintained after the operation has failed.
         *
         * @param point point
         * @throws IOException if the operation cannot complete
         */
        private void hit(final Failure point) throws IOException {
            if (!fired && failure == point) {
                fired = true;
                throw new IOException("injected snapshot IO: " + point);
            }
        }

        /**
         * Stop after writing the real first half.
         *
         * @param channel channel
         * @param bytes bytes
         * @throws IOException if the operation cannot complete
         */
        @Override
        public void write(final FileChannel channel, final ByteBuffer bytes) throws IOException {
            operations.add("WRITE");
            if (failure == Failure.PARTIAL_WRITE) {
                var half = bytes.slice();
                half.limit(Math.max(1, half.remaining() / 2));
                RatisGenerationRecord.write(channel, half);
                hit(Failure.PARTIAL_WRITE);
            }
            RatisSnapshotStorage.Io.super.write(channel, bytes);
        }

        /**
         * Confirms the full mirror content and the transfer summary, respectively.
         *
         * @param path path
         * @param channel channel
         * @throws IOException if the operation cannot complete
         */
        @Override
        public void forceFile(final Path path, final FileChannel channel) throws IOException {
            boolean digest = path.getFileName().toString().endsWith(".md5");
            operations.add(digest ? "FORCE_DIGEST" : "FORCE_DATA");
            hit(digest ? Failure.BEFORE_DIGEST_FORCE : Failure.BEFORE_DATA_FORCE);
            RatisSnapshotStorage.Io.super.forceFile(path, channel);
            hit(digest ? Failure.AFTER_DIGEST_FORCE : Failure.AFTER_DATA_FORCE);
        }

        /**
         * The summary produces a real occurrence, and the failure allows for an isolated summary to be cleared.
         *
         * @param target target
         * @param digest digest
         * @throws IOException if the operation cannot complete
         */
        @Override
        public void saveDigest(final Path target, final MD5Hash digest) throws IOException {
            operations.add("DIGEST_SAVE");
            hit(Failure.BEFORE_DIGEST_SAVE);
            RatisSnapshotStorage.Io.super.saveDigest(target, digest);
            hit(Failure.AFTER_DIGEST_SAVE);
        }

        /**
         * Verify More key direct committee check to use a single bar in query.
         *
         * @param source database connection source
         * @param target target
         * @throws IOException if the operation cannot complete
         */
        @Override
        public void move(final Path source, final Path target) throws IOException {
            operations.add("MOVE");
            hit(Failure.BEFORE_MOVE);
            RatisSnapshotStorage.Io.super.move(source, target);
            hit(Failure.AFTER_MOVE);
        }

        /**
         * The new release confirms the name of the summary, then the mirror name and parent directory; there are two directories with retry confirmation.
         *
         * @param path path
         * @throws IOException if the operation cannot complete
         */
        @Override
        public void forceDirectory(final Path path) throws IOException {
            directories++;
            Failure before = directories == 1 ? Failure.BEFORE_FIRST_DIR : directories == 2 ? Failure.BEFORE_SECOND_DIR : Failure.BEFORE_PARENT_DIR;
            Failure after = directories == 1 ? Failure.AFTER_FIRST_DIR : directories == 2 ? Failure.AFTER_SECOND_DIR : Failure.AFTER_PARENT_DIR;
            hit(before);
            RatisSnapshotStorage.Io.super.forceDirectory(path);
            hit(after);
        }
    }
}
