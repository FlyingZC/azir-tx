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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Complete snapshot retention, protection of borders, break-out and latest damage rejection specific.
 */
class RatisSnapshotRetirementTest {

    @TempDir
    Path directory;

    private final RaftGroupId group = RaftGroupId.randomId();

    /**
     * One round of entries also cleans up old data and isolates MD5, keeping the latest two and visible borders.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void boundedCleanupPreservesLatestAndProtectedSnapshot() throws Exception {
        Path backup = backup();
        try (var storage = storage()) {
            var shelf = new RatisSnapshotStorage(group, limits());
            shelf.init(storage);
            publish(shelf, backup);
            for (int i = 0; i < 10; i++) {
                var work = new RatisRetirementWork(options());
                shelf.cleanup(work, Set.of(10L));
                assertTrue(work.removed <= 1);
            }
            Assertions.assertEquals(Set.of(10L, 13L, 14L), indices(shelf));
            Assertions.assertEquals(14, shelf.getLatestSnapshot().getIndex());
            for (int i = 0; i < 4; i++) {
                shelf.cleanup(new RatisRetirementWork(options()), Set.of());
            }
            Assertions.assertEquals(Set.of(13L, 14L), indices(shelf));
        }
    }

    /**
     * When the latest file is damaged, the clean-up cannot go back to the old snapshot and continue to remove other evidence.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void corruptNewestStopsCleanupBeforeAnyOldFileDeletion() throws Exception {
        Path backup = backup();
        try (var storage = storage()) {
            var shelf = new RatisSnapshotStorage(group, limits());
            shelf.init(storage);
            publish(shelf, backup);
            Path newest = shelf.getLatestSnapshot().getFiles().getFirst().getPath();
            byte[] bytes = Files.readAllBytes(newest);
            bytes[bytes.length / 2] ^= 1;
            Files.write(newest, bytes);
            assertThrows(java.io.IOException.class, () -> shelf.cleanup(new RatisRetirementWork(options()), Set.of()));
            Assertions.assertEquals(5, indices(shelf).size());
        }
    }

    /**
     * The latest snapshot already seen by the live process disappears and old files cannot be remarked silently as lateest.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void missingLatestCannotSilentlyRegress() throws Exception {
        Path backup = backup();
        try (var storage = storage()) {
            var shelf = new RatisSnapshotStorage(group, limits());
            shelf.init(storage);
            publish(shelf, backup);
            Path newest = shelf.getLatestSnapshot().getFiles().getFirst().getPath();
            Files.delete(newest);
            assertThrows(java.io.IOException.class, () -> shelf.cleanup(new RatisRetirementWork(options()), Set.of()));
            Assertions.assertEquals(4, indices(shelf).size());
            Assertions.assertEquals(14, shelf.getLatestSnapshot().getIndex());
        }
    }

    /**
     * Using an official Raft storage directory, the location is given to a person and this feature does not pretend to be actual consensus generation snapshot.
     *
     * @return the operation result
     * @throws Exception if the operation cannot complete
     */
    private RaftStorage storage() throws Exception {
        var storage = RaftStorage.newBuilder().setDirectory(directory.resolve("raft").toFile()).setOption(RaftStorage.StartupOption.FORMAT)
                .setStorageFreeSpaceMin(org.apache.ratis.util.SizeInBytes.valueOf("1MB")).build();
        storage.initialize();
        return storage;
    }

    /**
     * Five successive legal seals for verify retention policy.
     *
     * @param shelf shelf
     * @param backup backup
     * @throws Exception if the operation cannot complete
     */
    private void publish(final RatisSnapshotStorage shelf, final Path backup) throws Exception {
        var config = RaftConfigurationProto.newBuilder();
        for (int i = 0; i < 3; i++) {
            config.addPeers(RaftPeer.newBuilder().setId("n" + i).setAddress("127.0.0.1:" + (19001 + i)).build().getRaftPeerProto());
        }
        for (int i = 10; i <= 14; i++) {
            var position = TermIndex.valueOf(3, i);
            Path bundle = directory.resolve("bundle-" + i);
            RatisSnapshotBundle.pack(backup, bundle, group, position, 0, config.build(), limits());
            shelf.publish(bundle, position);
        }
    }

    /**
     * The original file count is independent of the cache value returned by shelf.
     *
     * @param shelf shelf
     * @return the operation result
     * @throws Exception if the operation cannot complete
     */
    private static Set<Long> indices(final RatisSnapshotStorage shelf) throws Exception {
        var result = new HashSet<Long>();
        try (var files = Files.list(shelf.getSnapshotDir().toPath())) {
            for (Path path : files.toList()) {
                if (org.apache.ratis.statemachine.impl.SimpleStateMachineStorage.SNAPSHOT_REGEX.matcher(path.getFileName().toString()).matches()) {
                    result.add(org.apache.ratis.statemachine.impl.SimpleStateMachineStorage.getTermIndexFromSnapshotFile(path.toFile()).getIndex());
                }
            }
        }
        return result;
    }

    /**
     * The real root image of the brush is used as an input, and the code does not use handmade business files.
     *
     * @return the operation result
     * @throws Exception if the operation cannot complete
     */
    private Path backup() throws Exception {
        var authority = new Authority();
        Path backup = directory.resolve("backup");
        var identity = new WalReplicaIdentity(group.getUuid(), group.getUuid(), "source");
        try (var core = WalMvccRuntime.openReplica(directory.resolve("source"), Set.of("a"), Map.of(), WalOptions.defaults(), WalRetentionOptions.preserveAll(), identity, authority)) {
            authority.core = core;
            var tx = core.begin(IsolationLevel.REPEATABLE_READ);
            core.put(tx, "a", "k", new byte[]{1});
            core.commit(tx);
            core.backupReplica(authority, backup, limits());
        }
        return backup;
    }

    /**
     * Deletes the upper limit to include files and directories.
     *
     * @return the operation result
     */
    private static RatisRetirementOptions options() {
        return new RatisRetirementOptions(1, 1, 512, 5000, 2);
    }

    /**
     * The current input is small and limited in total.
     *
     * @return the operation result
     */
    private static WalBackupOptions limits() {
        return new WalBackupOptions(16_000_000, 8, 64_000, 60_000);
    }

    /**
     * The controlled port only provides a valid backup input.
     */
    private static final class Authority implements WalCommitAuthority {

        WalMvccRuntime core;

        /**
         * Fixed single term.
         *
         * @return the operation result
         */
        @Override
        public long epoch() {
            return 1;
        }

        /**
         * Do not access the network.
         *
         * @param timeout timeout
         * @return the operation result
         */
        @Override
        public long readBarrier(final long timeout) {
            return 1;
        }

        /**
         * Confirmation after installation has been completed.
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
}
