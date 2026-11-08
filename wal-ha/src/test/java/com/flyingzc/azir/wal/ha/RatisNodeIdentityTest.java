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

import com.flyingzc.azir.wal.WalCommit;
import com.flyingzc.azir.wal.WalCommitAuthority;
import com.flyingzc.azir.wal.WalMvccRuntime;
import com.flyingzc.azir.wal.WalOptions;
import com.flyingzc.azir.wal.WalReplicaIdentity;
import com.flyingzc.azir.wal.WalRetentionOptions;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.Assertions;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * The error group directory cannot replace the voting status of this group, which must fail before starting the server.
 */
class RatisNodeIdentityTest {

    @TempDir
    Path directory;

    /**
     * Even if another group has the same name, raft-meta cannot reformat the already running group vote identity.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void anotherGroupsMetadataCannotAuthorizeRecoveringAnExistingReplica() throws Exception {
        var group = group();
        var identity = new WalReplicaIdentity(group.getGroupId().getUuid(), group.getGroupId().getUuid(), "n0");
        try (var ignored = WalMvccRuntime.openReplica(directory.resolve("wal"), Set.of("a"), Map.of(), WalOptions.defaults(), WalRetentionOptions.preserveAll(), identity, new Unavailable())) {
        }
        byte[] engine = Files.readAllBytes(directory.resolve("wal/ENGINE"));
        Path wrong = directory.resolve("raft").resolve(UUID.randomUUID().toString()).resolve("current/raft-meta");
        Files.createDirectories(wrong.getParent());
        Files.writeString(wrong, "unrelated");
        assertThrows(IOException.class, () -> open(group));
        assertArrayEquals(engine, Files.readAllBytes(directory.resolve("wal/ENGINE")));
        assertFalse(Files.exists(directory.resolve("raft").resolve(group.getGroupId().getUuid().toString())));
    }

    /**
     * There is only a log without an old catalogue of voting metadata, which cannot be re-voted as a new replica.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void orphanRaftFilesAreNotAValidFreshNode() throws Exception {
        var group = group();
        Path segment = directory.resolve("raft").resolve(group.getGroupId().getUuid().toString()).resolve("current/log_inprogress_0");
        Files.createDirectories(segment.getParent());
        Files.writeString(segment, "old-log");
        assertThrows(IOException.class, () -> open(group));
        assertFalse(Files.exists(directory.resolve("wal")));
        Assertions.assertEquals("old-log", Files.readString(segment));
    }

    /**
     * Retention of voting status and loss of operations WAL similarly refuses to use empty operational status as a normal replica service.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void votingStateWithoutMvccCannotCreateAnEmptyEngine() throws Exception {
        var group = group();
        Path metadata = directory.resolve("raft").resolve(group.getGroupId().getUuid().toString()).resolve("current/raft-meta");
        Files.createDirectories(metadata.getParent());
        Files.writeString(metadata, "guard-only");
        assertThrows(IOException.class, () -> open(group));
        assertFalse(Files.exists(directory.resolve("wal")));
    }

    /**
     * Path link cannot lend the voting status of another directory to the current replica.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void linkedVotingGroupDirectoryIsRejected() throws Exception {
        var group = group();
        Path other = directory.resolve("other/current");
        Files.createDirectories(other);
        Files.writeString(other.resolve("raft-meta"), "guard-only");
        Files.createDirectories(directory.resolve("raft"));
        Files.createSymbolicLink(directory.resolve("raft").resolve(group.getGroupId().getUuid().toString()), directory.resolve("other"));
        assertThrows(IOException.class, () -> open(group));
        assertFalse(Files.exists(directory.resolve("wal")));
    }

    /**
     * The error must be detected before any network listening takes place, and the port value only meets the group configuration format.
     *
     * @param group group
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    private RatisWalNode open(final RaftGroup group) throws IOException {
        return RatisWalNode.openLoopback(directory, RaftPeerId.valueOf("n0"), group, Set.of("a"), Map.of(), WalOptions.defaults());
    }

    /**
     * All error scenes use a stand-alone identity to avoid the same voting identity cross-test reuse.
     *
     * @return the operation result
     */
    private static RaftGroup group() {
        return RaftGroup.valueOf(RaftGroupId.valueOf(UUID.randomUUID()), List.of(
                RaftPeer.newBuilder().setId("n0").setAddress("127.0.0.1:19001").build(),
                RaftPeer.newBuilder().setId("n1").setAddress("127.0.0.1:19002").build(),
                RaftPeer.newBuilder().setId("n2").setAddress("127.0.0.1:19003").build()));
    }

    /**
     * Only durable mode tags are created, and no real majority capability is simulated in this test.
     */
    private static final class Unavailable implements WalCommitAuthority {

        /**
         * There is no leader.
         *
         * @return the operation result
         */
        @Override
        public long epoch() {
            return 0;
        }

        /**
         * Error directory test does not create business snapshot.
         *
         * @param timeoutMillis timeout millis
         * @return the operation result
         * @throws IOException if the operation cannot complete
         */
        @Override
        public long readBarrier(final long timeoutMillis) throws IOException {
            throw new IOException("not running");
        }

        /**
         * The error directory test does not allow commit.
         *
         * @param epoch epoch
         * @param commits commits
         * @return the operation result
         */
        @Override
        public CompletionStage<Void> submit(final long epoch, final List<WalCommit> commits) {
            return CompletableFuture.failedFuture(new IOException("not running"));
        }
    }
}
