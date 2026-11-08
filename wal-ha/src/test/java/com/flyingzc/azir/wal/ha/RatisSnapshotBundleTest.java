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

import com.flyingzc.azir.wal.WalBackup;
import com.flyingzc.azir.wal.WalBackupOptions;
import com.flyingzc.azir.wal.WalCommit;
import com.flyingzc.azir.wal.WalCommitAuthority;
import com.flyingzc.azir.wal.WalMvccRuntime;
import com.flyingzc.azir.wal.WalOptions;
import com.flyingzc.azir.wal.WalReplicaIdentity;
import com.flyingzc.azir.wal.WalRetentionOptions;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Assertions;

import com.flyingzc.azir.common.IsolationLevel;

import org.apache.ratis.proto.RaftProtos.RaftConfigurationProto;

import org.apache.ratis.server.protocol.TermIndex;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * The metadata and content of a separate verify snapshot file are bound, and bad input cannot release the installed directory.
 */
class RatisSnapshotBundleTest {

    @TempDir
    Path directory;

    private final RaftGroupId group = RaftGroupId.randomId();

    private final TermIndex position = TermIndex.valueOf(3, 10);

    /**
     * The full package can still bind the new replica identity recovery, and business serial numbers cannot be confused with the Raft index.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void bundleBindsRaftPositionConfigurationAndCompleteBusinessState() throws Exception {
        Path backup = backup();
        Path bundle = directory.resolve("snapshot.bin");
        var configuration = configuration();
        var packed = RatisSnapshotBundle.pack(backup, bundle, group, position, 0, configuration, limits());
        Assertions.assertEquals(1, packed.boundary().sequence());
        Assertions.assertEquals(10, packed.position().getIndex());
        Assertions.assertEquals(configuration, RatisSnapshotBundle.inspect(bundle, group, position, limits()).configuration());
        Path extracted = directory.resolve("extracted");
        Assertions.assertEquals(packed, RatisSnapshotBundle.extract(bundle, extracted, group, position, limits()));
        Path restored = directory.resolve("restored");
        var identity = new WalReplicaIdentity(group.getUuid(), group.getUuid(), "receiver");
        WalBackup.restoreReplica(extracted, restored, Map.of(), WalOptions.defaults(), limits(), identity);
        var authority = new Authority();
        try (var runtime = WalMvccRuntime.openReplica(restored, Set.of("a", "b"), Map.of(), WalOptions.defaults(), WalRetentionOptions.preserveAll(), identity, authority)) {
            authority.runtime = runtime;
            var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
            var view = runtime.beginReadStatement(tx);
            Assertions.assertEquals("a-value", new String(runtime.get(tx, "a", "k", view).orElseThrow(), StandardCharsets.UTF_8));
            Assertions.assertEquals("b-value", new String(runtime.get(tx, "b", "k", view).orElseThrow(), StandardCharsets.UTF_8));
            runtime.rollback(tx);
        }
    }

    /**
     * The actual location and grouping of the installation came from a consensus agreement and could not be accepted for another group or different positions of the same group.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void wrongGroupPositionAndOccupiedTargetAreRejected() throws Exception {
        Path backup = backup();
        Path bundle = directory.resolve("snapshot.bin");
        RatisSnapshotBundle.pack(backup, bundle, group, position, 0, configuration(), limits());
        Path target = directory.resolve("target");
        assertThrows(IOException.class, () -> RatisSnapshotBundle.extract(bundle, target, RaftGroupId.randomId(), position, limits()));
        assertFalse(Files.exists(target));
        assertThrows(IOException.class, () -> RatisSnapshotBundle.extract(bundle, target, group, TermIndex.valueOf(3, 11), limits()));
        assertFalse(Files.exists(target));
        Files.createDirectory(target);
        Files.writeString(target.resolve("user-file"), "keep");
        assertThrows(IOException.class, () -> RatisSnapshotBundle.extract(bundle, target, group, position, limits()));
        Assertions.assertEquals("keep", Files.readString(target.resolve("user-file")));
    }

    /**
     * None of the intercepts, data flipping or tail multibytes can be sealed by validate, and no target is published.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void corruptionTruncationAndTrailingBytesCannotPublish() throws Exception {
        Path backup = backup();
        Path bundle = directory.resolve("snapshot.bin");
        RatisSnapshotBundle.pack(backup, bundle, group, position, 0, configuration(), limits());
        byte[] good = Files.readAllBytes(bundle);
        var corrupt = good.clone();
        corrupt[corrupt.length / 2] ^= 1;
        for (byte[] bytes : List.of(Arrays.copyOf(good, 24), Arrays.copyOf(good, good.length - 1), Arrays.copyOf(good, good.length + 1), corrupt)) {
            Files.write(bundle, bytes);
            Path target = directory.resolve("rejected-" + UUID.randomUUID());
            assertThrows(IOException.class, () -> RatisSnapshotBundle.extract(bundle, target, group, position, limits()));
            assertFalse(Files.exists(target));
        }
    }

    /**
     * Even if the outer summary is recalculated, the path through and forgery of the operational boundary will be rejected by format and internal mirror validate.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void recomputedChecksumCannotAuthorizeTraversalOrFalseBusinessBoundary() throws Exception {
        Path backup = backup();
        Path bundle = directory.resolve("snapshot.bin");
        RatisSnapshotBundle.pack(backup, bundle, group, position, 0, configuration(), limits());
        byte[] good = Files.readAllBytes(bundle);
        byte[] traversal = good.clone();
        byte[] token = "COMPLETE".getBytes(StandardCharsets.US_ASCII);
        int offset = -1;
        for (int i = 0; i <= traversal.length - token.length; i++) {
            if (Arrays.equals(traversal, i, i + token.length, token, 0, token.length)) {
                offset = i;
                break;
            }
        }
        assertTrue(offset > 0);
        System.arraycopy("../evil!".getBytes(StandardCharsets.US_ASCII), 0, traversal, offset, token.length);
        checksum(traversal);
        Files.write(bundle, traversal);
        Path target = directory.resolve("traversal-target");
        assertThrows(IOException.class, () -> RatisSnapshotBundle.extract(bundle, target, group, position, limits()));
        assertFalse(Files.exists(target));
        assertFalse(Files.exists(directory.resolve("evil!")));
        byte[] forged = good.clone();
        int configurationLength = ByteBuffer.wrap(forged).getInt(48);
        int sequenceOffset = 52 + configurationLength + 16;
        ByteBuffer.wrap(forged).putLong(sequenceOffset, 999);
        checksum(forged);
        Files.write(bundle, forged);
        Path wrong = directory.resolve("wrong-sequence");
        assertThrows(IOException.class, () -> RatisSnapshotBundle.extract(bundle, wrong, group, position, limits()));
        assertFalse(Files.exists(wrong));
        try (var paths = Files.list(directory)) {
            assertFalse(paths.anyMatch(path -> path.getFileName().toString().startsWith(".azir-unpack-")));
        }
    }

    /**
     * Encoding headers and configurations also consume bytes budget; resources refuse not to overwrite original files or influence source backup.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void limitsUnknownFilesAndInvalidConfigurationFailClosed() throws Exception {
        Path backup = backup();
        Path bundle = directory.resolve("snapshot.bin");
        long bytes = WalBackup.inspect(backup, limits()).bytes();
        assertTrue(bytes >= 1024);
        assertThrows(IOException.class, () -> RatisSnapshotBundle.pack(backup, bundle, group, position, 0, configuration(), new WalBackupOptions(bytes + 1, 16, 1024, 60_000)));
        assertTrue(Files.exists(bundle));
        assertThrows(IOException.class, () -> RatisSnapshotBundle.inspect(bundle, group, position, limits()));
        Assertions.assertEquals(1, WalBackup.inspect(backup, limits()).sequence());
        Path invalid = directory.resolve("invalid.bin");
        assertThrows(IllegalArgumentException.class, () -> RatisSnapshotBundle.pack(backup, invalid, group, position, 11, configuration(), limits()));
        assertFalse(Files.exists(invalid));
        Files.writeString(backup.resolve("local/unknown-file"), "must not travel");
        assertThrows(IOException.class, () -> RatisSnapshotBundle.pack(backup, invalid, group, position, 0, configuration(), limits()));
        assertFalse(Files.exists(invalid));
    }

    /**
     * Creates replica mirrors with real WAL force and backup validate; fixed port is only encoded and not listening.
     *
     * @return the operation result
     * @throws Exception if the operation cannot complete
     */
    private Path backup() throws Exception {
        var identity = new WalReplicaIdentity(group.getUuid(), group.getUuid(), "source");
        var authority = new Authority();
        Path backup = directory.resolve("backup");
        try (var runtime = WalMvccRuntime.openReplica(directory.resolve("source"), Set.of("a", "b"), Map.of(), WalOptions.defaults(), WalRetentionOptions.preserveAll(), identity, authority)) {
            authority.runtime = runtime;
            var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
            runtime.put(tx, "a", "k", "a-value".getBytes(StandardCharsets.UTF_8));
            runtime.put(tx, "b", "k", "b-value".getBytes(StandardCharsets.UTF_8));
            runtime.commit(tx);
            runtime.backupReplica(authority, backup, limits());
        }
        return backup;
    }

    /**
     * The member configuration uses Ratis own protobuf to keep fields that can be changed by the member recovery.
     *
     * @return the operation result
     */
    private static RaftConfigurationProto configuration() {
        var builder = RaftConfigurationProto.newBuilder();
        for (int i = 0; i < 3; i++) {
            builder.addPeers(RaftPeer.newBuilder().setId("n" + i).setAddress("127.0.0.1:" + (19001 + i)).build().getRaftPeerProto());
        }
        return builder.build();
    }

    /**
     * Single maintenance bytes and time limits, with no compression package to expand the multiple breakout total budget.
     *
     * @return the operation result
     */
    private static WalBackupOptions limits() {
        return new WalBackupOptions(8_000_000, 16, 64_000, 60_000);
    }

    /**
     * The negative test simulates a malice sender with a legitimate outer summary, which cannot be measured only for random position flipping.
     *
     * @param bytes bytes
     * @throws Exception if the operation cannot complete
     */
    private static void checksum(final byte[] bytes) throws Exception {
        byte[] hash = java.security.MessageDigest.getInstance("SHA-256").digest(Arrays.copyOf(bytes, bytes.length - 32));
        System.arraycopy(hash, 0, bytes, bytes.length - 32, 32);
    }

    /**
     * The commit port tested in the unit only provides mirror input, not production consensus.
     */
    private static final class Authority implements WalCommitAuthority {

        WalMvccRuntime runtime;

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
         * Fixed single test barrier.
         *
         * @param timeoutMillis timeout millis
         * @return the operation result
         */
        @Override
        public long readBarrier(final long timeoutMillis) {
            return 1;
        }

        /**
         * First install complete batch and then complete single test commit.
         *
         * @param epoch epoch
         * @param commits commits
         * @return the operation result
         */
        @Override
        public CompletionStage<Void> submit(final long epoch, final List<WalCommit> commits) {
            runtime.applyCommitted(this, commits);
            return CompletableFuture.completedFuture(null);
        }
    }
}
