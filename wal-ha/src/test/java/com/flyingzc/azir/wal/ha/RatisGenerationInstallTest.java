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
import com.flyingzc.azir.wal.WalMaterializedStore;
import com.flyingzc.azir.wal.WalMvccRuntime;
import com.flyingzc.azir.wal.WalOptions;
import com.flyingzc.azir.wal.WalReplicaIdentity;
import com.flyingzc.azir.wal.WalRetentionOptions;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Assertions;

import com.flyingzc.azir.common.IsolationLevel;

import org.apache.ratis.proto.RaftProtos.RaftConfigurationProto;
import org.apache.ratis.server.protocol.TermIndex;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Actual durable generation border failure verify; location given for test, real Raft installation as a separate integration verify.
 */
class RatisGenerationInstallTest {

    @TempDir
    Path directory;

    private final RaftGroupId group = RaftGroupId.randomId();

    private final TermIndex position = TermIndex.valueOf(3, 10);

    /**
     * When records, recovery, pointers and operations fail to clear any boundary, re-opening can only continue with the same complete installation.
     *
     * @param stage stage
     * @throws Exception if the operation cannot complete
     */
    @ParameterizedTest
    @EnumSource(value = RatisWalState.Stage.class, names = {"BEFORE_INPUT_FORCE", "AFTER_INSTALL_RECORD", "AFTER_RESTORE", "BEFORE_ACTIVE", "AFTER_ACTIVE", "AFTER_CLEARED"})
    void crashAtInstallationBoundaryResumesSameGenerationWithoutServingOldState(final RatisWalState.Stage stage) throws Exception {
        Path bundle = bundle();
        Path root = directory.resolve("target");
        var identity = new WalReplicaIdentity(group.getUuid(), group.getUuid(), "target");
        var opens = new AtomicInteger();
        var closes = new AtomicInteger();
        WalReplicaBackendFactory factory = (owner, generation) -> {
            opens.incrementAndGet();
            return new WalReplicaBackendFactory.Lease(Map.of(), () -> closes.incrementAndGet());
        };
        var authority = new Authority();
        try (var state = new RatisWalState(root, identity, Set.of("a", "b"), WalOptions.defaults(), authority, factory, limits(), hit -> {
            if (hit == stage) {
                throw new IOException("injected " + stage);
            }
        })) {
            authority.state = state;
            state.initialize(null, null);
            assertTrue(state.ready());
            Assertions.assertEquals(0, state.core().stats().publishedSequence());
            assertThrows(IOException.class, () -> state.initialize(bundle, position));
            assertFalse(state.ready());
        }
        int before = opens.get();
        boolean pending = Files.exists(root.resolve("INSTALLING"));
        try (var state = new RatisWalState(root, identity, Set.of("a", "b"), WalOptions.defaults(), authority, factory, limits(), hit -> {
        })) {
            authority.state = state;
            assertFalse(state.ready());
            if (pending) {
                Assertions.assertEquals(before, opens.get(), "unfinished install must not open old generation");
            }
            state.initialize(bundle, position);
            assertTrue(state.ready());
            assertFalse(Files.exists(root.resolve("INSTALLING")));
            Assertions.assertEquals(2, state.core().stats().publishedSequence());
            var tx = state.core().begin(IsolationLevel.REPEATABLE_READ);
            var snapshot = state.core().beginReadStatement(tx);
            for (String shard : Set.of("a", "b")) {
                Assertions.assertEquals(shard + ":new", new String(state.core().get(tx, shard, "k", snapshot).orElseThrow(), StandardCharsets.UTF_8));
            }
            state.core().rollback(tx);
            var record = RatisGenerationRecord.read(root.resolve("ACTIVE"));
            Assertions.assertEquals(position.getIndex(), record.index());
            Assertions.assertEquals(2, record.sequence());
        }
        Assertions.assertEquals(opens.get(), closes.get(), "every owned backend lease must close");
    }

    /**
     * To the recovery node registration capability requirement, a real opening backend failure must be refused prior to import and service.
     *
     * @param requirement requirement
     * @throws Exception if the operation cannot complete
     */
    @ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = {"GC", "SNAPSHOT"})
    void pendingLeaseRequirementIsDeferredButEnforcedBeforeRestore(final String requirement) throws Exception {
        Path bundle = bundle(), root = directory.resolve("target");
        var identity = new WalReplicaIdentity(group.getUuid(), group.getUuid(), "target");
        var authority = new Authority();
        try (var state = new RatisWalState(root, identity, Set.of("a", "b"), WalOptions.defaults(), authority, WalReplicaBackendFactory.localOnly(), limits(), stage -> {
        })) {
            authority.state = state;
            state.initialize(null, null);
            state.pause();
            state.durableBeforeSnapshotAck(bundle, position);
        }
        var unsupported = (WalMaterializedStore) java.lang.reflect.Proxy.newProxyInstance(WalMaterializedStore.class.getClassLoader(), new Class<?>[]{WalMaterializedStore.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "supportsReplicaOwnership", "supportsReplicaRestore" -> true;
                    case "supportsBoundedMaintenance", "supportsPinnedSnapshot" -> false;
                    default -> throw new AssertionError("backend data accessed before capability validation: " + method.getName());
                });
        WalReplicaBackendFactory invalid = (owner, generation) -> new WalReplicaBackendFactory.Lease(Map.of("a", unsupported), () -> {
        });
        try (var state = new RatisWalState(root, identity, Set.of("a", "b"), WalOptions.defaults(), authority, invalid, limits(), stage -> {
        })) {
            authority.state = state;
            if (requirement.equals("GC")) {
                state.requireBoundedGc();
            } else {
                state.requirePinnedSnapshots();
            }
            var failure = assertThrows(IOException.class, () -> state.initialize(bundle, position));
            assertTrue(failure.getMessage().contains(requirement.equals("GC") ? "bounded version GC" : "automatic snapshots"));
            assertFalse(state.ready());
            assertTrue(Files.exists(root.resolve("INSTALLING")));
            Assertions.assertEquals(-1, RatisGenerationRecord.read(root.resolve("ACTIVE")).index());
        }
        try (var state = new RatisWalState(root, identity, Set.of("a", "b"), WalOptions.defaults(), authority, WalReplicaBackendFactory.localOnly(), limits(), stage -> {
        })) {
            authority.state = state;
            state.requireBoundedGc();
            state.requirePinnedSnapshots();
            state.initialize(bundle, position);
            assertTrue(state.ready());
            Assertions.assertEquals(2, state.core().stats().publishedSequence());
            assertFalse(Files.exists(root.resolve("INSTALLING")));
        }
    }

    /**
     * When the initial control record has disappeared, retry release must still be re-synchronized to a directory and cannot skip previous failed locations.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void retryCannotBecomeReadyBeforeDirectoryConfirmation() throws Exception {
        var identity = new WalReplicaIdentity(group.getUuid(), group.getUuid(), "target");
        var authority = new Authority();
        Path root = directory.resolve("target");
        var attempts = new AtomicInteger();
        try (var state = new RatisWalState(root, identity, Set.of("a", "b"), WalOptions.defaults(), authority, WalReplicaBackendFactory.localOnly(), limits(), stage -> {
            if (stage == RatisWalState.Stage.BEFORE_READY_DIRECTORY_FORCE && attempts.getAndIncrement() < 2) {
                throw new IOException("injected readiness directory force failure");
            }
        })) {
            authority.state = state;
            assertThrows(IOException.class, () -> state.initialize(null, null));
            assertFalse(state.ready());
            assertFalse(Files.exists(root.resolve("BOOTSTRAPPING")));
            assertThrows(IOException.class, () -> state.initialize(null, null));
            assertFalse(state.ready());
            Assertions.assertEquals(2, attempts.get());
            state.initialize(null, null);
            assertTrue(state.ready());
            Assertions.assertEquals(3, attempts.get());
        }
    }

    /**
     * ACTIVE has been completely replaced but the directory has failed to confirm, subsequent collection proves that it cannot be successful simply by reading a new record.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void visibleActiveRecordStillRequiresDurabilityBeforePurgeProof() throws Exception {
        Path first = bundle(), root = directory.resolve("target");
        var identity = new WalReplicaIdentity(group.getUuid(), group.getUuid(), "target");
        var authority = new Authority();
        var nextPosition = TermIndex.valueOf(3, 11);
        var oldHeader = RatisSnapshotBundle.inspect(first, group, position, limits());
        Path next = directory.resolve("next.bundle");
        var nextHeader = RatisSnapshotBundle.pack(directory.resolve("backup"), next, group, nextPosition, 0, oldHeader.configuration(), limits());
        try (var state = new RatisWalState(root, identity, Set.of("a", "b"), WalOptions.defaults(), authority, WalReplicaBackendFactory.localOnly(), limits(), stage -> {
        })) {
            authority.state = state;
            state.initialize(first, position);
            var note = state.prepareSnapshotNote(nextHeader, next);
            assertThrows(IOException.class, () -> state.noteSnapshot(note, new RatisMutableRecordIoTest.Fault(RatisMutableRecordIoTest.Failure.BEFORE_DIRECTORY_FORCE)));
            Assertions.assertEquals(11, RatisGenerationRecord.read(root.resolve("ACTIVE")).index());
            var failedProof = new RatisMutableRecordIoTest.Fault(RatisMutableRecordIoTest.Failure.BEFORE_DIRECTORY_FORCE);
            assertThrows(IOException.class, () -> state.activatedSnapshotCovers(11, failedProof));
            assertTrue(failedProof.fired);
            var confirmed = new RatisMutableRecordIoTest.Fault(null);
            assertTrue(state.activatedSnapshotCovers(11, confirmed));
            assertTrue(confirmed.operations.contains("FILE_FORCE"));
            assertTrue(confirmed.operations.contains("DIRECTORY_FORCE"));
            assertTrue(state.ready());
        }
    }

    /**
     * Damaged pointer and error node identity cannot trigger an empty initialization, and failure should release the directory lock.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void corruptPointerAndWrongNodeCannotBootstrapOrRetainWriterLock() throws Exception {
        Path root = directory.resolve("target");
        var identity = new WalReplicaIdentity(group.getUuid(), group.getUuid(), "target");
        var authority = new Authority();
        try (var state = new RatisWalState(root, identity, Set.of("a", "b"), WalOptions.defaults(), authority, WalReplicaBackendFactory.localOnly(), limits(), hit -> {
        })) {
            authority.state = state;
            state.initialize(null, null);
        }
        byte[] good = Files.readAllBytes(root.resolve("ACTIVE"));
        byte[] bad = good.clone();
        bad[bad.length - 1] ^= 1;
        Files.write(root.resolve("ACTIVE"), bad);
        assertThrows(IOException.class, () -> new RatisWalState(root, identity, Set.of("a", "b"), WalOptions.defaults(), authority, WalReplicaBackendFactory.localOnly(), limits(), hit -> {
        }));
        Files.write(root.resolve("ACTIVE"), good);
        var wrong = new WalReplicaIdentity(group.getUuid(), group.getUuid(), "other");
        assertThrows(IOException.class, () -> new RatisWalState(root, wrong, Set.of("a", "b"), WalOptions.defaults(), authority, WalReplicaBackendFactory.localOnly(), limits(), hit -> {
        }));
        try (var state = new RatisWalState(root, identity, Set.of("a", "b"), WalOptions.defaults(), authority, WalReplicaBackendFactory.localOnly(), limits(), hit -> {
        })) {
            authority.state = state;
            state.initialize(null, null);
            assertTrue(state.ready());
        }
    }

    /**
     * An independent durable input is required prior to successful confirmation and can be continued after the original receipt file has been lost; force cannot be successfully returned.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void snapshotAckRequiresDurableIndependentRecoveryInput() throws Exception {
        Path bundle = bundle();
        Path failingInput = directory.resolve("force-failure.bundle");
        Files.copy(bundle, failingInput);
        var identity = new WalReplicaIdentity(group.getUuid(), group.getUuid(), "target");
        var authority = new Authority();
        Path root = directory.resolve("target");
        Path durable;
        try (var state = new RatisWalState(root, identity, Set.of("a", "b"), WalOptions.defaults(), authority, WalReplicaBackendFactory.localOnly(), limits(), hit -> {
        })) {
            authority.state = state;
            state.initialize(null, null);
            state.pause();
            state.durableBeforeSnapshotAck(bundle, position);
            assertFalse(state.ready());
            assertTrue(Files.exists(root.resolve("INSTALLING")));
            durable = state.pendingBundle();
            assertNotNull(durable);
            Assertions.assertEquals(position, RatisSnapshotBundle.inspect(durable, group, position, limits()).position());
            Files.delete(bundle);
        }
        try (var state = new RatisWalState(root, identity, Set.of("a", "b"), WalOptions.defaults(), authority, WalReplicaBackendFactory.localOnly(), limits(), hit -> {
        })) {
            authority.state = state;
            state.initialize(durable, position);
            assertTrue(state.ready());
            Assertions.assertEquals(2, state.core().stats().publishedSequence());
        }
        Path failureRoot = directory.resolve("failed-target");
        try (var state = new RatisWalState(failureRoot, identity, Set.of("a", "b"), WalOptions.defaults(), authority, WalReplicaBackendFactory.localOnly(), limits(), hit -> {
            if (hit == RatisWalState.Stage.BEFORE_INPUT_FORCE) {
                throw new IOException("cannot force received input");
            }
        })) {
            authority.state = state;
            state.initialize(null, null);
            state.pause();
            assertThrows(IOException.class, () -> state.durableBeforeSnapshotAck(failingInput, position));
            assertFalse(Files.exists(failureRoot.resolve("INSTALLING")));
            assertFalse(state.ready());
        }
    }

    /**
     * The outer directory lock and backend lease cannot be released when the closure is interrupted, and retry will not be closed until the break has been cleared.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void interruptedCoreCloseKeepsGenerationOwnershipUntilActualDrain() throws Exception {
        var identity = new WalReplicaIdentity(group.getUuid(), group.getUuid(), "target");
        var authority = new Authority();
        Path root = directory.resolve("target");
        var released = new AtomicInteger();
        WalReplicaBackendFactory factory = (owner, generation) -> new WalReplicaBackendFactory.Lease(Map.of(), () -> released.incrementAndGet());
        var state = new RatisWalState(root, identity, Set.of("a", "b"), WalOptions.defaults(), authority, factory, limits(), hit -> {
        });
        authority.state = state;
        state.initialize(null, null);
        try (var tasks = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            authority.blocked = new CompletableFuture<>();
            authority.submitted = new java.util.concurrent.CountDownLatch(1);
            var tx = state.core().begin(IsolationLevel.REPEATABLE_READ);
            state.core().put(tx, "a", "pending", new byte[]{1});
            var committing = tasks.submit(() -> state.core().commit(tx));
            assertTrue(authority.submitted.await(5, java.util.concurrent.TimeUnit.SECONDS));
            Thread.currentThread().interrupt();
            try {
                assertThrows(IllegalStateException.class, state::close);
            } finally {
                Thread.interrupted();
            }
            Assertions.assertEquals(0, released.get());
            assertFalse(state.ready());
            assertThrows(IOException.class, () -> new RatisWalState(root, identity, Set.of("a", "b"), WalOptions.defaults(), authority, factory, limits(), hit -> {
            }));
            authority.blocked.completeExceptionally(new IOException("release test proposal"));
            assertThrows(java.util.concurrent.ExecutionException.class, () -> committing.get(5, java.util.concurrent.TimeUnit.SECONDS));
        } finally {
            Thread.interrupted();
            if (authority.blocked != null) {
                authority.blocked.completeExceptionally(new IOException("test cleanup"));
            }
            state.close();
        }
        Assertions.assertEquals(1, released.get());
    }

    /**
     * Backend lease retains directory ownership when it fails to release, and only after retry is successful will the handle be emptied and the lock released.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void failedBackendReleaseKeepsLeaseAndDirectoryUntilRetry() throws Exception {
        var identity = new WalReplicaIdentity(group.getUuid(), group.getUuid(), "target");
        var authority = new Authority();
        var attempts = new AtomicInteger();
        Path root = directory.resolve("release-failure");
        WalReplicaBackendFactory factory = (owner, generation) -> new WalReplicaBackendFactory.Lease(Map.of(), () -> {
            if (attempts.incrementAndGet() == 1) {
                throw new IOException("lease release failed");
            }
        });
        var state = new RatisWalState(root, identity, Set.of("a", "b"), WalOptions.defaults(), authority, factory, limits(), hit -> {
        });
        try {
            assertThrows(IOException.class, state::close);
            assertThrows(IOException.class, () -> new RatisWalState(root, identity, Set.of("a", "b"), WalOptions.defaults(), authority, WalReplicaBackendFactory.localOnly(), limits(), hit -> {
            }));
            state.close();
            state.close();
            Assertions.assertEquals(2, attempts.get());
        } finally {
            state.close();
        }
    }

    /**
     * The backend cleanup handle is also maintained when the original node has not returned, and does not need to start a Raft or database verify directory lock liability.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void rawNodeStartupFailureRetainsCleanupForAnUnreturnedGeneration() throws Exception {
        var peers = List.of(RaftPeer.newBuilder().setId("target").setAddress("127.0.0.1:19001").build(),
                RaftPeer.newBuilder().setId("peer-b").setAddress("127.0.0.1:19002").build(),
                RaftPeer.newBuilder().setId("peer-c").setAddress("127.0.0.1:19003").build());
        var configuration = RaftGroup.valueOf(group, peers);
        var allowed = new java.util.concurrent.atomic.AtomicBoolean();
        Path root = directory.resolve("partial-node");
        WalReplicaBackendFactory factory = (owner, generation) -> {
            throw new RatisStartupException(new IOException("backend open failed"), () -> {
                if (!allowed.get()) {
                    throw new IOException("backend cleanup pending");
                }
            });
        };
        var pending = assertThrows(RatisStartupException.class, () -> RatisWalNode.openGenerationalLoopback(root, peers.getFirst().getId(), configuration,
                Set.of("a", "b"), factory, WalOptions.defaults(), limits()));
        var identity = new WalReplicaIdentity(group.getUuid(), group.getUuid(), "target");
        try {
            assertTrue(pending.cleanupPending());
            assertThrows(IOException.class, pending::retryCleanup);
            assertThrows(IOException.class,
                    () -> new RatisWalState(root.resolve("business"), identity, Set.of("a", "b"), WalOptions.defaults(), new Authority(), WalReplicaBackendFactory.localOnly(), limits(), hit -> {
                    }));
        } finally {
            allowed.set(true);
            pending.retryCleanup();
        }
        assertFalse(pending.cleanupPending());
        try (var reopened = new RatisWalState(root.resolve("business"), identity, Set.of("a", "b"), WalOptions.defaults(), new Authority(), WalReplicaBackendFactory.localOnly(), limits(), hit -> {
        })) {
            assertNotNull(reopened.core());
        }
    }

    /**
     * Construct completed business mirrors and seals, generation tests do not forge Raft majority certificates.
     *
     * @return the operation result
     * @throws Exception if the operation cannot complete
     */
    private Path bundle() throws Exception {
        var identity = new WalReplicaIdentity(group.getUuid(), group.getUuid(), "source");
        var authority = new Authority();
        Path backup = directory.resolve("backup");
        try (var core = WalMvccRuntime.openReplica(directory.resolve("source"), Set.of("a", "b"), Map.of(), WalOptions.defaults(), WalRetentionOptions.preserveAll(), identity, authority)) {
            authority.source = core;
            for (String value : List.of("old", "new")) {
                var tx = core.begin(IsolationLevel.REPEATABLE_READ);
                for (String shard : Set.of("a", "b")) {
                    core.put(tx, shard, "k", (shard + ":" + value).getBytes(StandardCharsets.UTF_8));
                }
                core.commit(tx);
            }
            core.backupReplica(authority, backup, limits());
        }
        var config = RaftConfigurationProto.newBuilder();
        for (int i = 0; i < 3; i++) {
            config.addPeers(RaftPeer.newBuilder().setId("n" + i).setAddress("127.0.0.1:" + (19001 + i)).build().getRaftPeerProto());
        }
        Path bundle = directory.resolve("snapshot.bundle");
        RatisSnapshotBundle.pack(backup, bundle, group, position, 0, config.build(), limits());
        return bundle;
    }

    /**
     * Each operation has a limited I/O time and total byte.
     *
     * @return the operation result
     */
    private static WalBackupOptions limits() {
        return new WalBackupOptions(16_000_000, 2, 64_000, 60_000);
    }

    /**
     * Controlled ports are only used for generation protocols and return to zero terms during installation.
     */
    private static final class Authority implements WalCommitAuthority {

        RatisWalState state;

        WalMvccRuntime source;

        CompletableFuture<Void> blocked;

        java.util.concurrent.CountDownLatch submitted;

        /**
         * Generation is not ready to create a business handle.
         *
         * @return the operation result
         */
        @Override
        public long epoch() {
            return state == null || state.ready() ? 1 : 0;
        }

        /**
         * The mirror does not access the network on a single basis.
         *
         * @param timeoutMillis timeout millis
         * @return the operation result
         */
        @Override
        public long readBarrier(final long timeoutMillis) {
            return epoch();
        }

        /**
         * Local has commit echoes do not represent production consensus.
         *
         * @param epoch epoch
         * @param commits commits
         * @return the operation result
         */
        @Override
        public CompletionStage<Void> submit(final long epoch, final List<WalCommit> commits) {
            if (blocked != null) {
                submitted.countDown();
                return blocked;
            }
            (state == null ? source : state.core()).applyCommitted(this, commits);
            return CompletableFuture.completedFuture(null);
        }
    }
}
