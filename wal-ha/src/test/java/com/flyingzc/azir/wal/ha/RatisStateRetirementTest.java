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
import com.flyingzc.azir.wal.WalReceipt;
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
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Assertions;

import com.flyingzc.azir.common.IsolationLevel;

import org.apache.ratis.proto.RaftProtos.RaftConfigurationProto;
import org.apache.ratis.server.protocol.TermIndex;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import java.io.IOException;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Current generation protection, file budget, durable retirement continuation and pointer retreat reject specifics.
 */
class RatisStateRetirementTest {

    @TempDir
    Path directory;

    private final UUID group = UUID.randomUUID();

    private final WalReplicaIdentity identity = new WalReplicaIdentity(group, group, "target");

    private final TermIndex position = TermIndex.valueOf(3, 10);

    private final List<WalReceipt> receipts = new ArrayList<>();

    /**
     * One file per round can also be completed, the current generation data and all receipt retentions cannot be revived by an old pointer.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void boundedFilesProtectCurrentAndRetiredPointerCannotReopen() throws Exception {
        Path bundle = bundle();
        Path root = directory.resolve("target");
        var authority = new Authority();
        RatisGenerationRecord old;
        try (var state = state(root, authority, stage -> {
        })) {
            authority.state = state;
            state.initialize(null, null);
            old = RatisGenerationRecord.read(root.resolve("ACTIVE"));
            state.initialize(bundle, position);
            var current = RatisGenerationRecord.read(root.resolve("ACTIVE"));
            Assertions.assertNotEquals(old.generation(), current.generation());
            Path scratch = root.resolve(".record-" + UUID.randomUUID());
            Files.write(scratch, new byte[]{1});
            Path note = root.resolve("user-note");
            Files.writeString(note, "keep");
            finish(state, 1);
            assertFalse(Files.exists(root.resolve("generation-" + old.generation())));
            assertTrue(Files.exists(root.resolve("generation-" + current.generation())));
            assertFalse(Files.exists(scratch));
            Assertions.assertEquals("keep", Files.readString(note));
            assertTrue(Files.exists(root.resolve("retired").resolve(old.generation().toString())));
            verify(state);
            old.publish(root.resolve("ACTIVE"));
        }
        assertThrows(IOException.class, () -> state(root, authority, stage -> {
        }));
        assertFalse(Files.exists(root.resolve("generation-" + old.generation())));
    }

    /**
     * Logs, backend progress, actual deletion and tombstone failure can all be restarted. Another generation cannot be deleted.
     *
     * @param wanted wanted
     * @throws Exception if the operation cannot complete
     */
    @ParameterizedTest
    @EnumSource(RatisWalState.RetirementStage.class)
    void interruptionResumesSameRetirement(final RatisWalState.RetirementStage wanted) throws Exception {
        Path bundle = bundle();
        Path root = directory.resolve("target");
        var authority = new Authority();
        var armed = new AtomicBoolean(true);
        UUID old;
        try (var state = state(root, authority, stage -> {
            if (stage == wanted && armed.compareAndSet(true, false)) {
                throw new IOException("injected retirement " + wanted);
            }
        })) {
            authority.state = state;
            state.initialize(null, null);
            old = RatisGenerationRecord.read(root.resolve("ACTIVE")).generation();
            state.initialize(bundle, position);
            assertThrows(IOException.class, () -> state.retire(new RatisRetirementWork(options(128))));
            assertTrue(state.retirementPending());
        }
        try (var state = state(root, authority, stage -> {
        })) {
            authority.state = state;
            state.initialize(bundle, position);
            finish(state, 2);
            verify(state);
            assertFalse(state.retirementPending());
            assertFalse(Files.exists(root.resolve("generation-" + old)));
        }
    }

    /**
     * Do not delete current data when no old generation; unknown files cannot be returned to clean-up mask.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void activeGenerationAndUnknownFilesAreProtected() throws Exception {
        Path bundle = bundle();
        Path root = directory.resolve("target");
        var authority = new Authority();
        try (var state = state(root, authority, stage -> {
        })) {
            authority.state = state;
            state.initialize(null, null);
            UUID old = RatisGenerationRecord.read(root.resolve("ACTIVE")).generation();
            var empty = new RatisRetirementWork(options(128));
            state.retire(empty);
            Assertions.assertEquals(0, empty.completed);
            assertTrue(Files.exists(root.resolve("generation-" + old)));
            state.initialize(bundle, position);
            Path unknown = root.resolve("generation-" + old + "/wal/user-file");
            Files.writeString(unknown, "keep");
            assertThrows(IOException.class, () -> state.retire(new RatisRetirementWork(options(128))));
            Assertions.assertEquals("keep", Files.readString(unknown));
            verify(state);
        }
    }

    /**
     * After taking protection collections, installation must wait for the actual removal phase to exit and cannot be replaced with generation during maintenance.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void snapshotProtectionRemainsLockedThroughCleanup() throws Exception {
        Path bundle = bundle();
        var authority = new Authority();
        try (var state = state(directory.resolve("target"), authority, stage -> {
        }); var executor = Executors.newSingleThreadExecutor()) {
            authority.state = state;
            state.initialize(bundle, position);
            var entered = new CountDownLatch(1);
            var task = new java.util.concurrent.atomic.AtomicReference<Future<?>>();
            state.cleanupSnapshots(new RatisRetirementWork(options(128)), (work, protectedIndices) -> {
                Assertions.assertEquals(Set.of(position.getIndex()), protectedIndices);
                task.set(executor.submit(() -> {
                    entered.countDown();
                    state.initialize(bundle, position);
                    return null;
                }));
                try {
                    assertTrue(entered.await(5, TimeUnit.SECONDS));
                    assertThrows(TimeoutException.class, () -> task.get().get(100, TimeUnit.MILLISECONDS));
                    assertTrue(state.ready());
                } catch (final InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IOException(interrupted);
                }
            });
            task.get().get(30, TimeUnit.SECONDS);
            verify(state);
        }
    }

    /**
     * Repeat maintenance using real return count, budget without exhausting the next round to continue.
     *
     * @param state state
     * @param files files
     * @throws AssertionError if the operation cannot complete
     * @throws Exception if the operation cannot complete
     */
    private static void finish(final RatisWalState state, final int files) throws Exception {
        for (int i = 0; i < 100; i++) {
            var work = new RatisRetirementWork(options(files));
            state.retire(work);
            assertTrue(work.removed <= files);
            if (!state.retirementPending() && work.completed > 0) {
                return;
            }
        }
        throw new AssertionError("retirement did not make bounded progress");
    }

    /**
     * The kernel can still read two pieces of data and two permanent receipt.
     *
     * @param state state
     */
    private void verify(final RatisWalState state) {
        var tx = state.core().begin(IsolationLevel.REPEATABLE_READ);
        var view = state.core().beginReadStatement(tx);
        for (String shard : List.of("a", "b")) {
            assertArrayEquals(new byte[]{2}, state.core().get(tx, shard, "k", view).orElseThrow());
        }
        state.core().rollback(tx);
        Assertions.assertEquals(2, state.core().stats().publishedSequence());
        for (var receipt : receipts) {
            Assertions.assertEquals(receipt, state.core().findCommitted(receipt.transactionId()).orElseThrow());
        }
    }

    /**
     * Limited number of scanned and single-wheeled files, with time reserved for desktop tasks other than test containers.
     *
     * @param files files
     * @return the operation result
     */
    private static RatisRetirementOptions options(final int files) {
        return new RatisRetirementOptions(1, files, 512, 5000, 2);
    }

    /**
     * The verify retirement boundary in the package is not injected into the official assembly structure.
     *
     * @param root root
     * @param authority authority
     * @param hook hook
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    private RatisWalState state(final Path root, final Authority authority, final RatisWalState.RetirementHook hook) throws IOException {
        return new RatisWalState(root, identity, Set.of("a", "b"), WalOptions.defaults(), authority, WalReplicaBackendFactory.localOnly(), limits(), stage -> {
        }, hook);
    }

    /**
     * Constructs a formal WAL backup, and the controlled port does not pretend to be production consensus.
     *
     * @return the operation result
     * @throws Exception if the operation cannot complete
     */
    private Path bundle() throws Exception {
        var authority = new Authority();
        Path backup = directory.resolve("backup");
        try (
                var source = WalMvccRuntime.openReplica(directory.resolve("source"), Set.of("a", "b"), Map.of(), WalOptions.defaults(), WalRetentionOptions.preserveAll(),
                        new WalReplicaIdentity(group, group, "source"), authority)) {
            authority.source = source;
            for (int value = 1; value <= 2; value++) {
                var tx = source.begin(IsolationLevel.REPEATABLE_READ);
                for (String shard : List.of("a", "b")) {
                    source.put(tx, shard, "k", new byte[]{(byte) value});
                }
                receipts.add(source.commit(tx));
            }
            source.backupReplica(authority, backup, limits());
        }
        var config = RaftConfigurationProto.newBuilder();
        for (int i = 0; i < 3; i++) {
            config.addPeers(RaftPeer.newBuilder().setId("n" + i).setAddress("127.0.0.1:" + (19001 + i)).build().getRaftPeerProto());
        }
        Path bundle = directory.resolve("snapshot.bundle");
        RatisSnapshotBundle.pack(backup, bundle, RaftGroupId.valueOf(group), position, 0, config.build(), limits());
        return bundle;
    }

    /**
     * There is a limited byte and time per wheeled mirror operation budget.
     *
     * @return the operation result
     */
    private static WalBackupOptions limits() {
        return new WalBackupOptions(16_000_000, 2, 64_000, 60_000);
    }

    /**
     * Only commit local records are provided for the retirement protocol instead of a real election test.
     */
    private static final class Authority implements WalCommitAuthority {

        RatisWalState state;

        WalMvccRuntime source;

        /**
         * Business term is not provided when installation is not ready.
         *
         * @return the operation result
         */
        @Override
        public long epoch() {
            return state == null || state.ready() ? 1 : 0;
        }

        /**
         * No network requests are detected.
         *
         * @param timeout timeout
         * @return the operation result
         */
        @Override
        public long readBarrier(final long timeout) {
            return epoch();
        }

        /**
         * Install real WAL decision to confirm later.
         *
         * @param epoch epoch
         * @param commits commits
         * @return the operation result
         */
        @Override
        public CompletionStage<Void> submit(final long epoch, final List<WalCommit> commits) {
            (state == null ? source : state.core()).applyCommitted(this, commits);
            return CompletableFuture.completedFuture(null);
        }
    }
}
