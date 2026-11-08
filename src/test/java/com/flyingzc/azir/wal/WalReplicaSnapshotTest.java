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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Assertions;

import com.flyingzc.azir.common.IsolationLevel;
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
import java.util.concurrent.atomic.AtomicInteger;
import static com.flyingzc.azir.wal.WalMvccRuntimeTest.*;

/**
 * Identity, recovery release and receipt verify for moving replica mirrors; controlled ports do not pretend to be true consensus.
 */
class WalReplicaSnapshotTest {

    @TempDir
    Path directory;

    /**
     * Receiving node with its own identity, data, tombstone and full confirmation of receipt retention, then continue with the same business serial number.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void snapshotRebindsReceiverWithoutCloningSourceIdentityOrLosingReceipts() throws Exception {
        var source = identity("source");
        var receiver = new WalReplicaIdentity(source.engineId(), source.groupId(), "receiver");
        Path backup = directory.resolve("backup");
        WalReceipt first;
        WalReceipt second;
        var a = new Authority();
        try (var runtime = open(directory.resolve("source"), source, a)) {
            first = pair(runtime, "old");
            second = pair(runtime, "new");
            var deletion = runtime.begin(IsolationLevel.REPEATABLE_READ);
            runtime.delete(deletion, "a", "deleted");
            runtime.delete(deletion, "b", "deleted");
            runtime.commit(deletion);
            Assertions.assertEquals(3, runtime.backupReplica(a, backup, WalBackupOptions.defaults()).sequence());
            Assertions.assertEquals(4, pair(runtime, "later").commitSequence());
        }
        Path target = directory.resolve("target");
        WalBackup.restoreReplica(backup, target, Map.of(), WalOptions.defaults(), WalBackupOptions.defaults(), receiver);
        Assertions.assertEquals(receiver, WalReplicaIdentity.read(target));
        Assertions.assertEquals(source, WalReplicaIdentity.read(backup.resolve("local")));
        assertThrows(WalMvccException.class, () -> WalMvccRuntime.open(target, Set.of("a", "b"), WalOptions.defaults()));
        var authority = new Authority();
        try (var runtime = open(target, receiver, authority)) {
            var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
            var view = runtime.beginReadStatement(tx);
            for (String shard : Set.of("a", "b")) {
                Assertions.assertEquals("new", text(runtime.get(tx, shard, "k", view).orElseThrow()));
                assertTrue(runtime.get(tx, shard, "deleted", view).isEmpty());
            }
            runtime.rollback(tx);
            Assertions.assertEquals(first, runtime.findCommitted(first.transactionId()).orElseThrow());
            Assertions.assertEquals(second, runtime.findCommitted(second.transactionId()).orElseThrow());
            Assertions.assertEquals(4, pair(runtime, "receiver-write").commitSequence());
        }
    }

    /**
     * Normal recovery and cross-group recovery are denied before creating target or contact database, which cannot be converted in secret.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void wrongModeAndWrongGroupCannotStartRestore() throws Exception {
        var source = identity("source");
        Path backup = snapshot(source);
        Path normal = directory.resolve("normal");
        assertThrows(IOException.class, () -> WalBackup.restore(backup, normal, Map.of(), WalOptions.defaults(), WalBackupOptions.defaults()));
        assertFalse(Files.exists(normal));
        var wrong = new WalReplicaIdentity(source.engineId(), UUID.randomUUID(), "receiver");
        Path target = directory.resolve("wrong-group");
        assertThrows(IOException.class, () -> WalBackup.restoreReplica(backup, target, Map.of(), WalOptions.defaults(), WalBackupOptions.defaults(), wrong));
        assertFalse(Files.exists(target));
        Path standalone = directory.resolve("standalone-backup");
        try (var runtime = WalMvccRuntime.open(directory.resolve("standalone"), Set.of("a", "b"), WalOptions.defaults())) {
            pair(runtime, "plain");
            runtime.backup(standalone, WalBackupOptions.defaults());
        }
        assertThrows(IOException.class, () -> WalBackup.restoreReplica(standalone, target, Map.of(), WalOptions.defaults(), WalBackupOptions.defaults(), source));
        assertFalse(Files.exists(target));
    }

    /**
     * Copy identity damage in mirrors cannot be bypassed by a complete but incorrect directory name.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void corruptedSourceIdentityFailsBeforeTargetChanges() throws Exception {
        var source = identity("source");
        Path backup = snapshot(source);
        byte[] bytes = Files.readAllBytes(backup.resolve("local/REPLICA"));
        bytes[20] ^= 1;
        Files.write(backup.resolve("local/REPLICA"), bytes);
        Path target = directory.resolve("target");
        assertThrows(IOException.class, () -> WalBackup.restoreReplica(backup, target, Map.of(), WalOptions.defaults(), WalBackupOptions.defaults(), source));
        assertFalse(Files.exists(target));
    }

    /**
     * Recovery is not serviceable when the tag has not been removed after it was issued, and only the same receiver identity is allowed to continue safely.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void publicationCrashCannotServeAndCannotChangeReceiverOnRetry() throws Exception {
        var source = identity("source");
        Path backup = snapshot(source);
        var receiver = new WalReplicaIdentity(source.engineId(), source.groupId(), "receiver");
        Path target = directory.resolve("target");
        assertThrows(IllegalStateException.class, () -> WalBackup.restoreReplicaForTest(backup, target, Map.of(), WalOptions.defaults(), WalBackupOptions.defaults(), receiver,
                stage -> {
                    if (stage == WalMvccRuntime.Stage.AFTER_RESTORE_MOVE) {
                        throw new IllegalStateException("injected publication crash");
                    }
                }, (path, channel) -> channel.force(true)));
        assertTrue(Files.exists(target.resolve("RESTORING")));
        assertThrows(WalMvccException.class, () -> open(target, receiver, new Authority()));
        var other = new WalReplicaIdentity(source.engineId(), source.groupId(), "other");
        assertThrows(IOException.class, () -> WalBackup.restoreReplica(backup, target, Map.of(), WalOptions.defaults(), WalBackupOptions.defaults(), other));
        WalBackup.restoreReplica(backup, target, Map.of(), WalOptions.defaults(), WalBackupOptions.defaults(), receiver);
        assertFalse(Files.exists(target.resolve("RESTORING")));
        try (var runtime = open(target, receiver, new Authority())) {
            Assertions.assertEquals(1, runtime.stats().publishedSequence());
        }
    }

    /**
     * The last copied complete file retry must still be brushed, and the receiver identity itself must also brush before removing the recovery mark.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void retryForcesExistingFilesAndReceiverIdentity() throws Exception {
        var source = identity("source");
        Path backup = snapshot(source);
        Path target = directory.resolve("target");
        var receiver = new WalReplicaIdentity(source.engineId(), source.groupId(), "receiver");
        var engines = new AtomicInteger();
        var replicas = new AtomicInteger();
        WalBackup.ForceHook force = (path, channel) -> {
            channel.force(true);
            if (path.getFileName().toString().equals("ENGINE")) {
                engines.incrementAndGet();
            }
            if (path.getFileName().toString().equals("REPLICA")) {
                replicas.incrementAndGet();
            }
        };
        assertThrows(IllegalStateException.class, () -> WalBackup.restoreReplicaForTest(backup, target, Map.of(), WalOptions.defaults(), WalBackupOptions.defaults(), receiver,
                stage -> {
                    if (stage == WalMvccRuntime.Stage.AFTER_RESTORE_FILE_COPY && engines.get() > 0) {
                        throw new IllegalStateException("copied before crash");
                    }
                }, force));
        Assertions.assertEquals(1, engines.get());
        WalBackup.restoreReplicaForTest(backup, target, Map.of(), WalOptions.defaults(), WalBackupOptions.defaults(), receiver, stage -> {
        }, force);
        Assertions.assertEquals(2, engines.get());
        Assertions.assertEquals(1, replicas.get());
        Assertions.assertEquals(receiver, WalReplicaIdentity.read(target));
    }

    /**
     * Zero business commit, but already copied identity node can also be exported and mode protection cannot be lost.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void emptyReplicaSnapshotRestoresAsReplica() throws Exception {
        var source = identity("source");
        Path backup = directory.resolve("empty-backup");
        var authority = new Authority();
        try (var runtime = open(directory.resolve("source"), source, authority)) {
            assertThrows(IllegalArgumentException.class, () -> runtime.backupReplica(new Authority(), directory.resolve("untrusted"), WalBackupOptions.defaults()));
            Assertions.assertEquals(0, runtime.backupReplica(authority, backup, WalBackupOptions.defaults()).sequence());
        }
        Path target = directory.resolve("target");
        WalBackup.restoreReplica(backup, target, Map.of(), WalOptions.defaults(), WalBackupOptions.defaults(), source);
        try (var runtime = open(target, source, new Authority())) {
            Assertions.assertEquals(1, pair(runtime, "first").commitSequence());
        }
    }

    /**
     * Creates a full source mirror, and subsequent tests can only change the document in their possession.
     *
     * @param identity identity
     * @return the operation result
     * @throws Exception if the operation cannot complete
     */
    private Path snapshot(final WalReplicaIdentity identity) throws Exception {
        Path backup = directory.resolve("backup");
        var authority = new Authority();
        try (var runtime = open(directory.resolve("source"), identity, authority)) {
            pair(runtime, "value");
            runtime.backupReplica(authority, backup, WalBackupOptions.defaults());
        }
        return backup;
    }

    /**
     * Group share business engine UUID, node identity for different receiving nodes remain independent.
     *
     * @param node node
     * @return the operation result
     */
    static WalReplicaIdentity identity(final String node) {
        UUID group = UUID.randomUUID();
        return new WalReplicaIdentity(group, group, node);
    }

    /**
     * Test to directly assemble a credible port, which does not mean that Raft has obtained verify for voting, election or majority.
     *
     * @param path path
     * @param identity identity
     * @param authority authority
     * @return the operation result
     */
    static WalMvccRuntime open(final Path path, final WalReplicaIdentity identity, final Authority authority) {
        var runtime = WalMvccRuntime.openReplica(path, Set.of("a", "b"), Map.of(), WalOptions.defaults(), WalRetentionOptions.preserveAll(), identity, authority);
        authority.runtime = runtime;
        return runtime;
    }

    /**
     * Write complete cross-shard transaction, check the serial number and UUID with the actual WAL receipt.
     *
     * @param runtime runtime
     * @param value value to write
     * @return the operation result
     */
    static WalReceipt pair(final WalMvccRuntime runtime, final String value) {
        var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
        for (String shard : Set.of("a", "b")) {
            runtime.put(tx, shard, "k", bytes(value));
        }
        return runtime.commit(tx);
    }

    /**
     * Synchronization for mirror protocol only is commit and real Raft has a separate module test.
     */
    static final class Authority implements WalCommitAuthority {

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
         * The network connection is not created.
         *
         * @param timeoutMillis timeout millis
         * @return the operation result
         */
        @Override
        public long readBarrier(final long timeoutMillis) {
            return 1;
        }

        /**
         * Install complete batch before the single test commit is completed.
         *
         * @param epoch epoch
         * @param records records
         * @return the operation result
         */
        @Override
        public CompletionStage<Void> submit(final long epoch, final List<WalCommit> records) {
            runtime.applyCommitted(this, records);
            return CompletableFuture.completedFuture(null);
        }
    }
}
