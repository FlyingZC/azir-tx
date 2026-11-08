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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Assertions;

import com.flyingzc.azir.common.IsolationLevel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import static com.flyingzc.azir.wal.WalMvccRuntimeTest.*;

/**
 * Complete local backup verify data, receipt, recovery release, damage reject and close competition.
 */
class WalBackupTest {

    @TempDir
    Path directory;

    /**
     * Backup is fixed at its own commit border, and the source does not change the value after recovery and receipt.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void localBackupRestoresDataReceiptsAndSequenceAtItsOwnCut() throws Exception {
        Path archive = directory.resolve("backup");
        Path restored = directory.resolve("restored");
        WalReceipt first;
        WalReceipt second;
        try (var runtime = WalMvccRuntime.open(directory.resolve("source"), Set.of("a", "b"), WalOptions.defaults())) {
            first = pair(runtime, "old");
            var old = runtime.begin(IsolationLevel.REPEATABLE_READ);
            second = pair(runtime, "new");
            var info = runtime.backup(archive, WalBackupOptions.defaults());
            Assertions.assertEquals(2, info.sequence());
            assertTrue(info.rows().isEmpty());
            Assertions.assertEquals("old", text(runtime.get(old, "a", "k").orElseThrow()));
            runtime.rollback(old);
            Assertions.assertEquals(3, pair(runtime, "after-backup").commitSequence());
            assertFalse(runtime.stats().backupRunning());
        }
        var inspected = WalBackup.inspect(archive, WalBackupOptions.defaults());
        Assertions.assertEquals(2, inspected.sequence());
        WalBackup.restore(archive, restored, Map.of(), WalOptions.defaults(), WalBackupOptions.defaults());
        try (var runtime = WalMvccRuntime.open(restored, Set.of("a", "b"), WalOptions.defaults())) {
            var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
            var snapshot = runtime.beginReadStatement(tx);
            for (String shard : new String[]{"a", "b"}) {
                Assertions.assertEquals("new", text(runtime.get(tx, shard, "k", snapshot).orElseThrow()));
            }
            runtime.rollback(tx);
            Assertions.assertEquals(first, runtime.findCommitted(first.transactionId()).orElseThrow());
            Assertions.assertEquals(second, runtime.findCommitted(second.transactionId()).orElseThrow());
            Assertions.assertEquals(3, pair(runtime, "restored-write").commitSequence());
        }
    }

    /**
     * An empty engine can also be backed up and recovery. The zero commit border cannot be mistaken for missing the recovery entrance.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void emptyEngineCanBeBackedUpAndRestored() throws Exception {
        Path backup = directory.resolve("empty-backup");
        Path target = directory.resolve("empty-target");
        try (var runtime = WalMvccRuntime.open(directory.resolve("empty"), Set.of("a", "b"), WalOptions.defaults())) {
            Assertions.assertEquals(0, runtime.backup(backup, WalBackupOptions.defaults()).sequence());
        }
        WalBackup.restore(backup, target, Map.of(), WalOptions.defaults(), WalBackupOptions.defaults());
        try (var runtime = WalMvccRuntime.open(target, Set.of("a", "b"), WalOptions.defaults())) {
            Assertions.assertEquals(1, pair(runtime, "first").commitSequence());
        }
    }

    /**
     * Backup retention of retirement query borders, missing the old receipt after recovery is still unknown or expired and cannot become rollback.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void expiredReceiptHistoryRetainsUnknownSemanticsAfterRestore() throws Exception {
        var clock = new java.util.concurrent.atomic.AtomicLong();
        var policy = new WalRetentionOptions(1, 3_600_000, 4_000_000, 1_000_000, 1000);
        Path backup = directory.resolve("expired-backup");
        WalReceipt old;
        WalReceipt recent;
        try (var runtime = WalMvccRuntime.openForTest(directory.resolve("expired-source"), Set.of("a", "b"), Map.of(), WalOptions.defaults(), stage -> {
        },
                WalLog.NORMAL_IO, System::nanoTime, policy, clock::get)) {
            old = pair(runtime, "old");
            runtime.checkpoint();
            clock.set(2);
            recent = pair(runtime, "recent");
            runtime.backup(backup, WalBackupOptions.defaults());
        }
        Path target = directory.resolve("expired-target");
        WalBackup.restore(backup, target, Map.of(), WalOptions.defaults(), WalBackupOptions.defaults());
        try (var runtime = WalMvccRuntime.open(target, Set.of("a", "b"), WalOptions.defaults())) {
            Assertions.assertEquals(WalCommitLookup.Status.UNKNOWN_OR_EXPIRED, runtime.lookupCommit(old.transactionId()).status());
            Assertions.assertEquals(recent, runtime.findCommitted(recent.transactionId()).orElseThrow());
        }
    }

    /**
     * Damage and lack of backup to complete the mark must be refused before changing recovery target.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void corruptionAndIncompleteBackupCannotPublishARecoveryDirectory() throws Exception {
        Path backup = localBackup();
        Path target = directory.resolve("bad-target");
        byte[] manifest = Files.readAllBytes(backup.resolve("COMPLETE"));
        byte[] bad = manifest.clone();
        bad[20] ^= 1;
        Files.write(backup.resolve("COMPLETE"), bad);
        assertThrows(IOException.class, () -> WalBackup.restore(backup, target, Map.of(), WalOptions.defaults(), WalBackupOptions.defaults()));
        assertFalse(Files.exists(target));
        Files.write(backup.resolve("COMPLETE"), manifest);
        Path engine = backup.resolve("local/ENGINE");
        byte[] identity = Files.readAllBytes(engine);
        identity[10] ^= 1;
        Files.write(engine, identity);
        assertThrows(IOException.class, () -> WalBackup.inspect(backup, WalBackupOptions.defaults()));
        assertFalse(Files.exists(target));
        Files.delete(backup.resolve("COMPLETE"));
        assertThrows(IOException.class, () -> WalBackup.inspect(backup, WalBackupOptions.defaults()));
    }

    /**
     * Existing directories will never be overx0 and symbol links will not guide external data.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void occupiedTargetsAndSymlinkFilesAreRejected() throws Exception {
        Path backup = localBackup();
        Path target = directory.resolve("occupied");
        Files.createDirectory(target);
        Files.writeString(target.resolve("user-file"), "keep");
        assertThrows(IOException.class, () -> WalBackup.restore(backup, target, Map.of(), WalOptions.defaults(), WalBackupOptions.defaults()));
        Assertions.assertEquals("keep", Files.readString(target.resolve("user-file")));
        Path engine = backup.resolve("local/ENGINE");
        Files.delete(engine);
        Files.createSymbolicLink(engine, directory.resolve("missing-external-file"));
        assertThrows(IOException.class, () -> WalBackup.inspect(backup, WalBackupOptions.defaults()));
    }

    /**
     * Byte budget depletion does not generate completed backups, nor can the health source engine be stopped or lost to confirm transaction.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void budgetFailureLeavesSourceHealthyAndBackupIncomplete() throws Exception {
        Path backup = directory.resolve("too-small");
        try (var runtime = WalMvccRuntime.open(directory.resolve("budget-source"), Set.of("a", "b"), WalOptions.defaults())) {
            pair(runtime, "x".repeat(4096));
            assertThrows(WalMvccException.class, () -> runtime.backup(backup, new WalBackupOptions(1024, 32, 8192, 60_000)));
            assertFalse(Files.exists(backup.resolve("COMPLETE")));
            assertFalse(runtime.stats().backupRunning());
            Assertions.assertEquals(2, pair(runtime, "still-live").commitSequence());
        }
        assertThrows(IOException.class, () -> WalBackup.inspect(backup, WalBackupOptions.defaults()));
    }

    /**
     * Backup completes incomplete and verifiable backup before and after completion of tagging, respectively, and the source should continue to be available.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void completionBoundaryFailuresHaveExplicitRecoverableArtifacts() throws Exception {
        for (var stage : new WalMvccRuntime.Stage[]{WalMvccRuntime.Stage.BEFORE_BACKUP_COMPLETE, WalMvccRuntime.Stage.AFTER_BACKUP_COMPLETE}) {
            Path source = directory.resolve("source-" + stage);
            Path backup = directory.resolve("backup-" + stage);
            var armed = new AtomicBoolean();
            try (var runtime = WalMvccRuntime.openForTest(source, Set.of("a", "b"), Map.of(), WalOptions.defaults(), hit -> {
                if (hit == stage && armed.compareAndSet(true, false)) {
                    throw new IllegalStateException("injected backup publication failure");
                }
            })) {
                pair(runtime, "one");
                armed.set(true);
                assertThrows(IllegalStateException.class, () -> runtime.backup(backup, WalBackupOptions.defaults()));
                if (stage == WalMvccRuntime.Stage.AFTER_BACKUP_COMPLETE) {
                    Assertions.assertEquals(1, WalBackup.inspect(backup, WalBackupOptions.defaults()).sequence());
                } else {
                    assertThrows(IOException.class, () -> WalBackup.inspect(backup, WalBackupOptions.defaults()));
                }
                Assertions.assertEquals(2, pair(runtime, "two").commitSequence());
            }
        }
    }

    /**
     * The service is denied when atomic moves and the recovery tag has not been cleared, and the same backup can be completed.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void restorePublicationFailureCanResumeWithoutExposingPartialState() throws Exception {
        Path backup = localBackup();
        Path target = directory.resolve("resume-target");
        assertThrows(IllegalStateException.class, () -> WalBackup.restoreForTest(backup, target, Map.of(), WalOptions.defaults(), WalBackupOptions.defaults(), stage -> {
            if (stage == WalMvccRuntime.Stage.AFTER_RESTORE_MOVE) {
                throw new IllegalStateException("lost restore publication response");
            }
        }));
        assertTrue(Files.exists(target.resolve("RESTORING")));
        assertThrows(WalMvccException.class, () -> WalMvccRuntime.open(target, Set.of("a", "b"), WalOptions.defaults()));
        assertFalse(Files.exists(target.resolve("writer.lock")));
        WalBackup.restore(backup, target, Map.of(), WalOptions.defaults(), WalBackupOptions.defaults());
        assertFalse(Files.exists(target.resolve("RESTORING")));
        try (var runtime = WalMvccRuntime.open(target, Set.of("a", "b"), WalOptions.defaults())) {
            Assertions.assertEquals(2, pair(runtime, "after").commitSequence());
        }
        assertThrows(IOException.class, () -> WalBackup.restore(backup, target, Map.of(), WalOptions.defaults(), WalBackupOptions.defaults()));
    }

    /**
     * The first copy of the actual write-up brush failed and must be synchronized again with this validate complete file, not just skipped it.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void resumedCompleteFileIsForcedAgainBeforePublication() throws Exception {
        Path backup = localBackup();
        Path target = directory.resolve("force-resume");
        var failed = new java.util.concurrent.atomic.AtomicReference<Path>();
        assertThrows(IOException.class, () -> WalBackup.restoreForTest(backup, target, Map.of(), WalOptions.defaults(), WalBackupOptions.defaults(), stage -> {
        }, (path, channel) -> {
            failed.set(path);
            throw new IOException("injected force failure after file contents were written");
        }));
        assertNotNull(failed.get());
        assertTrue(Files.size(failed.get()) > 0);
        assertFalse(Files.exists(target));
        var forced = new java.util.HashSet<Path>();
        WalBackup.restoreForTest(backup, target, Map.of(), WalOptions.defaults(), WalBackupOptions.defaults(), stage -> {
        }, (path, channel) -> {
            channel.force(true);
            forced.add(path);
        });
        assertTrue(forced.contains(failed.get()), "a complete file left by an unsuccessful force must be forced again");
        try (var runtime = WalMvccRuntime.open(target, Set.of("a", "b"), WalOptions.defaults())) {
            Assertions.assertEquals(2, pair(runtime, "after").commitSequence());
        }
    }

    /**
     * Backup and close the line, close it pending the release of the writer ' s lock or interrupt your own backup file.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void closeWaitsForBackupAndNewTransactionsAreBackpressured() throws Exception {
        var reached = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var runtime = WalMvccRuntime.openForTest(directory.resolve("closing-source"), Set.of("a", "b"), Map.of(), WalOptions.defaults(), stage -> {
            if (stage == WalMvccRuntime.Stage.BEFORE_BACKUP_COMPLETE) {
                reached.countDown();
                await(release);
            }
        });
        try (var tasks = Executors.newVirtualThreadPerTaskExecutor()) {
            pair(runtime, "value");
            var backup = tasks.submit(() -> runtime.backup(directory.resolve("closing-backup"), WalBackupOptions.defaults()));
            assertTrue(reached.await(30, TimeUnit.SECONDS));
            assertTrue(runtime.stats().backupRunning());
            assertThrows(WalMvccException.class, () -> runtime.begin(IsolationLevel.READ_COMMITTED));
            var closing = tasks.submit(runtime::close);
            assertFalse(closing.isDone());
            release.countDown();
            Assertions.assertEquals(1, backup.get(30, TimeUnit.SECONDS).sequence());
            closing.get(30, TimeUnit.SECONDS);
            Assertions.assertEquals(1, WalBackup.inspect(directory.resolve("closing-backup"), WalBackupOptions.defaults()).sequence());
        } finally {
            release.countDown();
            runtime.close();
        }
    }

    /**
     * Creates a complete local backup after the actual commit.
     *
     * @return the operation result
     */
    private Path localBackup() {
        Path backup = directory.resolve("local-backup");
        try (var runtime = WalMvccRuntime.open(directory.resolve("local-source"), Set.of("a", "b"), WalOptions.defaults())) {
            pair(runtime, "value");
            runtime.backup(backup, WalBackupOptions.defaults());
        }
        return backup;
    }

    /**
     * Use the same WAL decision commit two shard s.
     *
     * @param runtime runtime
     * @param value value to write
     * @return the operation result
     */
    private static WalReceipt pair(final WalMvccRuntime runtime, final String value) {
        var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
        runtime.put(tx, "a", "k", bytes(value));
        runtime.put(tx, "b", "k", bytes(value));
        return runtime.commit(tx);
    }
}
