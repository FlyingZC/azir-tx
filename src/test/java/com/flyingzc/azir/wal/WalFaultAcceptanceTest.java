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

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Assertions;

import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.IsolationLevel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import static com.flyingzc.azir.wal.WalMvccRuntimeTest.*;

/**
 * Repeatable failure acceptance for real log writing, publishing, closing the lease border.
 */
class WalFaultAcceptanceTest {

    @TempDir
    Path directory;

    /**
     * True-written partial frame to simulate disk full, recovery can only keep the confirmed prefix.
     *
     * @param bytesIntoFrame bytes into frame
     */
    @ParameterizedTest
    @ValueSource(ints = {8, 20})
    void partialWriteFailureCannotLoseAcknowledgedPrefix(final int bytesIntoFrame) {
        var failAt = new AtomicLong(Long.MAX_VALUE);
        var io = new WalLog.IoHook() {

            /**
             * Limit next actual writing to failure position.
             *
             * @param position position
             * @param remaining remaining
             * @return the operation result
             */
            @Override
            public int writeLimit(final long position, final int remaining) {
                return (int) Math.min(remaining, Math.max(1, failAt.get() - position));
            }

            /**
             * Throws a device error after the part that has already occurred.
             *
             * @param position position
             * @throws IOException if the operation cannot complete
             */
            @Override
            public void afterWrite(final long position) throws IOException {
                if (position >= failAt.get()) {
                    throw new IOException("injected ENOSPC after partial write");
                }
            }
        };
        UUID confirmed;
        UUID uncertain;
        long prefixSize;
        try (var runtime = open(io, stage -> {
        }, System::nanoTime)) {
            confirmed = pair(runtime, "old").transactionId();
            prefixSize = runtime.stats().walBytes();
            failAt.set(prefixSize + bytesIntoFrame);
            var tx = pairedWrite(runtime, "new");
            uncertain = tx.id();
            Assertions.assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN, assertThrows(WalMvccException.class,
                    () -> runtime.commit(tx)).errorCode());
            assertTrue(runtime.findCommitted(uncertain).isEmpty());
            assertThrows(WalMvccException.class, () -> runtime.begin(IsolationLevel.READ_COMMITTED));
        }
        try (var runtime = WalMvccRuntime.open(directory, Set.of("a", "b"), WalOptions.defaults())) {
            Assertions.assertEquals(prefixSize, runtime.stats().walBytes());
            assertTrue(runtime.findCommitted(confirmed).isPresent());
            assertTrue(runtime.findCommitted(uncertain).isEmpty());
            assertPair(runtime, "old");
            Assertions.assertEquals(2, pair(runtime, "after-recovery").commitSequence());
        }
    }

    /**
     * A complete frame cannot be confirmed in advance if it fails before brushing, and recovery allows adoptions to be not fully recorded.
     */
    @Test
    void forceFailureHasUnknownOutcomeUntilRecovery() {
        forceBoundary(false);
    }

    /**
     * Recovery must still complete the original transaction when the brush actually works but returns an error.
     */
    @Test
    void lostForceAcknowledgementMustRecoverTheSameTransaction() {
        forceBoundary(true);
    }

    /**
     * Failed to inject before or after brushing, verify confirms the border with recovery.
     *
     * @param afterForce after force
     */
    private void forceBoundary(final boolean afterForce) {
        var armed = new AtomicBoolean();
        var io = new WalLog.IoHook() {

            /**
             * Simulates a complete frame that has been written but the brushing failed.
             *
             * @throws IOException if the operation cannot complete
             */
            @Override
            public void beforeForce() throws IOException {
                if (!afterForce && armed.get()) {
                    throw new IOException("injected force failure");
                }
            }

            /**
             * The loss is confirmed after the simulation brush has been successful.
             *
             * @throws IOException if the operation cannot complete
             */
            @Override
            public void afterForce() throws IOException {
                if (afterForce && armed.get()) {
                    throw new IOException("injected lost force acknowledgement");
                }
            }
        };
        UUID id;
        try (var runtime = open(io, stage -> {
        }, System::nanoTime)) {
            pair(runtime, "old");
            armed.set(true);
            var tx = pairedWrite(runtime, "new");
            id = tx.id();
            Assertions.assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN, assertThrows(WalMvccException.class,
                    () -> runtime.commit(tx)).errorCode());
            assertTrue(runtime.findCommitted(id).isEmpty());
        }
        try (var recovered = WalMvccRuntime.open(directory, Set.of("a", "b"), WalOptions.defaults())) {
            Assertions.assertEquals(2, recovered.findCommitted(id).orElseThrow().commitSequence());
            assertPair(recovered, "new");
        }
    }

    /**
     * Only the first shard failed to be installed. Half a pen of transaction could not be confirmed, and complete write set must be installed after recovery.
     */
    @Test
    void failureHalfwayThroughInstallationCannotPublishHalfATransaction() {
        var armed = new AtomicBoolean();
        UUID id;
        try (var runtime = open(WalLog.NORMAL_IO, stage -> {
            if (stage == WalMvccRuntime.Stage.DURING_INSTALL && armed.compareAndSet(true, false)) {
                throw new IllegalStateException("injected halfway installation failure");
            }
        }, System::nanoTime)) {
            pair(runtime, "old");
            armed.set(true);
            var tx = pairedWrite(runtime, "new");
            id = tx.id();
            Assertions.assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN, assertThrows(WalMvccException.class,
                    () -> runtime.commit(tx)).errorCode());
            Assertions.assertEquals(1, runtime.stats().publishedSequence());
            assertTrue(runtime.findCommitted(id).isEmpty());
            assertThrows(WalMvccException.class, () -> runtime.begin(IsolationLevel.REPEATABLE_READ));
        }
        try (var recovered = WalMvccRuntime.open(directory, Set.of("a", "b"), WalOptions.defaults())) {
            assertTrue(recovered.findCommitted(id).isPresent());
            assertPair(recovered, "new");
        }
    }

    /**
     * The same transaction concurrent commit can only produce one log and the same result twice.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void simultaneousDuplicateCommitSharesOneDecision() throws Exception {
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (var runtime = open(WalLog.NORMAL_IO, stage -> {
            if (stage == WalMvccRuntime.Stage.BEFORE_APPEND) {
                entered.countDown();
                waitLong(release);
            }
        }, System::nanoTime); var tasks = Executors.newVirtualThreadPerTaskExecutor()) {
            var tx = pairedWrite(runtime, "once");
            var first = tasks.submit(() -> runtime.commit(tx));
            assertTrue(entered.await(30, TimeUnit.SECONDS));
            var second = tasks.submit(() -> runtime.commit(tx));
            try {
                Assertions.assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN, assertThrows(WalMvccException.class,
                        () -> runtime.rollback(tx)).errorCode());
            } finally {
                release.countDown();
            }
            Assertions.assertEquals(first.get(30, TimeUnit.SECONDS), second.get(30, TimeUnit.SECONDS));
            Assertions.assertEquals(1, runtime.stats().publishedSequence());
            Assertions.assertEquals(1, runtime.stats().groupForces());
            assertPair(runtime, "once");
        } finally {
            release.countDown();
        }
    }

    /**
     * When normal closures compete with commit in transit, requests have been accepted before the logs are released.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void closeDrainsAcceptedCommitBeforeReleasingWal() throws Exception {
        var forced = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var runtime = open(WalLog.NORMAL_IO, stage -> {
            if (stage == WalMvccRuntime.Stage.AFTER_FORCE) {
                forced.countDown();
                waitLong(release);
            }
        }, System::nanoTime);
        UUID id;
        try (var tasks = Executors.newVirtualThreadPerTaskExecutor()) {
            var tx = pairedWrite(runtime, "closing");
            id = tx.id();
            var commit = tasks.submit(() -> runtime.commit(tx));
            assertTrue(forced.await(30, TimeUnit.SECONDS));
            var close = tasks.submit(runtime::close);
            release.countDown();
            commit.get(30, TimeUnit.SECONDS);
            close.get(30, TimeUnit.SECONDS);
        } finally {
            release.countDown();
            runtime.close();
        }
        try (var reopened = WalMvccRuntime.open(directory, Set.of("a", "b"), WalOptions.defaults())) {
            assertTrue(reopened.findCommitted(id).isPresent());
            assertPair(reopened, "closing");
        }
    }

    /**
     * Duration and collection use controllable monotonic clock, machines are busy and do not change expectations.
     */
    @Test
    void expiredReaderCannotKeepOrReuseAnOldSnapshot() {
        var clock = new AtomicLong();
        try (var runtime = open(WalLog.NORMAL_IO, stage -> {
        }, clock::get)) {
            pair(runtime, "old");
            var reader = runtime.begin(IsolationLevel.REPEATABLE_READ);
            var snapshot = runtime.beginReadStatement(reader);
            pair(runtime, "new");
            clock.addAndGet(TimeUnit.SECONDS.toNanos(61));
            runtime.collectGarbage(10);
            Assertions.assertEquals(2, runtime.stats().safePoint());
            assertThrows(WalMvccException.class, () -> runtime.get(reader, "a", "k", snapshot));
            assertPair(runtime, "new");
        }
    }

    /**
     * Injecting point in the same WAL directory package.
     *
     * @param io io
     * @param hook hook
     * @param clock clock
     * @return the operation result
     */
    private WalMvccRuntime open(final WalLog.IoHook io, final WalMvccRuntime.FaultHook hook,
                                final java.util.function.LongSupplier clock) {
        return WalMvccRuntime.openForTest(directory, Set.of("a", "b"), Map.of(), WalOptions.defaults(), hook, io, clock);
    }

    /**
     * Construct a complete cross-shard write set that is not yet commit.
     *
     * @param runtime runtime
     * @param value value to write
     * @return the operation result
     */
    private static WalTransaction pairedWrite(final WalMvccRuntime runtime, final String value) {
        var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
        runtime.put(tx, "a", "k", bytes(value));
        runtime.put(tx, "b", "k", bytes(value));
        return tx;
    }

    /**
     * Commit cross-shard changes.
     *
     * @param runtime runtime
     * @param value value to write
     * @return the operation result
     */
    private static WalReceipt pair(final WalMvccRuntime runtime, final String value) {
        return runtime.commit(pairedWrite(runtime, value));
    }

    /**
     * Checks two shard values in the same snapshot.
     *
     * @param runtime runtime
     * @param value value to write
     */
    private static void assertPair(final WalMvccRuntime runtime, final String value) {
        var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
        Assertions.assertEquals(value, text(runtime.get(tx, "a", "k").orElseThrow()));
        Assertions.assertEquals(value, text(runtime.get(tx, "b", "k").orElseThrow()));
        runtime.rollback(tx);
    }

    /**
     * Relax schedule budget to avoid miscalculating the sharing machine as a failure of the agreement.
     *
     * @param latch latch
     * @throws AssertionError if the operation cannot complete
     */
    private static void waitLong(final CountDownLatch latch) {
        try {
            if (!latch.await(30, TimeUnit.SECONDS)) {
                throw new AssertionError("fault barrier timed out");
            }
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new AssertionError(ex);
        }
    }
}
