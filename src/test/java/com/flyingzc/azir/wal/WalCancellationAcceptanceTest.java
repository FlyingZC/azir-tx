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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import org.junit.jupiter.api.Assertions;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import static com.flyingzc.azir.wal.WalMvccRuntimeTest.*;

/**
 * Verify Thread Cancel does not close shared WAL writing channels or pre-release unique locks.
 */
class WalCancellationAcceptanceTest {

    @TempDir
    Path directory;

    /**
     * Interrupted log reader only closes independent reading channels, and subsequent additions and monopolization locks remain valid.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void interruptedLogReaderDoesNotCloseWriterOrReleaseItsLock() throws Exception {
        try (var log = new WalLog(directory, Set.of("a"), 1_000_000)) {
            log.append(List.of(record(1)));
            var failure = new AtomicReference<Throwable>();
            Thread reader = Thread.ofVirtual().start(() -> {
                Thread.currentThread().interrupt();
                try {
                    log.read(1, 1);
                    fail("interrupted reader should fail");
                } catch (final IOException expected) {
                } catch (final Throwable unexpected) {
                    failure.set(unexpected);
                }
            });
            reader.join(30_000);
            assertFalse(reader.isAlive());
            assertNull(failure.get());
            assertThrows(Exception.class, () -> new WalLog(directory, Set.of("a"), 1_000_000));
            Assertions.assertEquals(0, otherProcessLockAttempt());
            log.append(List.of(record(2)));
            Assertions.assertEquals(2, log.lastSequence());
            Assertions.assertEquals(log.bytes(), java.nio.file.Files.size(directory.resolve("transactions.wal")));
        }
        try (var recovered = new WalLog(directory, Set.of("a"), 1_000_000)) {
            Assertions.assertEquals(2, recovered.lastSequence());
        }
        Assertions.assertEquals(7, otherProcessLockAttempt());
    }

    /**
     * Interrupted statistical callers cannot call off shared writing channels through the size system.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void interruptedStatsCallerCannotPoisonTheRuntime() throws Exception {
        try (var runtime = WalMvccRuntime.open(directory, Set.of("a"), WalOptions.defaults())) {
            write(runtime, "a", "k", "one");
            var failure = new AtomicReference<Throwable>();
            Thread reader = Thread.ofVirtual().start(() -> {
                Thread.currentThread().interrupt();
                try {
                    assertTrue(runtime.stats().walBytes() > 60);
                } catch (final Throwable unexpected) {
                    failure.set(unexpected);
                }
            });
            reader.join(30_000);
            assertFalse(reader.isAlive());
            assertNull(failure.get());
            Assertions.assertEquals(2, write(runtime, "a", "k", "two").commitSequence());
        }
    }

    /**
     * Normal close to waiting materialization, without interrupting its JDBC or log reading.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void closeDoesNotInterruptAnInFlightMaterializer() throws Exception {
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var closing = new CountDownLatch(1);
        var interrupted = new AtomicBoolean();
        var store = new TestMaterializedStore();
        store.onApply = () -> {
            entered.countDown();
            try {
                if (!release.await(30, TimeUnit.SECONDS)) {
                    throw new AssertionError("materializer barrier timeout");
                }
            } catch (final InterruptedException failure) {
                interrupted.set(true);
                throw new IllegalStateException(failure);
            }
        };
        var runtime = WalMvccRuntime.openForTest(directory, Set.of("a"), Map.of("a", store), WalOptions.defaults(),
                stage -> {
                    if (stage == WalMvccRuntime.Stage.BEFORE_MATERIALIZER_JOIN) {
                        closing.countDown();
                    }
                });
        try (var tasks = Executors.newVirtualThreadPerTaskExecutor()) {
            write(runtime, "a", "k", "committed");
            assertTrue(entered.await(30, TimeUnit.SECONDS));
            var stopped = tasks.submit(runtime::close);
            try {
                assertTrue(closing.await(30, TimeUnit.SECONDS));
                assertFalse(interrupted.get());
            } finally {
                release.countDown();
            }
            stopped.get(30, TimeUnit.SECONDS);
            assertFalse(interrupted.get());
        } finally {
            release.countDown();
            runtime.close();
        }
        Assertions.assertEquals(1, store.checkpoint().sequence());
    }

    /**
     * Repeated closure of an old object cannot remove this process registration or system lock for the new author.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void repeatedOldCloseCannotReleaseANewWritersLock() throws Exception {
        var old = new WalLog(directory, Set.of("a"), 1_000_000);
        old.close();
        try (var current = new WalLog(directory, Set.of("a"), 1_000_000)) {
            old.close();
            assertThrows(Exception.class, () -> new WalLog(directory, Set.of("a"), 1_000_000));
            Assertions.assertEquals(0, otherProcessLockAttempt());
            current.append(List.of(record(1)));
        }
    }

    /**
     * Creates a simple durable record.
     *
     * @param sequence sequence
     * @return the operation result
     */
    private static WalCommit record(final long sequence) {
        return new WalCommit(UUID.randomUUID(), sequence, 0, List.of(new WalMutation("a", "k", bytes("v" + sequence))));
    }

    /**
     * Check whether the monopoly lock is still held by the father process through the real son JVM.
     *
     * @return the operation result
     * @throws Exception if the operation cannot complete
     */
    private int otherProcessLockAttempt() throws Exception {
        String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        Process child = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp", classpath, WalExclusiveWriterProcess.class.getName(), directory.toString())
                .redirectErrorStream(true).redirectOutput(directory.resolve("lock-probe.log").toFile()).start();
        assertTrue(child.waitFor(30, TimeUnit.SECONDS));
        return child.exitValue();
    }
}
