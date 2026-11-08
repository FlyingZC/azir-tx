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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import org.junit.jupiter.api.Assertions;

import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.IsolationLevel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import static com.flyingzc.azir.wal.WalMvccRuntimeTest.*;

/**
 * Use of real process to kill, respond to loss and concurrent for joint approval of verify durable commit.
 */
class WalRecoveryTest {

    @TempDir
    Path directory;

    /**
     * Commit write set cannot recovery as commit version after adding a pre-killer.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void killBeforeAppendHasNoCommittedVersion() throws Exception {
        killAndRecover("BEFORE_APPEND", false);
    }

    /**
     * Forced to kill after the brush has not been released, recovery whole cross-shard transaction must be restarted.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void killAfterForceRecoversWholeTransaction() throws Exception {
        killAndRecover("AFTER_FORCE", true);
    }

    /**
     * The durable query can still check the original transaction ID before responding.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void killAfterPublishRetainsCommitReceipt() throws Exception {
        killAndRecover("AFTER_PUBLISH", true);
    }

    /**
     * The caller is killed after success and the confirmed data cannot be lost.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void killAfterAcknowledgementRetainsData() throws Exception {
        killAndRecover("ACK", true);
    }

    /**
     * The subprocesses kill after the border has stopped and then open the same log check.
     *
     * @param stage stage
     * @param committed committed
     * @throws Exception if the operation cannot complete
     */
    private void killAndRecover(final String stage, final boolean committed) throws Exception {
        String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        Process child = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp", classpath, WalCrashProcess.class.getName(), directory.toString(), stage)
                .redirectErrorStream(true).redirectOutput(directory.resolve("child.log").toFile()).start();
        try {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (!Files.exists(directory.resolve("ready"))) {
                if (!child.isAlive() || System.nanoTime() > deadline) {
                    fail("child did not reach stage: " + Files.readString(directory.resolve("child.log")));
                }
                Thread.sleep(10);
            }
        } finally {
            child.destroyForcibly();
            assertTrue(child.waitFor(5, TimeUnit.SECONDS));
        }
        UUID id = UUID.fromString(Files.readString(directory.resolve("transaction-id")));
        try (var recovered = WalMvccRuntime.open(directory, Set.of("a", "b"), WalOptions.defaults())) {
            Assertions.assertEquals(committed, recovered.findCommitted(id).isPresent());
            var reader = recovered.begin(IsolationLevel.REPEATABLE_READ);
            Assertions.assertEquals(committed, recovered.get(reader, "a", "balance").isPresent());
            Assertions.assertEquals(committed, recovered.get(reader, "b", "balance").isPresent());
            if (committed) {
                assertArrayEquals(new byte[]{90}, recovered.get(reader, "a", "balance").orElseThrow());
                assertArrayEquals(new byte[]{110}, recovered.get(reader, "b", "balance").orElseThrow());
                Assertions.assertEquals(1, recovered.findCommitted(id).orElseThrow().commitSequence());
            }
        }
    }

    /**
     * Brushing failure must return unknown, stop running and recovery is commit after restarting.
     */
    @Test
    void durableFailureCannotTurnIntoRollback() {
        UUID id;
        try (var runtime = WalMvccRuntime.openForTest(directory, Set.of("a"), Map.of(), WalOptions.defaults(), stage -> {
            if (stage == WalMvccRuntime.Stage.AFTER_FORCE) {
                throw new IllegalStateException("injected crash");
            }
        })) {
            var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
            id = tx.id();
            runtime.put(tx, "a", "k", bytes("durable"));
            Assertions.assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN, assertThrows(WalMvccException.class,
                    () -> runtime.commit(tx)).errorCode());
            assertThrows(WalMvccException.class, () -> runtime.rollback(tx));
            assertThrows(WalMvccException.class, () -> runtime.begin(IsolationLevel.REPEATABLE_READ));
        }
        try (var recovered = WalMvccRuntime.open(directory, Set.of("a"), WalOptions.defaults())) {
            assertTrue(recovered.findCommitted(id).isPresent());
            var tx = recovered.begin(IsolationLevel.REPEATABLE_READ);
            Assertions.assertEquals("durable", text(recovered.get(tx, "a", "k").orElseThrow()));
        }
    }

    /**
     * A large number of independent transaction batch brushes, all receipt and the results remain unique and complete.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void groupCommitSharesForcesWithoutLosingTransactions() throws Exception {
        var o = WalOptions.defaults();
        var limits = new WalOptions(100, 10, 1024, 100, 64, 20,
                o.maxWalBytes(), o.maxMemoryBytes(), 5000, 10, 20);
        try (
                var runtime = WalMvccRuntime.open(directory, Set.of("a"), limits);
                var tasks = Executors.newVirtualThreadPerTaskExecutor()) {
            var ready = new CountDownLatch(32);
            var start = new CountDownLatch(1);
            var futures = new ArrayList<java.util.concurrent.Future<WalReceipt>>();
            for (int i = 0; i < 32; i++) {
                String key = "k" + i;
                futures.add(tasks.submit(() -> {
                    var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
                    runtime.put(tx, "a", key, bytes(key));
                    ready.countDown();
                    await(start);
                    return runtime.commit(tx);
                }));
            }
            assertTrue(ready.await(5, TimeUnit.SECONDS));
            start.countDown();
            var receipts = new java.util.HashSet<Long>();
            for (var future : futures) {
                receipts.add(future.get(5, TimeUnit.SECONDS).commitSequence());
            }
            Assertions.assertEquals(32, receipts.size());
            assertTrue(runtime.stats().groupForces() < 32);
            Assertions.assertEquals(32, runtime.stats().publishedSequence());
        }
        try (var recovered = WalMvccRuntime.open(directory, Set.of("a"), limits)) {
            var tx = recovered.begin(IsolationLevel.REPEATABLE_READ);
            Assertions.assertEquals(32, recovered.scan(tx, "a", null, null).size());
        }
    }

    /**
     * WAL cannot take over the tied backend, nor can backward logs cover lead checkpoints.
     */
    @Test
    void backendBindingAndCheckpointMustMatchWal() {
        var store = new TestMaterializedStore();
        try (var runtime = WalMvccRuntime.open(directory, Map.of("a", store), WalOptions.defaults())) {
            write(runtime, "a", "k", "one");
            runtime.flush();
        }
        assertThrows(IllegalStateException.class, () -> WalMvccRuntime.open(directory.resolve("other"),
                Map.of("a", store), WalOptions.defaults()));
        store.apply(List.of(new WalCommit(UUID.randomUUID(), 2, 1, List.of(new WalMutation("a", "k", bytes("two"))))));
        assertThrows(WalMvccException.class, () -> WalMvccRuntime.open(directory, Map.of("a", store), WalOptions.defaults()));
    }
}
