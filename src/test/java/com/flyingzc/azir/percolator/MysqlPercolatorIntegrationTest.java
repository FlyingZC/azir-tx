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

package com.flyingzc.azir.percolator;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.flyingzc.azir.percolator.PercolatorStore.Action;
import com.flyingzc.azir.percolator.PercolatorStore.Decision;
import com.flyingzc.azir.percolator.PercolatorStore.Lock;
import com.flyingzc.azir.percolator.PercolatorStore.Snapshot;
import com.flyingzc.azir.percolator.PercolatorStore.State;
import org.junit.jupiter.api.Assertions;

import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.MvccException;
import com.flyingzc.azir.storage.Mutation;
import com.flyingzc.azir.storage.mysql.MysqlDataSourceFactory;
import com.flyingzc.azir.tso.TimestampOracle;
import com.flyingzc.azir.tso.mysql.MysqlTimestampOracle;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import com.flyingzc.azir.e2e.MysqlPercolatorBenchmark;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import static com.flyingzc.azir.common.IsolationLevel.*;

/**
 * Use two stand-alone MySQL examples verify Primary decision, cross-shard snapshot, recovery Contest and batch Optimization.
 */
@Testcontainers
class MysqlPercolatorIntegrationTest {

    @Container
    private static final MySQLContainer<?> MYSQL_A = new MySQLContainer<>("mysql:8.4")
            .withDatabaseName("percolator").withUsername("test").withPassword("test");

    @Container
    private static final MySQLContainer<?> MYSQL_B = new MySQLContainer<>("mysql:8.4")
            .withDatabaseName("percolator").withUsername("test").withPassword("test");

    private static DataSource poolA;

    private static DataSource poolB;

    private MysqlPercolatorStore a;

    private MysqlPercolatorStore b;

    private MysqlTimestampOracle tso;

    private PercolatorRuntime runtime;

    /**
     * Creates a list of connection pool and isolation protocols for this test only.
     */
    @BeforeAll
    static void open() {
        poolA = MysqlDataSourceFactory.create(MYSQL_A.getJdbcUrl(), MYSQL_A.getUsername(), MYSQL_A.getPassword());
        poolB = MysqlDataSourceFactory.create(MYSQL_B.getJdbcUrl(), MYSQL_B.getUsername(), MYSQL_B.getPassword());
        new MysqlPercolatorStore("a", poolA).initialize();
        new MysqlPercolatorStore("b", poolB).initialize();
        new MysqlTimestampOracle(poolA).initialize(100);
    }

    /**
     * Turn connection pool off first and test database by TestContainers collection.
     *
     * @throws Exception if the operation cannot complete
     */
    @AfterAll
    static void close() throws Exception {
        if (poolA instanceof AutoCloseable closeable) {
            closeable.close();
        }
        if (poolB instanceof AutoCloseable closeable) {
            closeable.close();
        }
    }

    /**
     * Emptys the test protocol state and does not reset global timestamp to avoid historical identity reuse.
     *
     * @throws Exception if the operation cannot complete
     */
    @BeforeEach
    void reset() throws Exception {
        for (DataSource pool : List.of(poolA, poolB)) {
            try (var connection = pool.getConnection(); var statement = connection.createStatement()) {
                for (String table : List.of("pc_lock", "pc_write", "pc_data", "pc_rollback", "pc_guard")) {
                    statement.executeUpdate("delete from " + table);
                }
            }
        }
        a = new MysqlPercolatorStore("a", poolA);
        b = new MysqlPercolatorStore("b", poolB);
        tso = new MysqlTimestampOracle(poolA);
        runtime = runtime(a, b);
    }

    /**
     * RC every statement takes a new boundary, RR and saved statement view keeps the original cross-shard snapshot.
     */
    @Test
    void rcAndRrUseGlobalStatementAndTransactionSnapshots() {
        seed("old");
        try (var rr = runtime.begin(REPEATABLE_READ); var rc = runtime.begin(READ_COMMITTED)) {
            var statement = rc.statement();
            Assertions.assertEquals("old", string(rr.get("a", "k")));
            Assertions.assertEquals("old", string(statement.get("b", "k")));
            seed("new");
            Assertions.assertEquals("old", string(rr.get("b", "k")));
            Assertions.assertEquals("old", string(statement.get("a", "k")));
            Assertions.assertEquals("new", string(rc.get("a", "k")));
            Assertions.assertEquals("new", string(rc.get("b", "k")));
        }
    }

    /**
     * Write and read both defensive copying, delete tombstone to keep old snapshot.
     */
    @Test
    void ownsWritesAndPreservesDeleteHistory() {
        seed("old");
        try (var old = runtime.begin(REPEATABLE_READ); var tx = runtime.begin(REPEATABLE_READ)) {
            byte[] value = bytes("new");
            tx.put("a", "k", value);
            value[0] = 'X';
            byte[] copy = tx.get("a", "k").orElseThrow();
            copy[0] = 'Y';
            Assertions.assertEquals("new", string(tx.get("a", "k")));
            tx.delete("b", "k");
            assertTrue(tx.get("b", "k").isEmpty());
            var result = tx.commit();
            assertTrue(result.pendingSecondaries().isEmpty());
            Assertions.assertSame(result, tx.commit());
            Assertions.assertEquals("old", string(old.get("b", "k")));
            try (var reader = runtime.begin(READ_COMMITTED)) {
                assertTrue(reader.get("b", "k").isEmpty());
                Assertions.assertEquals("new", string(reader.get("a", "k")));
            }
            assertThrows(MvccException.class, () -> tx.get("a", "k"));
        }
    }

    /**
     * Consistency reading cannot go beyond the relevant lock in prewrite and return part of the old snapshot.
     */
    @Test
    void relevantLiveLockBlocksButNewerLockDoesNotAffectOldSnapshot() {
        seed("old");
        try (var old = runtime.begin(REPEATABLE_READ); var writer = runtime.begin(REPEATABLE_READ)) {
            writer.put("a", "k", bytes("new"));
            writer.put("b", "k", bytes("new"));
            writer.prewrite();
            Assertions.assertEquals("old", string(old.get("a", "k")));
            try (var fresh = runtime.begin(READ_COMMITTED)) {
                var failure = assertThrows(MvccException.class, () -> fresh.get("b", "k"));
                Assertions.assertEquals(ErrorCode.LOCK_CONFLICT, failure.errorCode());
                assertTrue(failure.retryable());
            }
            Assertions.assertEquals("new", string(writer.get("b", "k")));
            assertThrows(MvccException.class, () -> writer.put("a", "later", bytes("frozen")));
            writer.commit();
            Assertions.assertEquals("old", string(old.get("b", "k")));
        }
    }

    /**
     * Read must wait for decision before missing version.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void allocatedCommitTimestampCannotCreateAHoleInSnapshot() throws Exception {
        seed("old");
        var pausedA = new FaultStore(a);
        pausedA.pauseCommit.set(true);
        try (
                var executor = Executors.newSingleThreadExecutor();
                var writer = runtime(pausedA, b).begin(REPEATABLE_READ)) {
            writer.put("a", "k", bytes("new"));
            writer.put("b", "k", bytes("new"));
            var future = executor.submit(writer::commit);
            try {
                assertTrue(pausedA.commitEntered.await(5, TimeUnit.SECONDS));
                try (var reader = runtime.begin(READ_COMMITTED)) {
                    var view = reader.statement();
                    Assertions.assertEquals(ErrorCode.LOCK_CONFLICT,
                            assertThrows(MvccException.class, () -> view.get("b", "k")).errorCode());
                    pausedA.commitAllowed.countDown();
                    var commit = future.get(10, TimeUnit.SECONDS);
                    assertTrue(commit.commitTs() < view.readTs());
                    Assertions.assertEquals("new", string(view.get("a", "k")));
                    Assertions.assertEquals("new", string(view.get("b", "k")));
                }
            } finally {
                pausedA.commitAllowed.countDown();
            }
        }
    }

    /**
     * Multiple independent coordinator concurrent executes cross-shard reading and writing, with confirmed commit times equal to two final values.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void concurrentCoordinatorsPreserveCrossShardReadModifyWriteHistory() throws Exception {
        seed("0");
        int writers = 4;
        int commitsPerWriter = 15;
        var gate = new CountDownLatch(1);
        var successes = new AtomicInteger();
        try (var executor = Executors.newFixedThreadPool(writers)) {
            var futures = new java.util.ArrayList<java.util.concurrent.Future<?>>();
            for (int worker = 0; worker < writers; worker++) {
                futures.add(executor.submit(() -> {
                    var coordinator = new PercolatorRuntime(new MysqlTimestampOracle(poolA), Map.of(
                            "a", new MysqlPercolatorStore("a", poolA), "b", new MysqlPercolatorStore("b", poolB)));
                    assertTrue(gate.await(5, TimeUnit.SECONDS));
                    int done = 0;
                    for (int attempts = 0; done < commitsPerWriter && attempts < 2000; attempts++) {
                        try (var tx = coordinator.begin(REPEATABLE_READ)) {
                            var view = tx.statement();
                            String first = string(view.get("a", "k"));
                            String second = string(view.get("b", "k"));
                            Assertions.assertEquals(first, second, "a fixed global snapshot must not tear across shards");
                            byte[] next = bytes(Integer.toString(Integer.parseInt(first) + 1));
                            tx.put("a", "k", next);
                            tx.put("b", "k", next);
                            assertTrue(tx.commit().pendingSecondaries().isEmpty());
                            done++;
                            successes.incrementAndGet();
                        } catch (final MvccException conflict) {
                            if (conflict.errorCode() != ErrorCode.LOCK_CONFLICT
                                    && conflict.errorCode() != ErrorCode.WRITE_CONFLICT
                                    && conflict.errorCode() != ErrorCode.TRANSACTION_ABORTED) {
                                throw conflict;
                            }
                            Thread.sleep(1);
                        }
                    }
                    Assertions.assertEquals(commitsPerWriter, done);
                    return null;
                }));
            }
            gate.countDown();
            for (var future : futures) {
                future.get(30, TimeUnit.SECONDS);
            }
        }
        Assertions.assertEquals(writers * commitsPerWriter, successes.get());
        try (var reader = runtime.begin(REPEATABLE_READ)) {
            Assertions.assertEquals(Integer.toString(successes.get()), string(reader.get("a", "k")));
            Assertions.assertEquals(Integer.toString(successes.get()), string(reader.get("b", "k")));
        }
    }

    /**
     * All shard prewrite applications will be successful before commit timestamp can be applied.
     */
    @Test
    void commitTimestampIsAllocatedAfterEveryPrewrite() {
        var calls = new AtomicInteger();
        TimestampOracle checked = () -> {
            if (calls.incrementAndGet() == 2) {
                Assertions.assertEquals(1, a.pending(10).size());
                Assertions.assertEquals(1, b.pending(10).size());
            }
            return tso.nextTimestamp();
        };
        var checkedRuntime = new PercolatorRuntime(checked, Map.of("a", a, "b", b));
        try (var tx = checkedRuntime.begin(REPEATABLE_READ)) {
            tx.put("b", "k", bytes("v"));
            tx.put("a", "k", bytes("v"));
            var result = tx.commit();
            Assertions.assertEquals(new PercolatorKey("a", "k"), result.handle().primary());
            Assertions.assertEquals(2, calls.get());
        }
    }

    /**
     * Primary has been commit but recreates the read side of runtime finalization original committeeTs when it is commit, but secondary failure.
     */
    @Test
    void committedSecondaryIsResolvedAfterCoordinatorRestart() {
        var brokenB = new FaultStore(b);
        brokenB.failFinish.set(true);
        var writerRuntime = runtime(a, brokenB);
        PercolatorRuntime.Commit committed;
        try (var tx = writerRuntime.begin(REPEATABLE_READ)) {
            tx.put("a", "k", bytes("A"));
            tx.put("b", "k", bytes("B"));
            committed = tx.commit();
            Assertions.assertEquals(List.of(new PercolatorKey("b", "k")), committed.pendingSecondaries());
        }
        var restarted = runtime(new MysqlPercolatorStore("a", poolA), new MysqlPercolatorStore("b", poolB));
        Assertions.assertEquals(committed.commitTs(), restarted.lookup(committed.handle()).commitTs());
        Assertions.assertEquals(1, b.pending(10).size());
        try (var reader = restarted.begin(REPEATABLE_READ)) {
            var statement = reader.statement();
            Assertions.assertEquals("A", string(statement.get("a", "k")));
            Assertions.assertEquals("B", string(statement.get("b", "k")));
        }
        assertTrue(b.pending(10).isEmpty());
        Assertions.assertEquals(0, restarted.recover(10));
        b.finish(committed.handle().startTs(), committed.handle().primary(),
                new Decision(State.COMMITTED, committed.commitTs()), List.of("k"));
        Assertions.assertEquals(committed.commitTs(), b.snapshot("k", Long.MAX_VALUE).value().orElseThrow().versionTs());
    }

    /**
     * If the primary decision is durable but its RPC response is lost, confirm the commit by querying the original primary.
     */
    @Test
    void lostPrimaryReplyIsConfirmedFromDurableWrite() {
        var brokenA = new FaultStore(a);
        brokenA.loseCommitReply.set(true);
        try (var tx = runtime(brokenA, b).begin(REPEATABLE_READ)) {
            tx.put("a", "k", bytes("A"));
            tx.put("b", "k", bytes("B"));
            var committed = tx.commit();
            Assertions.assertEquals(PercolatorTransaction.State.COMMITTED, tx.state());
            Assertions.assertEquals(committed.commitTs(), runtime.lookup(committed.handle()).commitTs());
            assertTrue(committed.pendingSecondaries().isEmpty());
        }
    }

    /**
     * Commit responses and queries have to be kept for UNKNOWN, close or rollback cannot delete secondary.
     */
    @Test
    void unknownPrimaryOutcomeCannotRollbackSecondaries() {
        var brokenA = new FaultStore(a);
        brokenA.loseCommitReply.set(true);
        brokenA.failInspect.set(true);
        var tx = runtime(brokenA, b).begin(REPEATABLE_READ);
        tx.put("a", "k", bytes("A"));
        tx.put("b", "k", bytes("B"));
        var error = assertThrows(MvccException.class, tx::commit);
        Assertions.assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN, error.errorCode());
        assertFalse(error.retryable());
        Assertions.assertEquals(tx.startTs(), error.txId());
        Assertions.assertEquals(PercolatorTransaction.State.UNKNOWN, tx.state());
        tx.close();
        assertThrows(MvccException.class, tx::rollback);
        Assertions.assertEquals(1, b.pending(10).size());
        Assertions.assertEquals(State.COMMITTED, runtime.lookup(tx.handle().orElseThrow()).state());
        Assertions.assertEquals(1, runtime.recover(10));
        assertTrue(b.pending(10).isEmpty());
    }

    /**
     * PENDING cannot extrapolate rollback when the original commit has not been executed; lease expires before deciding rollback.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void pendingAfterCommitFailureRemainsUnknownUntilFenced() throws Exception {
        var brokenA = new FaultStore(a);
        brokenA.failBeforeCommit.set(true);
        var tx = runtime(brokenA, b).begin(REPEATABLE_READ);
        tx.put("a", "k", bytes("A"));
        tx.put("b", "k", bytes("B"));
        Assertions.assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN, assertThrows(MvccException.class, tx::commit).errorCode());
        Assertions.assertEquals(State.PENDING, runtime.lookup(tx.handle().orElseThrow()).state());
        forceExpire("k");
        Assertions.assertEquals(State.ROLLED_BACK, runtime.lookup(tx.handle().orElseThrow()).state());
        Assertions.assertEquals(1, runtime.recover(10));
        assertTrue(a.pending(10).isEmpty());
        assertTrue(b.pending(10).isEmpty());
        Assertions.assertEquals(State.ROLLED_BACK, a.primary(new PercolatorKey("a", "k"), tx.startTs(), Action.COMMIT, tso.nextTimestamp()).state());
    }

    /**
     * Cleanup of unattended primary must first write a permanent rollback fence to prevent late prewrite resurrection.
     */
    @Test
    void missingPrimaryIsFencedBeforeSecondaryCleanup() {
        long start = tso.nextTimestamp();
        var primary = new PercolatorKey("a", "missing");
        b.prewrite(start, primary, 60_000, List.of(Mutation.put("secondary", bytes("v"))));
        try (var reader = runtime.begin(READ_COMMITTED)) {
            assertTrue(reader.get("b", "secondary").isEmpty());
        }
        assertTrue(b.pending(10).isEmpty());
        var failure = assertThrows(MvccException.class,
                () -> a.prewrite(start, primary, 60_000, List.of(Mutation.put("missing", bytes("late")))));
        Assertions.assertEquals(ErrorCode.TRANSACTION_ABORTED, failure.errorCode());
        Assertions.assertEquals(State.ROLLED_BACK, a.primary(primary, start, Action.COMMIT, tso.nextTimestamp()).state());
    }

    /**
     * Visible rollback and repeat recovery idempotent and will not remove the subsequent transaction lock.
     */
    @Test
    void rollbackReplayDoesNotTouchAnotherTransaction() {
        long oldStart;
        var primary = new PercolatorKey("a", "k");
        try (var tx = runtime.begin(REPEATABLE_READ)) {
            oldStart = tx.startTs();
            tx.put("a", "k", bytes("old"));
            tx.put("b", "k", bytes("old"));
            tx.prewrite();
            tx.rollback();
            tx.rollback();
        }
        try (var newer = runtime.begin(REPEATABLE_READ)) {
            newer.put("a", "k", bytes("new"));
            newer.put("b", "k", bytes("new"));
            newer.prewrite();
            b.finish(oldStart, primary, new Decision(State.ROLLED_BACK, 0), List.of("k"));
            Assertions.assertEquals(newer.startTs(), b.pending(10).getFirst().startTs());
            Assertions.assertEquals(State.ROLLED_BACK, a.primary(primary, oldStart, Action.ROLLBACK, 0).state());
            newer.commit();
        }
        Assertions.assertEquals(0, runtime.recover(10));
    }

    /**
     * The new commit version conflicts with the old transaction, and the whole batch of the same prewrite failed to keep half the data.
     */
    @Test
    void writeConflictRollsBackWholeLocalBatch() {
        long old = tso.nextTimestamp();
        seed("new");
        var primary = new PercolatorKey("a", "a-first");
        var failure = assertThrows(MvccException.class, () -> a.prewrite(old, primary, 60_000,
                List.of(Mutation.put("a-first", bytes("v")), Mutation.put("k", bytes("stale")))));
        Assertions.assertEquals(ErrorCode.WRITE_CONFLICT, failure.errorCode());
        assertTrue(a.pending(10).isEmpty());
        assertTrue(a.snapshot("a-first", Long.MAX_VALUE).value().isEmpty());
    }

    /**
     * Cross-shard prewrite failed first with rollback Primary and can no longer use the first half of commit already on prewrite.
     */
    @Test
    void secondaryPrewriteFailureFencesAndCleansEveryAttemptedKey() {
        var brokenB = new FaultStore(b);
        brokenB.losePrewriteReply.set(true);
        try (var tx = runtime(a, brokenB).begin(REPEATABLE_READ)) {
            tx.put("a", "k", bytes("A"));
            tx.put("b", "k", bytes("B"));
            assertThrows(MvccException.class, tx::prewrite);
            Assertions.assertEquals(PercolatorTransaction.State.ABORTED, tx.state());
            Assertions.assertEquals(State.ROLLED_BACK, runtime.lookup(tx.handle().orElseThrow()).state());
            assertTrue(a.pending(10).isEmpty());
            assertTrue(b.pending(10).isEmpty());
            assertThrows(MvccException.class, tx::commit);
        }
    }

    /**
     * Recreate coordinator only recovery decision or expired transaction and cannot suspend the prewrite valid without permission.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void restartRecoversExpiredLocksButKeepsLivePrimary() throws Exception {
        var tx = runtime.begin(REPEATABLE_READ);
        tx.put("a", "k", bytes("A"));
        tx.put("b", "k", bytes("B"));
        tx.prewrite();
        var restarted = runtime(new MysqlPercolatorStore("a", poolA), new MysqlPercolatorStore("b", poolB));
        Assertions.assertEquals(0, restarted.recover(10));
        Assertions.assertEquals(1, a.pending(10).size());
        forceExpire("k");
        restarted.recover(10);
        assertTrue(a.pending(10).isEmpty());
        assertTrue(b.pending(10).isEmpty());
        Assertions.assertEquals(State.ROLLED_BACK, restarted.lookup(tx.handle().orElseThrow()).state());
        Assertions.assertEquals(ErrorCode.TRANSACTION_ABORTED, assertThrows(MvccException.class, tx::commit).errorCode());
    }

    /**
     * Commit itself must also atomic refuse to expire when no read side or recovery is involved.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void primaryCommitItselfRejectsExpiredLease() throws Exception {
        try (var tx = runtime.begin(REPEATABLE_READ)) {
            tx.put("a", "k", bytes("A"));
            tx.put("b", "k", bytes("B"));
            tx.prewrite();
            forceExpire("k");
            Assertions.assertEquals(ErrorCode.TRANSACTION_ABORTED, assertThrows(MvccException.class, tx::commit).errorCode());
            Assertions.assertEquals(PercolatorTransaction.State.ABORTED, tx.state());
            assertTrue(a.pending(10).isEmpty());
            assertTrue(b.pending(10).isEmpty());
            assertTrue(a.snapshot("k", Long.MAX_VALUE).value().isEmpty());
        }
    }

    /**
     * The only non-revertable terminal state is determined by the same guard line when the Primary commit and concurrent are cleaned.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void primaryCommitAndRollbackRaceHasOneDurableWinner() throws Exception {
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int iteration = 0; iteration < 20; iteration++) {
                long start = tso.nextTimestamp();
                long commit = tso.nextTimestamp();
                var primary = new PercolatorKey("a", "race:" + iteration);
                a.prewrite(start, primary, 60_000, List.of(Mutation.put(primary.key(), bytes("v"))));
                var gate = new CountDownLatch(1);
                var committing = executor.submit(() -> {
                    assertTrue(gate.await(5, TimeUnit.SECONDS));
                    return a.primary(primary, start, Action.COMMIT, commit);
                });
                var rollingBack = executor.submit(() -> {
                    assertTrue(gate.await(5, TimeUnit.SECONDS));
                    return new MysqlPercolatorStore("a", poolA).primary(primary, start, Action.ROLLBACK, 0);
                });
                gate.countDown();
                Decision first = committing.get(10, TimeUnit.SECONDS);
                Decision second = rollingBack.get(10, TimeUnit.SECONDS);
                Assertions.assertEquals(first, second);
                Assertions.assertNotEquals(State.PENDING, first.state());
                Assertions.assertEquals(first, a.primary(primary, start, Action.INSPECT, 0));
            }
        }
    }

    /**
     * Idempotent prewrite does not renew or change the value, and replay prewrite after commit cannot recreate the lock.
     */
    @Test
    void prewriteReplayCannotChangeOrResurrectCommittedMutation() {
        long start = tso.nextTimestamp();
        var primary = new PercolatorKey("a", "k");
        List<Mutation> mutations = List.of(Mutation.put("k", bytes("v")));
        a.prewrite(start, primary, 60_000, mutations);
        a.prewrite(start, primary, 60_000, mutations);
        Assertions.assertEquals(ErrorCode.ILLEGAL_STATE, assertThrows(MvccException.class,
                () -> a.prewrite(start, primary, 60_000, List.of(Mutation.put("k", bytes("changed"))))).errorCode());
        long commit = tso.nextTimestamp();
        Assertions.assertEquals(State.COMMITTED, a.primary(primary, start, Action.COMMIT, commit).state());
        assertThrows(MvccException.class, () -> a.prewrite(start, primary, 60_000, mutations));
        assertTrue(a.pending(10).isEmpty());
        Assertions.assertEquals("v", string(a.snapshot("k", Long.MAX_VALUE).value().map(v -> v.value())));
        assertThrows(MvccException.class, () -> a.finish(start, primary, new Decision(State.ROLLED_BACK, 0), List.of("k")));
    }

    /**
     * The batch and the key-by-key paths are all written in four keys, changing only the number of local transaction times and maintaining the same meaning.
     */
    @Test
    void shardBatchingReducesLocalTransactionsWithoutChangingVisibility() {
        for (boolean batch : List.of(false, true)) {
            var countedA = new FaultStore(a);
            var countedB = new FaultStore(b);
            var optimized = new PercolatorRuntime(tso, Map.of("a", countedA, "b", countedB),
                    new PercolatorRuntime.Options(60_000, 4096, 16L * 1024 * 1024, batch));
            try (var tx = optimized.begin(REPEATABLE_READ)) {
                for (String shard : List.of("a", "b")) {
                    tx.put(shard, "k1:" + batch, bytes("one"));
                    tx.put(shard, "k2:" + batch, bytes("two"));
                }
                assertTrue(tx.commit().pendingSecondaries().isEmpty());
            }
            Assertions.assertEquals(batch ? 2 : 4, countedA.prewrites.get() + countedB.prewrites.get());
            Assertions.assertEquals(batch ? 2 : 3, countedA.finishes.get() + countedB.finishes.get());
            try (var reader = runtime.begin(READ_COMMITTED)) {
                Assertions.assertEquals("two", string(reader.get("b", "k2:" + batch)));
            }
        }
    }

    /**
     * Reads only local storage without lock, and does not search for Primary or penetrating centre metadata.
     */
    @Test
    void ordinaryCommittedReadDoesNotConsultPrimary() {
        seed("v");
        var countedA = new FaultStore(a);
        var countedB = new FaultStore(b);
        countedA.failInspect.set(true);
        countedB.failInspect.set(true);
        try (var tx = runtime(countedA, countedB).begin(READ_COMMITTED)) {
            Assertions.assertEquals("v", string(tx.get("a", "k")));
            Assertions.assertEquals("v", string(tx.get("b", "k")));
        }
    }

    /**
     * Small budget refused to write beyond the limit, valid overwrite could still be commit and pure read-only does not produce durable decision.
     */
    @Test
    void budgetsAndReadOnlyLifecycleAreExplicit() {
        var limited = new PercolatorRuntime(tso, Map.of("a", a, "b", b), new PercolatorRuntime.Options(60_000, 1, 10, true));
        try (var tx = limited.begin(REPEATABLE_READ)) {
            tx.put("a", "k", bytes("v"));
            Assertions.assertEquals(ErrorCode.RESOURCE_EXHAUSTED, assertThrows(MvccException.class,
                    () -> tx.put("b", "k", bytes("v"))).errorCode());
            tx.put("a", "k", bytes("next"));
            tx.commit();
        }
        try (var tx = runtime.begin(REPEATABLE_READ)) {
            var statement = tx.statement();
            var result = tx.commit();
            assertNull(result.handle());
            Assertions.assertEquals(0, result.commitTs());
            assertThrows(MvccException.class, () -> statement.get("a", "k"));
        }
        assertThrows(IllegalArgumentException.class, () -> new PercolatorKey("a", "\ud800"));
        assertThrows(IllegalArgumentException.class, () -> new PercolatorKey("a", "界".repeat(171)));
    }

    /**
     * Runs the A/B/B/A comparison at two temporary instances when visible enabled; normal returns do not perform a capacity measurement.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    @EnabledIfSystemProperty(named = "percolator.benchmark", matches = "true")
    void benchmarkPerKeyAgainstShardBatches() throws Exception {
        for (boolean batch : List.of(false, true, true, false)) {
            MysqlPercolatorBenchmark.run(tso, Map.of("a", a, "b", b), 50, 2, batch);
            var result = MysqlPercolatorBenchmark.run(tso, Map.of("a", a, "b", b), 50, 10, batch);
            System.out.println("PERCOLATOR_BENCHMARK " + result.json());
            Assertions.assertEquals(0, result.failures());
            assertTrue(result.committed() > 0);
        }
    }

    /**
     * Start running again based on two real shard configurations.
     *
     * @param first first
     * @param second second
     * @return the operation result
     */
    private PercolatorRuntime runtime(final PercolatorStore first, final PercolatorStore second) {
        return new PercolatorRuntime(tso, Map.of("a", first, "b", second));
    }

    /**
     * Creates an initial version of two pieces of the same value.
     *
     * @param value value to write
     */
    private void seed(final String value) {
        try (var tx = runtime.begin(REPEATABLE_READ)) {
            tx.put("a", "k", bytes(value));
            tx.put("b", "k", bytes(value));
            tx.commit();
        }
    }

    /**
     * Test SQL to advance lease, avoid relying on unstable wall clocks.
     *
     * @param key business key
     * @throws Exception if the operation cannot complete
     */
    private void forceExpire(final String key) throws Exception {
        try (var connection = poolA.getConnection(); var update = connection.prepareStatement("update pc_lock set expires_at=0 where user_key=?")) {
            update.setBytes(1, bytes(key));
            Assertions.assertEquals(1, update.executeUpdate());
        }
    }

    /**
     * Generates test UTF-8 values.
     *
     * @param value value to write
     * @return the operation result
     */
    private static byte[] bytes(final String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Converts the results of an optional binary test to a string.
     *
     * @param value value to write
     * @return the operation result
     */
    private static String string(final Optional<byte[]> value) {
        return value.map(v -> new String(v, StandardCharsets.UTF_8)).orElse(null);
    }

    /**
     * Injecting a response to loss and materialization failure at the real database border, and counting the number of batch calls.
     */
    private static final class FaultStore implements PercolatorStore {

        private final PercolatorStore delegate;

        final AtomicBoolean loseCommitReply = new AtomicBoolean();

        final AtomicBoolean failBeforeCommit = new AtomicBoolean();

        final AtomicBoolean failInspect = new AtomicBoolean();

        final AtomicBoolean failFinish = new AtomicBoolean();

        final AtomicBoolean losePrewriteReply = new AtomicBoolean();

        final AtomicBoolean pauseCommit = new AtomicBoolean();

        final CountDownLatch commitEntered = new CountDownLatch(1);

        final CountDownLatch commitAllowed = new CountDownLatch(1);

        final AtomicInteger prewrites = new AtomicInteger();

        final AtomicInteger finishes = new AtomicInteger();

        /**
         * Packaging is real storage and does not forge any durable decision.
         *
         * @param delegate delegate
         */
        FaultStore(final PercolatorStore delegate) {
            this.delegate = delegate;
        }

        /**
         * Returns the real shard number.
         *
         * @return the operation result
         */
        @Override
        public String shardId() {
            return delegate.shardId();
        }

        /**
         * Initialize real storage.
         */
        @Override
        public void initialize() {
            delegate.initialize();
        }

        /**
         * You can discard a reply after the real prewrite drop.
         *
         * @param start start
         * @param primary original Percolator primary identity
         * @param ttl ttl
         * @param mutations transaction write set
         */
        @Override
        public void prewrite(final long start, final PercolatorKey primary, final long ttl, final List<Mutation> mutations) {
            prewrites.incrementAndGet();
            delegate.prewrite(start, primary, ttl, mutations);
            if (losePrewriteReply.getAndSet(false)) {
                throw unavailable(start);
            }
        }

        /**
         * Keeps a true and consistent reading path.
         *
         * @param key business key
         * @param readTs snapshot read timestamp
         * @return the operation result
         */
        @Override
        public Snapshot snapshot(final String key, final long readTs) {
            return delegate.snapshot(key, readTs);
        }

        /**
         * The page break continues to use a true permanent candidate index and does not bypass the original key.
         *
         * @param range range
         * @param afterKeyExclusive after key exclusive
         * @param limit maximum number of items
         * @return the operation result
         */
        @Override
        public List<String> keys(final com.flyingzc.azir.storage.KeyRange range, final String afterKeyExclusive, final int limit) {
            return delegate.keys(range, afterKeyExclusive, limit);
        }

        /**
         * Distinguishing commit before failure, commit returns lost and status queries are not available.
         *
         * @param key business key
         * @param start start
         * @param action action
         * @param commitTs durable commit timestamp
         * @return the operation result
         */
        @Override
        public Decision primary(final PercolatorKey key, final long start, final Action action, final long commitTs) {
            if (action == Action.COMMIT && pauseCommit.get()) {
                commitEntered.countDown();
                try {
                    if (!commitAllowed.await(10, TimeUnit.SECONDS)) {
                        throw unavailable(start);
                    }
                } catch (final InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw unavailable(start);
                }
            }
            if ((action == Action.COMMIT && failBeforeCommit.getAndSet(false))
                    || (action == Action.INSPECT && failInspect.get())) {
                throw unavailable(start);
            }
            Decision result = delegate.primary(key, start, action, commitTs);
            if (action == Action.COMMIT && loseCommitReply.getAndSet(false)) {
                throw unavailable(start);
            }
            return result;
        }

        /**
         * Failure is not materialization secondary, the lock must be kept for recovery.
         *
         * @param start start
         * @param primary original Percolator primary identity
         * @param decision decision
         * @param keys keys
         */
        @Override
        public void finish(final long start, final PercolatorKey primary, final Decision decision, final List<String> keys) {
            finishes.incrementAndGet();
            if (failFinish.get()) {
                throw unavailable(start);
            }
            delegate.finish(start, primary, decision, keys);
        }

        /**
         * Use real residue locks to detect.
         *
         * @param limit maximum number of items
         * @return the operation result
         */
        @Override
        public List<Lock> pending(final int limit) {
            return delegate.pending(limit);
        }

        /**
         * Constructs an identifiable failure injection anomaly.
         *
         * @param start start
         * @return the operation result
         */
        private static MvccException unavailable(final long start) {
            return new MvccException(ErrorCode.STORAGE_UNAVAILABLE, "injected lost reply or outage", true, start);
        }
    }
}
