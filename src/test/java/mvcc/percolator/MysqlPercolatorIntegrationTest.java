package mvcc.percolator;

import mvcc.common.ErrorCode;
import mvcc.common.MvccException;
import mvcc.storage.Mutation;
import mvcc.storage.mysql.MysqlDataSourceFactory;
import mvcc.tso.TimestampOracle;
import mvcc.tso.mysql.MysqlTimestampOracle;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import mvcc.e2e.MysqlPercolatorBenchmark;
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
import static mvcc.common.IsolationLevel.*;
import static mvcc.percolator.PercolatorStore.*;
import static org.junit.jupiter.api.Assertions.*;

/** 用两个独立 MySQL 实例验证 primary 决议、跨片快照、恢复竞态和批量优化。 */
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

    /** 创建仅供本测试使用的连接池及隔离协议表。 */
    @BeforeAll
    static void open() {
        poolA = MysqlDataSourceFactory.create(MYSQL_A.getJdbcUrl(), MYSQL_A.getUsername(), MYSQL_A.getPassword());
        poolB = MysqlDataSourceFactory.create(MYSQL_B.getJdbcUrl(), MYSQL_B.getUsername(), MYSQL_B.getPassword());
        new MysqlPercolatorStore("a", poolA).initialize();
        new MysqlPercolatorStore("b", poolB).initialize();
        new MysqlTimestampOracle(poolA).initialize(100);
    }

    /** 先关闭连接池再由 Testcontainers 回收测试数据库。 */
    @AfterAll
    static void close() throws Exception {
        if (poolA instanceof AutoCloseable closeable) { closeable.close(); }
        if (poolB instanceof AutoCloseable closeable) { closeable.close(); }
    }

    /** 清空测试协议状态，不重置全局时间戳以避免历史身份复用。 */
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

    /** RC 每语句取得新边界，RR 和已保存语句视图保持原始跨片快照。 */
    @Test
    void rcAndRrUseGlobalStatementAndTransactionSnapshots() {
        seed("old");
        try (var rr = runtime.begin(REPEATABLE_READ); var rc = runtime.begin(READ_COMMITTED)) {
            var statement = rc.statement();
            assertEquals("old", string(rr.get("a", "k")));
            assertEquals("old", string(statement.get("b", "k")));
            seed("new");
            assertEquals("old", string(rr.get("b", "k")));
            assertEquals("old", string(statement.get("a", "k")));
            assertEquals("new", string(rc.get("a", "k")));
            assertEquals("new", string(rc.get("b", "k")));
        }
    }

    /** 写入与读取都防御性复制，删除以历史 tombstone 保持旧快照。 */
    @Test
    void ownsWritesAndPreservesDeleteHistory() {
        seed("old");
        try (var old = runtime.begin(REPEATABLE_READ); var tx = runtime.begin(REPEATABLE_READ)) {
            byte[] value = bytes("new");
            tx.put("a", "k", value);
            value[0] = 'X';
            byte[] copy = tx.get("a", "k").orElseThrow();
            copy[0] = 'Y';
            assertEquals("new", string(tx.get("a", "k")));
            tx.delete("b", "k");
            assertTrue(tx.get("b", "k").isEmpty());
            var result = tx.commit();
            assertTrue(result.pendingSecondaries().isEmpty());
            assertSame(result, tx.commit());
            assertEquals("old", string(old.get("b", "k")));
            try (var reader = runtime.begin(READ_COMMITTED)) {
                assertTrue(reader.get("b", "k").isEmpty());
                assertEquals("new", string(reader.get("a", "k")));
            }
            assertThrows(MvccException.class, () -> tx.get("a", "k"));
        }
    }

    /** 一致性读不能跨过正在预写的相关锁而返回部分旧快照。 */
    @Test
    void relevantLiveLockBlocksButNewerLockDoesNotAffectOldSnapshot() {
        seed("old");
        try (var old = runtime.begin(REPEATABLE_READ); var writer = runtime.begin(REPEATABLE_READ)) {
            writer.put("a", "k", bytes("new"));
            writer.put("b", "k", bytes("new"));
            writer.prewrite();
            assertEquals("old", string(old.get("a", "k")));
            try (var fresh = runtime.begin(READ_COMMITTED)) {
                var failure = assertThrows(MvccException.class, () -> fresh.get("b", "k"));
                assertEquals(ErrorCode.LOCK_CONFLICT, failure.errorCode());
                assertTrue(failure.retryable());
            }
            assertEquals("new", string(writer.get("b", "k")));
            assertThrows(MvccException.class, () -> writer.put("a", "later", bytes("frozen")));
            writer.commit();
            assertEquals("old", string(old.get("b", "k")));
        }
    }

    /** commitTs 早于读边界但 primary 尚未提交时，读必须等待决议而不能漏掉该版本。 */
    @Test
    void allocatedCommitTimestampCannotCreateAHoleInSnapshot() throws Exception {
        seed("old");
        var pausedA = new FaultStore(a);
        pausedA.pauseCommit.set(true);
        try (var executor = Executors.newSingleThreadExecutor();
             var writer = runtime(pausedA, b).begin(REPEATABLE_READ)) {
            writer.put("a", "k", bytes("new"));
            writer.put("b", "k", bytes("new"));
            var future = executor.submit(writer::commit);
            try {
                assertTrue(pausedA.commitEntered.await(5, TimeUnit.SECONDS));
                try (var reader = runtime.begin(READ_COMMITTED)) {
                    var view = reader.statement();
                    assertEquals(ErrorCode.LOCK_CONFLICT,
                            assertThrows(MvccException.class, () -> view.get("b", "k")).errorCode());
                    pausedA.commitAllowed.countDown();
                    var commit = future.get(10, TimeUnit.SECONDS);
                    assertTrue(commit.commitTs() < view.readTs());
                    assertEquals("new", string(view.get("a", "k")));
                    assertEquals("new", string(view.get("b", "k")));
                }
            } finally { pausedA.commitAllowed.countDown(); }
        }
    }

    /** 多个独立协调器并发执行跨片读改写，已确认提交次数必须等于两片最终值。 */
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
                            assertEquals(first, second, "a fixed global snapshot must not tear across shards");
                            byte[] next = bytes(Integer.toString(Integer.parseInt(first) + 1));
                            tx.put("a", "k", next);
                            tx.put("b", "k", next);
                            assertTrue(tx.commit().pendingSecondaries().isEmpty());
                            done++;
                            successes.incrementAndGet();
                        } catch (MvccException conflict) {
                            if (conflict.errorCode() != ErrorCode.LOCK_CONFLICT
                                    && conflict.errorCode() != ErrorCode.WRITE_CONFLICT
                                    && conflict.errorCode() != ErrorCode.TRANSACTION_ABORTED) { throw conflict; }
                            Thread.sleep(1);
                        }
                    }
                    assertEquals(commitsPerWriter, done);
                    return null;
                }));
            }
            gate.countDown();
            for (var future : futures) { future.get(30, TimeUnit.SECONDS); }
        }
        assertEquals(writers * commitsPerWriter, successes.get());
        try (var reader = runtime.begin(REPEATABLE_READ)) {
            assertEquals(Integer.toString(successes.get()), string(reader.get("a", "k")));
            assertEquals(Integer.toString(successes.get()), string(reader.get("b", "k")));
        }
    }

    /** 所有分片预写成功后才能申请提交时间戳。 */
    @Test
    void commitTimestampIsAllocatedAfterEveryPrewrite() {
        var calls = new AtomicInteger();
        TimestampOracle checked = () -> {
            if (calls.incrementAndGet() == 2) {
                assertEquals(1, a.pending(10).size());
                assertEquals(1, b.pending(10).size());
            }
            return tso.nextTimestamp();
        };
        var checkedRuntime = new PercolatorRuntime(checked, Map.of("a", a, "b", b));
        try (var tx = checkedRuntime.begin(REPEATABLE_READ)) {
            tx.put("b", "k", bytes("v"));
            tx.put("a", "k", bytes("v"));
            var result = tx.commit();
            assertEquals(new PercolatorKey("a", "k"), result.handle().primary());
            assertEquals(2, calls.get());
        }
    }

    /** primary 已提交但 secondary 故障时，重建运行时的读侧补交原 commitTs。 */
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
            assertEquals(List.of(new PercolatorKey("b", "k")), committed.pendingSecondaries());
        }
        var restarted = runtime(new MysqlPercolatorStore("a", poolA), new MysqlPercolatorStore("b", poolB));
        assertEquals(committed.commitTs(), restarted.lookup(committed.handle()).commitTs());
        assertEquals(1, b.pending(10).size());
        try (var reader = restarted.begin(REPEATABLE_READ)) {
            var statement = reader.statement();
            assertEquals("A", string(statement.get("a", "k")));
            assertEquals("B", string(statement.get("b", "k")));
        }
        assertTrue(b.pending(10).isEmpty());
        assertEquals(0, restarted.recover(10));
        b.finish(committed.handle().startTs(), committed.handle().primary(),
                new Decision(State.COMMITTED, committed.commitTs()), List.of("k"));
        assertEquals(committed.commitTs(), b.snapshot("k", Long.MAX_VALUE).value().orElseThrow().versionTs());
    }

    /** primary 已落盘但 RPC 回复丢失时，查询原 primary 即可确认提交。 */
    @Test
    void lostPrimaryReplyIsConfirmedFromDurableWrite() {
        var brokenA = new FaultStore(a);
        brokenA.loseCommitReply.set(true);
        try (var tx = runtime(brokenA, b).begin(REPEATABLE_READ)) {
            tx.put("a", "k", bytes("A"));
            tx.put("b", "k", bytes("B"));
            var committed = tx.commit();
            assertEquals(PercolatorTransaction.State.COMMITTED, tx.state());
            assertEquals(committed.commitTs(), runtime.lookup(committed.handle()).commitTs());
            assertTrue(committed.pendingSecondaries().isEmpty());
        }
    }

    /** 提交回复和查询均失败必须保留 UNKNOWN，close 或 rollback 不能删除 secondary。 */
    @Test
    void unknownPrimaryOutcomeCannotRollbackSecondaries() {
        var brokenA = new FaultStore(a);
        brokenA.loseCommitReply.set(true);
        brokenA.failInspect.set(true);
        var tx = runtime(brokenA, b).begin(REPEATABLE_READ);
        tx.put("a", "k", bytes("A"));
        tx.put("b", "k", bytes("B"));
        var error = assertThrows(MvccException.class, tx::commit);
        assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN, error.errorCode());
        assertFalse(error.retryable());
        assertEquals(tx.startTs(), error.txId());
        assertEquals(PercolatorTransaction.State.UNKNOWN, tx.state());
        tx.close();
        assertThrows(MvccException.class, tx::rollback);
        assertEquals(1, b.pending(10).size());
        assertEquals(State.COMMITTED, runtime.lookup(tx.handle().orElseThrow()).state());
        assertEquals(1, runtime.recover(10));
        assertTrue(b.pending(10).isEmpty());
    }

    /** 原提交尚未执行时 PENDING 也不能推断回滚；租约过期后才能决定回滚。 */
    @Test
    void pendingAfterCommitFailureRemainsUnknownUntilFenced() throws Exception {
        var brokenA = new FaultStore(a);
        brokenA.failBeforeCommit.set(true);
        var tx = runtime(brokenA, b).begin(REPEATABLE_READ);
        tx.put("a", "k", bytes("A"));
        tx.put("b", "k", bytes("B"));
        assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN, assertThrows(MvccException.class, tx::commit).errorCode());
        assertEquals(State.PENDING, runtime.lookup(tx.handle().orElseThrow()).state());
        forceExpire("k");
        assertEquals(State.ROLLED_BACK, runtime.lookup(tx.handle().orElseThrow()).state());
        assertEquals(1, runtime.recover(10));
        assertTrue(a.pending(10).isEmpty());
        assertTrue(b.pending(10).isEmpty());
        assertEquals(State.ROLLED_BACK, a.primary(new PercolatorKey("a", "k"), tx.startTs(), Action.COMMIT, tso.nextTimestamp()).state());
    }

    /** 清理未出现的 primary 必须先写永久回滚栅栏，阻止迟到预写复活。 */
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
        assertEquals(ErrorCode.TRANSACTION_ABORTED, failure.errorCode());
        assertEquals(State.ROLLED_BACK, a.primary(primary, start, Action.COMMIT, tso.nextTimestamp()).state());
    }

    /** 显式回滚和重复恢复幂等，并且不会删除后来事务的锁。 */
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
            assertEquals(newer.startTs(), b.pending(10).getFirst().startTs());
            assertEquals(State.ROLLED_BACK, a.primary(primary, oldStart, Action.ROLLBACK, 0).state());
            newer.commit();
        }
        assertEquals(0, runtime.recover(10));
    }

    /** 新提交版本使旧写事务冲突，同片预写整批失败不能保留半批数据。 */
    @Test
    void writeConflictRollsBackWholeLocalBatch() {
        long old = tso.nextTimestamp();
        seed("new");
        var primary = new PercolatorKey("a", "a-first");
        var failure = assertThrows(MvccException.class, () -> a.prewrite(old, primary, 60_000,
                List.of(Mutation.put("a-first", bytes("v")), Mutation.put("k", bytes("stale")))));
        assertEquals(ErrorCode.WRITE_CONFLICT, failure.errorCode());
        assertTrue(a.pending(10).isEmpty());
        assertTrue(a.snapshot("a-first", Long.MAX_VALUE).value().isEmpty());
    }

    /** 跨片预写失败时先回滚 primary，不能再提交已经预写的前半部分。 */
    @Test
    void secondaryPrewriteFailureFencesAndCleansEveryAttemptedKey() {
        var brokenB = new FaultStore(b);
        brokenB.losePrewriteReply.set(true);
        try (var tx = runtime(a, brokenB).begin(REPEATABLE_READ)) {
            tx.put("a", "k", bytes("A"));
            tx.put("b", "k", bytes("B"));
            assertThrows(MvccException.class, tx::prewrite);
            assertEquals(PercolatorTransaction.State.ABORTED, tx.state());
            assertEquals(State.ROLLED_BACK, runtime.lookup(tx.handle().orElseThrow()).state());
            assertTrue(a.pending(10).isEmpty());
            assertTrue(b.pending(10).isEmpty());
            assertThrows(MvccException.class, tx::commit);
        }
    }

    /** 重建协调器只恢复已决议或过期事务，不能擅自中止仍有效的预写。 */
    @Test
    void restartRecoversExpiredLocksButKeepsLivePrimary() throws Exception {
        var tx = runtime.begin(REPEATABLE_READ);
        tx.put("a", "k", bytes("A"));
        tx.put("b", "k", bytes("B"));
        tx.prewrite();
        var restarted = runtime(new MysqlPercolatorStore("a", poolA), new MysqlPercolatorStore("b", poolB));
        assertEquals(0, restarted.recover(10));
        assertEquals(1, a.pending(10).size());
        forceExpire("k");
        restarted.recover(10);
        assertTrue(a.pending(10).isEmpty());
        assertTrue(b.pending(10).isEmpty());
        assertEquals(State.ROLLED_BACK, restarted.lookup(tx.handle().orElseThrow()).state());
        assertEquals(ErrorCode.TRANSACTION_ABORTED, assertThrows(MvccException.class, tx::commit).errorCode());
    }

    /** 无读侧或恢复器介入时，提交自身也必须原子拒绝过期 primary。 */
    @Test
    void primaryCommitItselfRejectsExpiredLease() throws Exception {
        try (var tx = runtime.begin(REPEATABLE_READ)) {
            tx.put("a", "k", bytes("A"));
            tx.put("b", "k", bytes("B"));
            tx.prewrite();
            forceExpire("k");
            assertEquals(ErrorCode.TRANSACTION_ABORTED, assertThrows(MvccException.class, tx::commit).errorCode());
            assertEquals(PercolatorTransaction.State.ABORTED, tx.state());
            assertTrue(a.pending(10).isEmpty());
            assertTrue(b.pending(10).isEmpty());
            assertTrue(a.snapshot("k", Long.MAX_VALUE).value().isEmpty());
        }
    }

    /** primary 提交与清理并发时，由同一 guard 行确定唯一不可反转终态。 */
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
                assertEquals(first, second);
                assertNotEquals(State.PENDING, first.state());
                assertEquals(first, a.primary(primary, start, Action.INSPECT, 0));
            }
        }
    }

    /** 幂等预写不续租也不改值，提交后重放预写不能重建锁。 */
    @Test
    void prewriteReplayCannotChangeOrResurrectCommittedMutation() {
        long start = tso.nextTimestamp();
        var primary = new PercolatorKey("a", "k");
        List<Mutation> mutations = List.of(Mutation.put("k", bytes("v")));
        a.prewrite(start, primary, 60_000, mutations);
        a.prewrite(start, primary, 60_000, mutations);
        assertEquals(ErrorCode.ILLEGAL_STATE, assertThrows(MvccException.class,
                () -> a.prewrite(start, primary, 60_000, List.of(Mutation.put("k", bytes("changed"))))).errorCode());
        long commit = tso.nextTimestamp();
        assertEquals(State.COMMITTED, a.primary(primary, start, Action.COMMIT, commit).state());
        assertThrows(MvccException.class, () -> a.prewrite(start, primary, 60_000, mutations));
        assertTrue(a.pending(10).isEmpty());
        assertEquals("v", string(a.snapshot("k", Long.MAX_VALUE).value().map(v -> v.value())));
        assertThrows(MvccException.class, () -> a.finish(start, primary, new Decision(State.ROLLED_BACK, 0), List.of("k")));
    }

    /** 批量与逐键路径都写入四键，仅改变本地事务次数并保持同一语义。 */
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
            assertEquals(batch ? 2 : 4, countedA.prewrites.get() + countedB.prewrites.get());
            assertEquals(batch ? 2 : 3, countedA.finishes.get() + countedB.finishes.get());
            try (var reader = runtime.begin(READ_COMMITTED)) {
                assertEquals("two", string(reader.get("b", "k2:" + batch)));
            }
        }
    }

    /** 无锁读取只访问本地存储，不查 primary 或逐笔中心元数据。 */
    @Test
    void ordinaryCommittedReadDoesNotConsultPrimary() {
        seed("v");
        var countedA = new FaultStore(a);
        var countedB = new FaultStore(b);
        countedA.failInspect.set(true);
        countedB.failInspect.set(true);
        try (var tx = runtime(countedA, countedB).begin(READ_COMMITTED)) {
            assertEquals("v", string(tx.get("a", "k")));
            assertEquals("v", string(tx.get("b", "k")));
        }
    }

    /** 小预算拒绝超限写入，合法覆盖仍可提交，纯只读不产生持久决议。 */
    @Test
    void budgetsAndReadOnlyLifecycleAreExplicit() {
        var limited = new PercolatorRuntime(tso, Map.of("a", a, "b", b), new PercolatorRuntime.Options(60_000, 1, 10, true));
        try (var tx = limited.begin(REPEATABLE_READ)) {
            tx.put("a", "k", bytes("v"));
            assertEquals(ErrorCode.RESOURCE_EXHAUSTED, assertThrows(MvccException.class,
                    () -> tx.put("b", "k", bytes("v"))).errorCode());
            tx.put("a", "k", bytes("next"));
            tx.commit();
        }
        try (var tx = runtime.begin(REPEATABLE_READ)) {
            var statement = tx.statement();
            var result = tx.commit();
            assertNull(result.handle());
            assertEquals(0, result.commitTs());
            assertThrows(MvccException.class, () -> statement.get("a", "k"));
        }
        assertThrows(IllegalArgumentException.class, () -> new PercolatorKey("a", "\ud800"));
        assertThrows(IllegalArgumentException.class, () -> new PercolatorKey("a", "界".repeat(171)));
    }

    /** 显式启用时在两个临时实例运行 A/B/B/A 对照；普通回归不执行容量测量。 */
    @Test
    @EnabledIfSystemProperty(named = "percolator.benchmark", matches = "true")
    void benchmarkPerKeyAgainstShardBatches() throws Exception {
        for (boolean batch : List.of(false, true, true, false)) {
            MysqlPercolatorBenchmark.run(tso, Map.of("a", a, "b", b), 50, 2, batch);
            var result = MysqlPercolatorBenchmark.run(tso, Map.of("a", a, "b", b), 50, 10, batch);
            System.out.println("PERCOLATOR_BENCHMARK " + result.json());
            assertEquals(0, result.failures());
            assertTrue(result.committed() > 0);
        }
    }

    /** 根据两个真实分片装配可重启运行时。 */
    private PercolatorRuntime runtime(PercolatorStore first, PercolatorStore second) {
        return new PercolatorRuntime(tso, Map.of("a", first, "b", second));
    }

    /** 创建两片同值的初始版本。 */
    private void seed(String value) {
        try (var tx = runtime.begin(REPEATABLE_READ)) {
            tx.put("a", "k", bytes(value));
            tx.put("b", "k", bytes(value));
            tx.commit();
        }
    }

    /** 用测试 SQL 推进租约，避免依赖不稳定的墙钟 sleep。 */
    private void forceExpire(String key) throws Exception {
        try (var connection = poolA.getConnection(); var update = connection.prepareStatement("update pc_lock set expires_at=0 where user_key=?")) {
            update.setBytes(1, bytes(key));
            assertEquals(1, update.executeUpdate());
        }
    }

    /** 生成测试 UTF-8 值。 */
    private static byte[] bytes(String value) { return value.getBytes(StandardCharsets.UTF_8); }

    /** 将可选二进制测试结果转为字符串。 */
    private static String string(Optional<byte[]> value) { return value.map(v -> new String(v, StandardCharsets.UTF_8)).orElse(null); }

    /** 在真实数据库边界注入响应丢失和物化故障，并统计批量调用次数。 */
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

        /** 包装真实存储，不伪造任何持久决议。 */
        FaultStore(PercolatorStore delegate) { this.delegate = delegate; }

        /** 返回真实分片编号。 */
        @Override
        public String shardId() { return delegate.shardId(); }

        /** 初始化真实存储。 */
        @Override
        public void initialize() { delegate.initialize(); }

        /** 可在真实预写落盘之后丢弃回复。 */
        @Override
        public void prewrite(long start, PercolatorKey primary, long ttl, List<Mutation> mutations) {
            prewrites.incrementAndGet();
            delegate.prewrite(start, primary, ttl, mutations);
            if (losePrewriteReply.getAndSet(false)) { throw unavailable(start); }
        }

        /** 保留真实一致性读路径。 */
        @Override
        public Snapshot snapshot(String key, long readTs) { return delegate.snapshot(key, readTs); }

        /** 分页仍使用真实永久候选索引，不绕过原始键。 */
        @Override
        public List<String> keys(mvcc.storage.KeyRange range, String afterKeyExclusive, int limit) {
            return delegate.keys(range, afterKeyExclusive, limit);
        }

        /** 区分提交前故障、提交后回复丢失和状态查询不可达。 */
        @Override
        public Decision primary(PercolatorKey key, long start, Action action, long commitTs) {
            if (action == Action.COMMIT && pauseCommit.get()) {
                commitEntered.countDown();
                try {
                    if (!commitAllowed.await(10, TimeUnit.SECONDS)) { throw unavailable(start); }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw unavailable(start);
                }
            }
            if ((action == Action.COMMIT && failBeforeCommit.getAndSet(false))
                    || (action == Action.INSPECT && failInspect.get())) { throw unavailable(start); }
            Decision result = delegate.primary(key, start, action, commitTs);
            if (action == Action.COMMIT && loseCommitReply.getAndSet(false)) { throw unavailable(start); }
            return result;
        }

        /** 故障时不物化 secondary，锁必须保留供恢复。 */
        @Override
        public void finish(long start, PercolatorKey primary, Decision decision, List<String> keys) {
            finishes.incrementAndGet();
            if (failFinish.get()) { throw unavailable(start); }
            delegate.finish(start, primary, decision, keys);
        }

        /** 使用真实残留锁发现。 */
        @Override
        public List<Lock> pending(int limit) { return delegate.pending(limit); }

        /** 构造可识别的故障注入异常。 */
        private static MvccException unavailable(long start) {
            return new MvccException(ErrorCode.STORAGE_UNAVAILABLE, "injected lost reply or outage", true, start);
        }
    }
}
