package mvcc.wal;

import mvcc.common.ErrorCode;
import mvcc.common.IsolationLevel;
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
import static mvcc.wal.WalMvccRuntimeTest.*;
import static org.junit.jupiter.api.Assertions.*;

/** 使用真实进程强杀、响应丢失和并发合批验证持久提交合同。 */
class WalRecoveryTest {
    @TempDir Path directory;

    /** 追加前强杀后，未提交写集不可恢复为已提交版本。 */
    @Test void killBeforeAppendHasNoCommittedVersion() throws Exception { killAndRecover("BEFORE_APPEND", false); }

    /** 刷盘后尚未发布时强杀，重启必须恢复整笔跨片事务。 */
    @Test void killAfterForceRecoversWholeTransaction() throws Exception { killAndRecover("AFTER_FORCE", true); }

    /** 发布后响应前强杀，持久查询仍能核对原事务号。 */
    @Test void killAfterPublishRetainsCommitReceipt() throws Exception { killAndRecover("AFTER_PUBLISH", true); }

    /** 调用方已获成功后强杀，确认过的数据不可丢失。 */
    @Test void killAfterAcknowledgementRetainsData() throws Exception { killAndRecover("ACK", true); }

    /** 子进程在边界停住后强杀，再打开同一日志核对结果。 */
    private void killAndRecover(String stage, boolean committed) throws Exception {
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
            assertEquals(committed, recovered.findCommitted(id).isPresent());
            var reader = recovered.begin(IsolationLevel.REPEATABLE_READ);
            assertEquals(committed, recovered.get(reader, "a", "balance").isPresent());
            assertEquals(committed, recovered.get(reader, "b", "balance").isPresent());
            if (committed) {
                assertArrayEquals(new byte[]{90}, recovered.get(reader, "a", "balance").orElseThrow());
                assertArrayEquals(new byte[]{110}, recovered.get(reader, "b", "balance").orElseThrow());
                assertEquals(1, recovered.findCommitted(id).orElseThrow().commitSequence());
            }
        }
    }

    /** 刷盘后注入失败必须返回未知，停止运行并在重开后恢复为已提交。 */
    @Test void durableFailureCannotTurnIntoRollback() {
        UUID id;
        try (var runtime = WalMvccRuntime.openForTest(directory, Set.of("a"), Map.of(), WalOptions.defaults(), stage -> {
            if (stage == WalMvccRuntime.Stage.AFTER_FORCE) { throw new IllegalStateException("injected crash"); }
        })) {
            var tx = runtime.begin(IsolationLevel.REPEATABLE_READ); id = tx.id();
            runtime.put(tx, "a", "k", bytes("durable"));
            assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN, assertThrows(WalMvccException.class,
                    () -> runtime.commit(tx)).errorCode());
            assertThrows(WalMvccException.class, () -> runtime.rollback(tx));
            assertThrows(WalMvccException.class, () -> runtime.begin(IsolationLevel.REPEATABLE_READ));
        }
        try (var recovered = WalMvccRuntime.open(directory, Set.of("a"), WalOptions.defaults())) {
            assertTrue(recovered.findCommitted(id).isPresent());
            var tx = recovered.begin(IsolationLevel.REPEATABLE_READ);
            assertEquals("durable", text(recovered.get(tx, "a", "k").orElseThrow()));
        }
    }

    /** 大量独立事务合批刷盘，所有凭据和结果保持唯一且完整。 */
    @Test void groupCommitSharesForcesWithoutLosingTransactions() throws Exception {
        var o = WalOptions.defaults();
        var limits = new WalOptions(100, 10, 1024, 100, 64, 20,
                o.maxWalBytes(), o.maxMemoryBytes(), 5000, 10, 20);
        try (var runtime = WalMvccRuntime.open(directory, Set.of("a"), limits);
             var tasks = Executors.newVirtualThreadPerTaskExecutor()) {
            var ready = new CountDownLatch(32);
            var start = new CountDownLatch(1);
            var futures = new ArrayList<java.util.concurrent.Future<WalReceipt>>();
            for (int i = 0; i < 32; i++) {
                String key = "k" + i;
                futures.add(tasks.submit(() -> {
                    var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
                    runtime.put(tx, "a", key, bytes(key));
                    ready.countDown(); await(start);
                    return runtime.commit(tx);
                }));
            }
            assertTrue(ready.await(5, TimeUnit.SECONDS)); start.countDown();
            var receipts = new java.util.HashSet<Long>();
            for (var future : futures) { receipts.add(future.get(5, TimeUnit.SECONDS).commitSequence()); }
            assertEquals(32, receipts.size());
            assertTrue(runtime.stats().groupForces() < 32);
            assertEquals(32, runtime.stats().publishedSequence());
        }
        try (var recovered = WalMvccRuntime.open(directory, Set.of("a"), limits)) {
            var tx = recovered.begin(IsolationLevel.REPEATABLE_READ);
            assertEquals(32, recovered.scan(tx, "a", null, null).size());
        }
    }

    /** 不同 WAL 不能接管已绑定后端，落后日志也不能覆盖领先检查点。 */
    @Test void backendBindingAndCheckpointMustMatchWal() {
        var store = new TestMaterializedStore();
        try (var runtime = WalMvccRuntime.open(directory, Map.of("a", store), WalOptions.defaults())) {
            write(runtime, "a", "k", "one"); runtime.flush();
        }
        assertThrows(IllegalStateException.class, () -> WalMvccRuntime.open(directory.resolve("other"),
                Map.of("a", store), WalOptions.defaults()));
        store.apply(List.of(new WalCommit(UUID.randomUUID(), 2, 1, List.of(new WalMutation("a", "k", bytes("two"))))));
        assertThrows(WalMvccException.class, () -> WalMvccRuntime.open(directory, Map.of("a", store), WalOptions.defaults()));
    }
}
