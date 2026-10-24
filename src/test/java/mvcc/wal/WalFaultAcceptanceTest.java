package mvcc.wal;

import mvcc.common.ErrorCode;
import mvcc.common.IsolationLevel;
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
import static mvcc.wal.WalMvccRuntimeTest.*;
import static org.junit.jupiter.api.Assertions.*;

/** 对真实日志写入、发布、关闭与租约边界执行可重复的故障验收。 */
class WalFaultAcceptanceTest {
    @TempDir Path directory;

    /** 真实写出部分帧后模拟磁盘满，恢复只能保留已确认前缀。 */
    @ParameterizedTest @ValueSource(ints = {8, 20})
    void partialWriteFailureCannotLoseAcknowledgedPrefix(int bytesIntoFrame) {
        var failAt = new AtomicLong(Long.MAX_VALUE);
        var io = new WalLog.IoHook() {
            /** 将下一次实际写入限制到故障位置。 */
            @Override public int writeLimit(long position, int remaining) {
                return (int) Math.min(remaining, Math.max(1, failAt.get() - position));
            }
            /** 在已经发生的部分写入之后抛出设备错误。 */
            @Override public void afterWrite(long position) throws IOException {
                if (position >= failAt.get()) { throw new IOException("injected ENOSPC after partial write"); }
            }
        };
        UUID confirmed;
        UUID uncertain;
        long prefixSize;
        try (var runtime = open(io, stage -> { }, System::nanoTime)) {
            confirmed = pair(runtime, "old").transactionId();
            prefixSize = runtime.stats().walBytes();
            failAt.set(prefixSize + bytesIntoFrame);
            var tx = pairedWrite(runtime, "new"); uncertain = tx.id();
            assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN, assertThrows(WalMvccException.class,
                    () -> runtime.commit(tx)).errorCode());
            assertTrue(runtime.findCommitted(uncertain).isEmpty());
            assertThrows(WalMvccException.class, () -> runtime.begin(IsolationLevel.READ_COMMITTED));
        }
        try (var runtime = WalMvccRuntime.open(directory, Set.of("a", "b"), WalOptions.defaults())) {
            assertEquals(prefixSize, runtime.stats().walBytes());
            assertTrue(runtime.findCommitted(confirmed).isPresent());
            assertTrue(runtime.findCommitted(uncertain).isEmpty());
            assertPair(runtime, "old");
            assertEquals(2, pair(runtime, "after-recovery").commitSequence());
        }
    }

    /** 完整帧在刷盘前失败时不可提前确认，恢复允许收养未确认完整记录。 */
    @Test void forceFailureHasUnknownOutcomeUntilRecovery() {
        forceBoundary(false);
    }

    /** 刷盘实际成功但返回错误时，恢复仍必须完成原事务。 */
    @Test void lostForceAcknowledgementMustRecoverTheSameTransaction() {
        forceBoundary(true);
    }

    /** 在刷盘前或刷盘后注入失败，验证确认与恢复的边界。 */
    private void forceBoundary(boolean afterForce) {
        var armed = new AtomicBoolean();
        var io = new WalLog.IoHook() {
            /** 模拟完整帧写完但刷盘调用失败。 */
            @Override public void beforeForce() throws IOException {
                if (!afterForce && armed.get()) { throw new IOException("injected force failure"); }
            }
            /** 模拟刷盘成功后确认丢失。 */
            @Override public void afterForce() throws IOException {
                if (afterForce && armed.get()) { throw new IOException("injected lost force acknowledgement"); }
            }
        };
        UUID id;
        try (var runtime = open(io, stage -> { }, System::nanoTime)) {
            pair(runtime, "old"); armed.set(true);
            var tx = pairedWrite(runtime, "new"); id = tx.id();
            assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN, assertThrows(WalMvccException.class,
                    () -> runtime.commit(tx)).errorCode());
            assertTrue(runtime.findCommitted(id).isEmpty());
        }
        try (var recovered = WalMvccRuntime.open(directory, Set.of("a", "b"), WalOptions.defaults())) {
            assertEquals(2, recovered.findCommitted(id).orElseThrow().commitSequence());
            assertPair(recovered, "new");
        }
    }

    /** 只安装第一个分片便失败时，不能确认半笔事务，恢复后必须安装完整写集。 */
    @Test void failureHalfwayThroughInstallationCannotPublishHalfATransaction() {
        var armed = new AtomicBoolean();
        UUID id;
        try (var runtime = open(WalLog.NORMAL_IO, stage -> {
            if (stage == WalMvccRuntime.Stage.DURING_INSTALL && armed.compareAndSet(true, false)) {
                throw new IllegalStateException("injected halfway installation failure");
            }
        }, System::nanoTime)) {
            pair(runtime, "old"); armed.set(true);
            var tx = pairedWrite(runtime, "new"); id = tx.id();
            assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN, assertThrows(WalMvccException.class,
                    () -> runtime.commit(tx)).errorCode());
            assertEquals(1, runtime.stats().publishedSequence());
            assertTrue(runtime.findCommitted(id).isEmpty());
            assertThrows(WalMvccException.class, () -> runtime.begin(IsolationLevel.REPEATABLE_READ));
        }
        try (var recovered = WalMvccRuntime.open(directory, Set.of("a", "b"), WalOptions.defaults())) {
            assertTrue(recovered.findCommitted(id).isPresent()); assertPair(recovered, "new");
        }
    }

    /** 同一事务并发提交两次只能形成一项日志和同一个结果。 */
    @Test void simultaneousDuplicateCommitSharesOneDecision() throws Exception {
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (var runtime = open(WalLog.NORMAL_IO, stage -> {
            if (stage == WalMvccRuntime.Stage.BEFORE_APPEND) { entered.countDown(); waitLong(release); }
        }, System::nanoTime); var tasks = Executors.newVirtualThreadPerTaskExecutor()) {
            var tx = pairedWrite(runtime, "once");
            var first = tasks.submit(() -> runtime.commit(tx));
            assertTrue(entered.await(30, TimeUnit.SECONDS));
            var second = tasks.submit(() -> runtime.commit(tx));
            try {
                assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN, assertThrows(WalMvccException.class,
                        () -> runtime.rollback(tx)).errorCode());
            } finally { release.countDown(); }
            assertEquals(first.get(30, TimeUnit.SECONDS), second.get(30, TimeUnit.SECONDS));
            assertEquals(1, runtime.stats().publishedSequence());
            assertEquals(1, runtime.stats().groupForces()); assertPair(runtime, "once");
        } finally { release.countDown(); }
    }

    /** 正常关闭与在途提交竞争时，已接受请求完成后才释放日志独占锁。 */
    @Test void closeDrainsAcceptedCommitBeforeReleasingWal() throws Exception {
        var forced = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var runtime = open(WalLog.NORMAL_IO, stage -> {
            if (stage == WalMvccRuntime.Stage.AFTER_FORCE) { forced.countDown(); waitLong(release); }
        }, System::nanoTime);
        UUID id;
        try (var tasks = Executors.newVirtualThreadPerTaskExecutor()) {
            var tx = pairedWrite(runtime, "closing"); id = tx.id();
            var commit = tasks.submit(() -> runtime.commit(tx));
            assertTrue(forced.await(30, TimeUnit.SECONDS));
            var close = tasks.submit(runtime::close);
            release.countDown();
            commit.get(30, TimeUnit.SECONDS); close.get(30, TimeUnit.SECONDS);
        } finally { release.countDown(); runtime.close(); }
        try (var reopened = WalMvccRuntime.open(directory, Set.of("a", "b"), WalOptions.defaults())) {
            assertTrue(reopened.findCommitted(id).isPresent()); assertPair(reopened, "closing");
        }
    }

    /** 期限与回收使用可控单调时钟，机器繁忙不改变期望交错。 */
    @Test void expiredReaderCannotKeepOrReuseAnOldSnapshot() {
        var clock = new AtomicLong();
        try (var runtime = open(WalLog.NORMAL_IO, stage -> { }, clock::get)) {
            pair(runtime, "old");
            var reader = runtime.begin(IsolationLevel.REPEATABLE_READ);
            var snapshot = runtime.beginReadStatement(reader);
            pair(runtime, "new");
            clock.addAndGet(TimeUnit.SECONDS.toNanos(61));
            runtime.collectGarbage(10);
            assertEquals(2, runtime.stats().safePoint());
            assertThrows(WalMvccException.class, () -> runtime.get(reader, "a", "k", snapshot));
            assertPair(runtime, "new");
        }
    }

    /** 在同一 WAL 目录装配包内注入点。 */
    private WalMvccRuntime open(WalLog.IoHook io, WalMvccRuntime.FaultHook hook,
                                java.util.function.LongSupplier clock) {
        return WalMvccRuntime.openForTest(directory, Set.of("a", "b"), Map.of(), WalOptions.defaults(), hook, io, clock);
    }

    /** 构造尚未提交的完整跨片写集。 */
    private static WalTransaction pairedWrite(WalMvccRuntime runtime, String value) {
        var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
        runtime.put(tx, "a", "k", bytes(value)); runtime.put(tx, "b", "k", bytes(value)); return tx;
    }

    /** 提交一笔跨片变更。 */
    private static WalReceipt pair(WalMvccRuntime runtime, String value) { return runtime.commit(pairedWrite(runtime, value)); }

    /** 核对同一快照中的两个分片值。 */
    private static void assertPair(WalMvccRuntime runtime, String value) {
        var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
        assertEquals(value, text(runtime.get(tx, "a", "k").orElseThrow()));
        assertEquals(value, text(runtime.get(tx, "b", "k").orElseThrow())); runtime.rollback(tx);
    }

    /** 放宽调度等待预算，避免把共享机器短时繁忙误判为协议失败。 */
    private static void waitLong(CountDownLatch latch) {
        try { if (!latch.await(30, TimeUnit.SECONDS)) { throw new AssertionError("fault barrier timed out"); } }
        catch (InterruptedException ex) { Thread.currentThread().interrupt(); throw new AssertionError(ex); }
    }
}
