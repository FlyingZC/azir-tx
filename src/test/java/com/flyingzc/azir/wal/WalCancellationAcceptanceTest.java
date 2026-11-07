package com.flyingzc.azir.wal;

import com.flyingzc.azir.common.IsolationLevel;
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
import static org.junit.jupiter.api.Assertions.*;

/** 验证线程取消不会关闭共享 WAL 写通道或提前释放写者独占锁。 */
class WalCancellationAcceptanceTest {
    @TempDir Path directory;

    /** 中断日志读者只关闭独立读通道，后续追加和独占锁仍有效。 */
    @Test void interruptedLogReaderDoesNotCloseWriterOrReleaseItsLock() throws Exception {
        try (var log = new WalLog(directory, Set.of("a"), 1_000_000)) {
            log.append(List.of(record(1)));
            var failure = new AtomicReference<Throwable>();
            Thread reader = Thread.ofVirtual().start(() -> {
                Thread.currentThread().interrupt();
                try { log.read(1, 1); fail("interrupted reader should fail"); }
                catch (IOException expected) { }
                catch (Throwable unexpected) { failure.set(unexpected); }
            });
            reader.join(30_000); assertFalse(reader.isAlive()); assertNull(failure.get());
            assertThrows(Exception.class, () -> new WalLog(directory, Set.of("a"), 1_000_000));
            assertEquals(0, otherProcessLockAttempt());
            log.append(List.of(record(2))); assertEquals(2, log.lastSequence());
            assertEquals(log.bytes(), java.nio.file.Files.size(directory.resolve("transactions.wal")));
        }
        try (var recovered = new WalLog(directory, Set.of("a"), 1_000_000)) { assertEquals(2, recovered.lastSequence()); }
        assertEquals(7, otherProcessLockAttempt());
    }

    /** 被中断的统计调用者不能通过 size 系统调用关闭共享写通道。 */
    @Test void interruptedStatsCallerCannotPoisonTheRuntime() throws Exception {
        try (var runtime = WalMvccRuntime.open(directory, Set.of("a"), WalOptions.defaults())) {
            write(runtime, "a", "k", "one");
            var failure = new AtomicReference<Throwable>();
            Thread reader = Thread.ofVirtual().start(() -> {
                Thread.currentThread().interrupt();
                try { assertTrue(runtime.stats().walBytes() > 60); }
                catch (Throwable unexpected) { failure.set(unexpected); }
            });
            reader.join(30_000); assertFalse(reader.isAlive()); assertNull(failure.get());
            assertEquals(2, write(runtime, "a", "k", "two").commitSequence());
        }
    }

    /** 正常关闭等待在途物化，不中断它的 JDBC 或日志读。 */
    @Test void closeDoesNotInterruptAnInFlightMaterializer() throws Exception {
        var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
        var closing = new CountDownLatch(1); var interrupted = new AtomicBoolean();
        var store = new TestMaterializedStore();
        store.onApply = () -> {
            entered.countDown();
            try { if (!release.await(30, TimeUnit.SECONDS)) { throw new AssertionError("materializer barrier timeout"); } }
            catch (InterruptedException failure) { interrupted.set(true); throw new IllegalStateException(failure); }
        };
        var runtime = WalMvccRuntime.openForTest(directory, Set.of("a"), Map.of("a", store), WalOptions.defaults(),
                stage -> { if (stage == WalMvccRuntime.Stage.BEFORE_MATERIALIZER_JOIN) { closing.countDown(); } });
        try (var tasks = Executors.newVirtualThreadPerTaskExecutor()) {
            write(runtime, "a", "k", "committed"); assertTrue(entered.await(30, TimeUnit.SECONDS));
            var stopped = tasks.submit(runtime::close);
            try { assertTrue(closing.await(30, TimeUnit.SECONDS)); assertFalse(interrupted.get()); }
            finally { release.countDown(); }
            stopped.get(30, TimeUnit.SECONDS); assertFalse(interrupted.get());
        } finally { release.countDown(); runtime.close(); }
        assertEquals(1, store.checkpoint().sequence());
    }

    /** 已关闭旧对象的重复关闭不能删除新写者的本进程注册或系统锁。 */
    @Test void repeatedOldCloseCannotReleaseANewWritersLock() throws Exception {
        var old = new WalLog(directory, Set.of("a"), 1_000_000); old.close();
        try (var current = new WalLog(directory, Set.of("a"), 1_000_000)) {
            old.close();
            assertThrows(Exception.class, () -> new WalLog(directory, Set.of("a"), 1_000_000));
            assertEquals(0, otherProcessLockAttempt()); current.append(List.of(record(1)));
        }
    }

    /** 创建一项简单的持久记录。 */
    private static WalCommit record(long sequence) {
        return new WalCommit(UUID.randomUUID(), sequence, 0, List.of(new WalMutation("a", "k", bytes("v" + sequence))));
    }

    /** 通过真实子 JVM 核对独占锁是否仍由父进程持有。 */
    private int otherProcessLockAttempt() throws Exception {
        String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        Process child = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp", classpath, WalExclusiveWriterProcess.class.getName(), directory.toString())
                .redirectErrorStream(true).redirectOutput(directory.resolve("lock-probe.log").toFile()).start();
        assertTrue(child.waitFor(30, TimeUnit.SECONDS)); return child.exitValue();
    }
}
