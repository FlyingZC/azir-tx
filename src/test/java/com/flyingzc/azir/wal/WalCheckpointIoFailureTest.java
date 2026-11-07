package com.flyingzc.azir.wal;

import com.flyingzc.azir.common.IsolationLevel;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import static com.flyingzc.azir.wal.WalMvccRuntimeTest.*;
import static org.junit.jupiter.api.Assertions.*;

/** 在真实新代文件的部分写出及刷盘前后注入设备错误，已有成功前缀必须保留。 */
class WalCheckpointIoFailureTest {
    @TempDir Path directory;

    /** 一个文件类型和刷盘边界的独立失败场景。 */
    private record Failure(String kind, String boundary, int repetition) {
        /** 在报告中保留精确失败身份。 */
        @Override public String toString() { return kind + "-" + boundary + "-" + repetition; }
    }

    /** 镜像、凭据、尾日志和指针分别覆盖部分写、刷盘前和刷盘后异常。 */
    static Stream<Failure> cases() {
        return Stream.of("IMAGE", "RECEIPTS", "TAIL", "POINTER").flatMap(kind ->
                Stream.of("PARTIAL", "BEFORE_FORCE", "AFTER_FORCE").flatMap(boundary ->
                        java.util.stream.IntStream.range(0, Integer.getInteger("azir.wal.checkpointIoRepeats", 1))
                                .mapToObj(n -> new Failure(kind, boundary, n))));
    }

    /** 第二个检查点失败后，不论第一代是否已截断，都必须恢复两笔已确认事务。 */
    @ParameterizedTest @MethodSource("cases")
    void fileErrorsNeverDeleteTheOnlyRecoverableState(Failure scenario) {
        long began = System.nanoTime();
        var armed = new AtomicBoolean();
        var io = new WalLog.IoHook() {
            /** 在真实文件写出 32 字节后停止，不能仅用抛异常替代部分写。 */
            @Override public int checkpointWriteLimit(String name, long position, int remaining) {
                return armed.get() && matches(scenario.kind, name) && scenario.boundary.equals("PARTIAL") ? Math.min(32, remaining) : remaining;
            }
            /** 部分内容已进入文件后注入磁盘错误。 */
            @Override public void afterCheckpointWrite(String name, long position) throws IOException { fail(name, "PARTIAL"); }
            /** 完整文件已经写出，但刷盘调用前失败。 */
            @Override public void beforeCheckpointForce(String name) throws IOException { fail(name, "BEFORE_FORCE"); }
            /** 刷盘成功但调用方收到异常，恢复仍须保留原先已确认前缀。 */
            @Override public void afterCheckpointForce(String name) throws IOException { fail(name, "AFTER_FORCE"); }
            /** 只在目标文件与目标阶段执行注入。 */
            private void fail(String name, String boundary) throws IOException {
                if (armed.get() && matches(scenario.kind, name) && scenario.boundary.equals(boundary)) { throw new IOException("injected checkpoint device failure"); }
            }
        };
        UUID first; UUID second;
        try (var runtime = WalMvccRuntime.openForTest(directory, Set.of("a"), Map.of(), WalOptions.defaults(), stage -> { }, io, System::nanoTime)) {
            first = writeValue(runtime, "one"); runtime.collectGarbage(10); runtime.checkpoint();
            second = writeValue(runtime, "two"); armed.set(true); assertThrows(WalMvccException.class, runtime::checkpoint);
            assertThrows(WalMvccException.class, () -> runtime.begin(IsolationLevel.REPEATABLE_READ));
        }
        try (var reopened = WalMvccRuntime.open(directory, Set.of("a"), WalOptions.defaults())) {
            assertEquals(1, reopened.findCommitted(first).orElseThrow().commitSequence());
            assertEquals(2, reopened.findCommitted(second).orElseThrow().commitSequence());
            var reader = reopened.begin(IsolationLevel.REPEATABLE_READ);
            assertEquals("two", text(reopened.get(reader, "a", "k").orElseThrow())); reopened.rollback(reader);
            assertTrue(reopened.checkpoint().created());
        }
        // 只降低本轮设备错误用例的执行频率，不调整用户其它进程。
        long minimum = java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(Long.getLong("azir.wal.checkpointIoIntervalMillis", 0));
        long remaining = minimum - (System.nanoTime() - began);
        if (remaining > 0) { java.util.concurrent.locks.LockSupport.parkNanos(remaining); }
    }

    /** 精确识别本实现生成的文件，不把 WAL 正常追加混入检查点失败场景。 */
    private static boolean matches(String kind, String name) {
        return switch (kind) {
            case "IMAGE" -> name.equals("snapshot.bin"); case "TAIL" -> name.equals("tail.wal");
            case "RECEIPTS" -> name.startsWith("receipts-"); case "POINTER" -> name.startsWith("CURRENT-");
            default -> false;
        };
    }

    /** 记录已经返回成功的事务身份。 */
    private static UUID writeValue(WalMvccRuntime runtime, String value) {
        var tx = runtime.begin(IsolationLevel.REPEATABLE_READ); runtime.put(tx, "a", "k", bytes(value)); runtime.commit(tx); return tx.id();
    }
}
