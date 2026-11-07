package com.flyingzc.azir.wal;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.zip.CRC32C;
import static org.junit.jupiter.api.Assertions.*;

/** 日志层持久性、身份、校验与截断尾部的独立验收。 */
class WalLogTest {
    @TempDir Path directory;

    /** 多笔事务共享一次刷盘，关闭重开后保持身份、写集和摘要。 */
    @Test void batchRoundTripRetainsIdentityAndReceipts() throws Exception {
        var first = record(1);
        var second = record(2);
        UUID engine;
        try (var log = new WalLog(directory, Set.of("a"), 1_000_000)) {
            engine = log.engineId();
            log.append(List.of(first, second));
            assertEquals(1, log.forces());
        }
        try (var log = new WalLog(directory, Set.of("a"), 1_000_000)) {
            assertEquals(engine, log.engineId());
            assertEquals(2, log.lastSequence());
            assertEquals(first.digest(), log.receipt(first.transactionId()).orElseThrow().digest());
            assertEquals(second.transactionId(), log.read(2, 1).getFirst().transactionId());
            assertArrayEquals(second.mutations().getFirst().value(), log.read(2, 1).getFirst().mutations().getFirst().value());
        }
    }

    /** 只修复未完成尾部，之后仍可连续追加新事务。 */
    @Test void incompleteTailIsDiscardedWithoutLosingCompletePrefix() throws Exception {
        try (var log = new WalLog(directory, Set.of("a"), 1_000_000)) { log.append(List.of(record(1))); }
        long completeSize = Files.size(file());
        byte[] data = WalCodec.encode(record(2));
        ByteBuffer frame = ByteBuffer.allocate(16 + data.length);
        frame.putInt(0x415A574C).putInt(data.length).putInt(crc(data));
        frame.putInt(crc(Arrays.copyOf(frame.array(), 12))).put(data);
        Files.write(file(), Arrays.copyOf(frame.array(), 20), StandardOpenOption.APPEND);
        try (var log = new WalLog(directory, Set.of("a"), 1_000_000)) {
            assertEquals(1, log.lastSequence());
            assertEquals(completeSize, Files.size(file()));
            log.append(List.of(record(2)));
            assertEquals(2, log.lastSequence());
        }
    }

    /** 完整记录内容损坏必须失败关闭，不可当作可丢弃尾部。 */
    @Test void corruptPayloadIsRejectedWithoutTruncation() throws Exception {
        try (var log = new WalLog(directory, Set.of("a"), 1_000_000)) { log.append(List.of(record(1))); }
        byte[] data = Files.readAllBytes(file());
        data[data.length - 1] ^= 1;
        Files.write(file(), data);
        assertThrows(IOException.class, () -> new WalLog(directory, Set.of("a"), 1_000_000));
        assertEquals(data.length, Files.size(file()));
    }

    /** 长度字段损坏不能伪装成未完成尾部并导致已提交数据被截断。 */
    @Test void corruptLengthHeaderIsRejectedWithoutTruncation() throws Exception {
        try (var log = new WalLog(directory, Set.of("a"), 1_000_000)) { log.append(List.of(record(1))); }
        byte[] data = Files.readAllBytes(file());
        ByteBuffer.wrap(data).putInt(64, 1_000_000);
        Files.write(file(), data);
        assertThrows(IOException.class, () -> new WalLog(directory, Set.of("a"), 1_000_000));
        assertEquals(data.length, Files.size(file()));
    }

    /** 文件锁阻止同目录双写，路由配置变化也必须拒绝启动。 */
    @Test void exclusiveWriterAndRouteBindingAreEnforced() throws Exception {
        try (var log = new WalLog(directory, Set.of("a"), 1_000_000)) {
            assertThrows(Exception.class, () -> new WalLog(directory, Set.of("a"), 1_000_000));
        }
        assertThrows(IOException.class, () -> new WalLog(directory, Set.of("b"), 1_000_000));
        try (var ignored = new WalLog(directory, Set.of("a"), 1_000_000)) { assertTrue(Files.exists(file())); }
    }

    /** 不连续追加和重复事务号必须在改变文件之前拒绝。 */
    @Test void invalidSequenceCannotChangeTheLog() throws Exception {
        try (var log = new WalLog(directory, Set.of("a"), 1_000_000)) {
            assertThrows(IOException.class, () -> log.append(List.of(record(2))));
            assertEquals(60, log.bytes());
        }
    }

    /** 同批重复事务号必须在刷盘前拒绝，不能写出无法恢复的日志。 */
    @Test void duplicateTransactionIdsAreRejectedBeforeAppend() throws Exception {
        try (var log = new WalLog(directory, Set.of("a"), 1_000_000)) {
            var first = record(1);
            var second = new WalCommit(first.transactionId(), 2, 0, first.mutations());
            assertThrows(IOException.class, () -> log.append(List.of(first, second)));
            assertEquals(60, log.bytes());
        }
    }

    /** 返回当前测试日志路径。 */
    private Path file() { return directory.resolve("transactions.wal"); }

    /** 创建独立事务日志项。 */
    private static WalCommit record(long sequence) {
        return new WalCommit(UUID.randomUUID(), sequence, 0,
                List.of(new WalMutation("a", "k", new byte[]{(byte) sequence})));
    }

    /** 为模拟写入中断构造正确帧校验。 */
    private static int crc(byte[] bytes) { var crc = new CRC32C(); crc.update(bytes); return (int) crc.getValue(); }
}
