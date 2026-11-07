package com.flyingzc.azir.wal;

import org.junit.jupiter.api.Test;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;

/** 备份格式的实际读写、截断和身份验证，不能用完整文件摘要代替内容语义检查。 */
class WalBackupDataTest {
    /** Unicode、空键和删除版本完整往返，消费者只需持有一行。 */
    @Test void streamsValuesAndTombstonesInBinaryKeyOrder() throws Exception {
        var header = header(); var bytes = new ByteArrayOutputStream(); var writer = new WalBackupData.Writer(bytes, header);
        List<String> keys = List.of("", "a\u0000b", "中", "x\uE000", "x😀").stream().sorted(com.flyingzc.azir.storage.Utf8KeyOrder::compare).toList();
        for (int i = 0; i < keys.size(); i++) { writer.accept(new WalBackupRow(keys.get(i), new WalValue(i + 1, i == 2 ? null : new byte[]{(byte) i}))); }
        assertEquals(keys.size(), writer.finish()); var rows = new ArrayList<WalBackupRow>();
        assertEquals(keys.size(), WalBackupData.read(new ByteArrayInputStream(bytes.toByteArray()), header, rows::add));
        assertEquals(keys, rows.stream().map(WalBackupRow::key).toList()); assertTrue(rows.get(2).version().deleted());
        for (int i = 0; i < rows.size(); i++) { if (i != 2) { assertArrayEquals(new byte[]{(byte) i}, rows.get(i).version().value()); } }
    }

    /** 每一种不完整前缀都必须拒绝，即使它刚好包含若干完整行。 */
    @Test void everyTruncatedPrefixIsRejected() throws Exception {
        byte[] valid = encoded();
        for (int size = 0; size < valid.length; size++) {
            byte[] prefix = java.util.Arrays.copyOf(valid, size);
            assertThrows(IOException.class, () -> WalBackupData.read(new ByteArrayInputStream(prefix), header(), row -> { }));
        }
    }

    /** 后续字节、错误行数和错误引擎身份都不能被当成同一份完整备份。 */
    @Test void rejectsTrailingDataWrongFooterAndIdentity() throws Exception {
        byte[] bytes = encoded(); byte[] trailing = java.util.Arrays.copyOf(bytes, bytes.length + 1);
        assertThrows(IOException.class, () -> WalBackupData.read(new ByteArrayInputStream(trailing), header(), row -> { }));
        bytes[bytes.length - 1] ^= 1;
        assertThrows(IOException.class, () -> WalBackupData.read(new ByteArrayInputStream(bytes), header(), row -> { }));
        assertThrows(IOException.class, () -> WalBackupData.read(new ByteArrayInputStream(encoded()),
                new WalBackupData.Header(UUID.randomUUID(), "a", header().boundary()), row -> { }));
    }

    /** 重复或倒序键、越界版本和重复结束均必须在编码时拒绝。 */
    @Test void writerRejectsInvalidOrderAndBoundary() throws Exception {
        var writer = new WalBackupData.Writer(new ByteArrayOutputStream(), header());
        writer.accept(new WalBackupRow("b", new WalValue(1, null)));
        assertThrows(IOException.class, () -> writer.accept(new WalBackupRow("b", new WalValue(2, null))));
        assertThrows(IOException.class, () -> writer.accept(new WalBackupRow("a", new WalValue(2, null))));
        assertThrows(IOException.class, () -> writer.accept(new WalBackupRow("c", new WalValue(11, null))));
        writer.finish(); assertThrows(IOException.class, writer::finish);
        assertThrows(IllegalArgumentException.class, () -> new WalBackupRow("k", new WalValue(0, null)));
    }

    /** 提供可稳定复现且不同测试调用一致的头部。 */
    private static WalBackupData.Header header() { return new WalBackupData.Header(new UUID(1, 2), "a", new WalCheckpoint(10, "0".repeat(64))); }

    /** 生成真实的完整格式样本，供逐字节截断和损坏测试使用。 */
    private static byte[] encoded() throws IOException {
        var buffer = new ByteArrayOutputStream(); var writer = new WalBackupData.Writer(buffer, header());
        writer.accept(new WalBackupRow("key", new WalValue(1, new byte[]{1, 2, 3}))); writer.finish(); return buffer.toByteArray();
    }
}
