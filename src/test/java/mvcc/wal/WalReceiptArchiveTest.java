package mvcc.wal;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;

/** 独立核对磁盘凭据索引、负向过滤和损坏检测，不能把概率命中作为提交证据。 */
class WalReceiptArchiveTest {
    @TempDir Path directory;

    /** 跨多个页逐个核对精确 UUID、序号和摘要，另核对不存在的 UUID。 */
    @Test void sortedPagesReturnOnlyExactReceipts() throws Exception {
        UUID engine = UUID.randomUUID(); var receipts = records(250);
        var descriptor = WalReceiptArchive.create(directory, engine, receipts, Long.MAX_VALUE);
        var archive = WalReceiptArchive.open(directory, engine, descriptor);
        for (var receipt : receipts) { assertEquals(receipt, archive.find(receipt.transactionId()).orElseThrow()); }
        assertTrue(archive.find(UUID.randomUUID()).isEmpty());
        var scanned = new ArrayList<WalReceipt>(); archive.forEach(scanned::add);
        assertEquals(Set.copyOf(receipts), Set.copyOf(scanned));
    }

    /** 过滤器损坏必须拒绝打开，不能产生查询假阴性。 */
    @Test void damagedBloomFailsBeforeLookup() throws Exception {
        UUID engine = UUID.randomUUID(); var descriptor = WalReceiptArchive.create(directory, engine, records(20), Long.MAX_VALUE);
        Path file = directory.resolve(descriptor.fileName()); byte[] data = Files.readAllBytes(file); data[70] ^= 1; Files.write(file, data);
        assertThrows(IOException.class, () -> WalReceiptArchive.open(directory, engine, descriptor));
    }

    /** 索引页中的键或值损坏均被整页校验拒绝，不能因二分走错而返回缺失。 */
    @Test void damagedIndexPageFailsBeforeUsingKeys() throws Exception {
        UUID engine = UUID.randomUUID(); var descriptor = WalReceiptArchive.create(directory, engine, records(20), Long.MAX_VALUE);
        Path file = directory.resolve(descriptor.fileName()); byte[] data = Files.readAllBytes(file); data[data.length - 100] ^= 1; Files.write(file, data);
        var archive = WalReceiptArchive.open(directory, engine, descriptor);
        assertThrows(IOException.class, () -> archive.forEach(receipt -> { }));
    }

    /** 不同引擎、截断或尾部多出记录均不能被当成正常归档。 */
    @Test void identityAndExactSizeAreRequired() throws Exception {
        UUID engine = UUID.randomUUID(); var descriptor = WalReceiptArchive.create(directory, engine, records(2), Long.MAX_VALUE);
        assertThrows(IOException.class, () -> WalReceiptArchive.open(directory, UUID.randomUUID(), descriptor));
        Files.write(directory.resolve(descriptor.fileName()), new byte[]{1}, java.nio.file.StandardOpenOption.APPEND);
        assertThrows(IOException.class, () -> WalReceiptArchive.open(directory, engine, descriptor));
    }

    /** 任意分配规模的过滤器都不能遗漏已经标记的 UUID。 */
    @Test void bloomNeverRejectsInsertedIds() {
        for (int count : new int[]{1, 50, 1000, 10_000}) {
            var filter = WalBloom.allocate(count, 16 * 1024 * 1024); var receipts = records(count);
            receipts.forEach(receipt -> filter.add(receipt.transactionId()));
            var restored = new WalBloom(filter.bytes());
            receipts.forEach(receipt -> assertTrue(restored.mightContain(receipt.transactionId())));
        }
    }

    /** 创建无外部依赖的稳定格式凭据数据。 */
    private static List<WalReceipt> records(int count) {
        var result = new ArrayList<WalReceipt>();
        for (int i = 1; i <= count; i++) { result.add(new WalReceipt(UUID.randomUUID(), i, WalCodec.digest(new byte[]{(byte) i}))); }
        return result;
    }
}
