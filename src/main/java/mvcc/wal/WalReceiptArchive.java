package mvcc.wal;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.zip.CRC32C;

/** 不可变提交凭据归档：精确 UUID 排序、布隆负向过滤和逐页校验。 */
final class WalReceiptArchive {
    private static final int MAGIC = 0x415A5243;
    private static final int HEADER = 64;
    private static final int PAGE = 4096;
    private static final int RECORD = 56;
    private static final int PER_PAGE = (PAGE - 4) / RECORD;
    private final Path path;
    private final Descriptor descriptor;
    private final WalBloom bloom;

    /** 归档目录只保存可信的元数据引用，正文按需读取，不重新装载全部事务凭据。 */
    record Descriptor(String fileName, long count, long minSequence, long maxSequence, long expiresAtMillis,
                      String metadataHash) {
        /** 拒绝路径穿越及不合理索引边界。 */
        Descriptor {
            if (fileName == null || !fileName.matches("receipts-[a-f0-9-]{36}\\.sst") || count < 1 || count > 1_000_000
                    || minSequence < 1 || minSequence > maxSequence || expiresAtMillis < 0
                    || metadataHash == null || !metadataHash.matches("[a-f0-9]{64}")) {
                throw new IllegalArgumentException("invalid receipt archive descriptor");
            }
        }
    }

    /** 加载有界过滤器并验证引擎身份及元数据摘要。 */
    private WalReceiptArchive(Path root, UUID engine, Descriptor expected) throws IOException {
        path = root.resolve(expected.fileName); descriptor = expected;
        try (var channel = FileChannel.open(path, StandardOpenOption.READ)) {
            byte[] header = WalFiles.read(channel, 0, HEADER); var in = ByteBuffer.wrap(header);
            if (in.getInt() != MAGIC || in.getInt() != 1 || !engine.equals(new UUID(in.getLong(), in.getLong()))
                    || in.getLong() != expected.count || in.getLong() != expected.minSequence
                    || in.getLong() != expected.maxSequence || in.getLong() != expected.expiresAtMillis) {
                throw new IOException("receipt archive identity mismatch");
            }
            int length = in.getInt(); int checksum = in.getInt();
            if (length < 128 || length > 2 * 1024 * 1024 || Integer.bitCount(length) != 1) { throw new IOException("invalid archive bloom size"); }
            byte[] bits = WalFiles.read(channel, HEADER, length);
            if (metadataCrc(header, bits) != checksum || !metadataHash(header, bits).equals(expected.metadataHash)) {
                throw new IOException("receipt archive metadata checksum mismatch");
            }
            bloom = new WalBloom(bits);
            if (channel.size() != diskBytes(expected.count, length)) { throw new IOException("receipt archive size mismatch"); }
        }
    }

    /** 打开一个已被当前检查点引用的归档。 */
    static WalReceiptArchive open(Path root, UUID engine, Descriptor descriptor) throws IOException {
        return new WalReceiptArchive(root, engine, descriptor);
    }

    /** 创建有序归档并同步正文；目录项同步由代际发布协议统一执行。 */
    static Descriptor create(Path root, UUID engine, List<WalReceipt> receipts, long expiresAtMillis) throws IOException {
        return create(root, engine, receipts, expiresAtMillis, WalLog.NORMAL_IO);
    }

    /** 在真实凭据文件写出位置注入故障，发布之前的部分归档永远不是恢复权威。 */
    static Descriptor create(Path root, UUID engine, List<WalReceipt> receipts, long expiresAtMillis, WalLog.IoHook io) throws IOException {
        if (receipts.isEmpty()) { throw new IllegalArgumentException("empty archive"); }
        var sorted = new ArrayList<>(receipts); sorted.sort((left, right) -> compare(left.transactionId(), right.transactionId()));
        var filter = WalBloom.allocate(sorted.size(), 2 * 1024 * 1024);
        long min = Long.MAX_VALUE; long max = 0; UUID previous = null;
        for (var receipt : sorted) {
            if (receipt.transactionId().equals(previous)) { throw new IOException("duplicate archived receipt"); }
            filter.add(receipt.transactionId()); previous = receipt.transactionId();
            min = Math.min(min, receipt.commitSequence()); max = Math.max(max, receipt.commitSequence());
        }
        byte[] bits = filter.bytes(); byte[] header = ByteBuffer.allocate(HEADER).putInt(MAGIC).putInt(1)
                .putLong(engine.getMostSignificantBits()).putLong(engine.getLeastSignificantBits()).putLong(sorted.size())
                .putLong(min).putLong(max).putLong(expiresAtMillis).putInt(bits.length).array();
        ByteBuffer.wrap(header).putInt(60, metadataCrc(header, bits));
        String name = "receipts-" + UUID.randomUUID() + ".sst";
        try (var channel = FileChannel.open(root.resolve(name), StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            WalFiles.write(channel, ByteBuffer.wrap(header), name, io); WalFiles.write(channel, ByteBuffer.wrap(bits), name, io);
            for (int first = 0; first < sorted.size(); first += PER_PAGE) {
                var page = ByteBuffer.allocate(PAGE);
                for (int i = first; i < Math.min(first + PER_PAGE, sorted.size()); i++) {
                    var receipt = sorted.get(i);
                    page.putLong(receipt.transactionId().getMostSignificantBits()).putLong(receipt.transactionId().getLeastSignificantBits())
                            .putLong(receipt.commitSequence()).put(HexFormat.of().parseHex(receipt.digest()));
                }
                page.putInt(PAGE - 4, WalFiles.crc(page.array(), 0, PAGE - 4)); page.position(0);
                WalFiles.write(channel, page, name, io);
            }
            io.beforeCheckpointForce(name); channel.force(true); io.afterCheckpointForce(name);
        }
        return new Descriptor(name, sorted.size(), min, max, expiresAtMillis, metadataHash(header, bits));
    }

    /** 布隆命中之后仍按 UUID 精确二分；索引页任何损坏都拒绝返回结果。 */
    Optional<WalReceipt> find(UUID id) throws IOException {
        if (!bloom.mightContain(id)) { return Optional.empty(); }
        try (var channel = FileChannel.open(path, StandardOpenOption.READ)) {
            long low = 0; long high = descriptor.count - 1; long cachedPage = -1; byte[] data = null;
            while (low <= high) {
                long middle = (low + high) >>> 1; long number = middle / PER_PAGE;
                if (number != cachedPage) { data = readPage(channel, number); cachedPage = number; }
                var receipt = decode(data, (int) (middle % PER_PAGE)); int order = compare(receipt.transactionId(), id);
                if (order == 0) { return Optional.of(receipt); }
                if (order < 0) { low = middle + 1; } else { high = middle - 1; }
            }
            return Optional.empty();
        }
    }

    /** 过滤器重新分配或有归档过期时，逐页验证并重建并集，不载入全部凭据对象。 */
    void forEach(Consumer<WalReceipt> consumer) throws IOException {
        try (var channel = FileChannel.open(path, StandardOpenOption.READ)) {
            for (long first = 0; first < descriptor.count; first += PER_PAGE) {
                byte[] page = readPage(channel, first / PER_PAGE);
                for (int i = 0; i < Math.min(PER_PAGE, descriptor.count - first); i++) { consumer.accept(decode(page, i)); }
            }
        }
    }

    /** 返回仅用于缓存容量记账的过滤器大小。 */
    int memoryBytes() { return bloom.byteSize(); }

    /** 返回归档的固定物理大小，用于写入前检查总预算。 */
    long diskBytes() throws IOException { return Files.size(path); }

    /** 在写入之前估算归档物理大小。 */
    static long estimatedBytes(int count) {
        return diskBytes(count, WalBloom.allocate(count, 2 * 1024 * 1024).bytes().length);
    }

    /** 计算页数和元数据长度，防止接受尾部缺失或额外记录。 */
    private static long diskBytes(long count, int bloomBytes) { return HEADER + bloomBytes + ((count + PER_PAGE - 1) / PER_PAGE) * PAGE; }

    /** 一次核对整页后才能使用其中的索引键，避免坏键导致二分走错分支。 */
    private byte[] readPage(FileChannel channel, long page) throws IOException {
        byte[] bytes = WalFiles.read(channel, HEADER + bloom.byteSize() + page * PAGE, PAGE);
        if (ByteBuffer.wrap(bytes).getInt(PAGE - 4) != WalFiles.crc(bytes, 0, PAGE - 4)) { throw new IOException("receipt index page checksum mismatch"); }
        return bytes;
    }

    /** 从已校验页解码一个凭据，提交序号必须属于本归档范围。 */
    private WalReceipt decode(byte[] page, int position) throws IOException {
        var in = ByteBuffer.wrap(page); in.position(position * RECORD);
        UUID id = new UUID(in.getLong(), in.getLong()); long sequence = in.getLong(); byte[] digest = new byte[32]; in.get(digest);
        if (sequence < descriptor.minSequence || sequence > descriptor.maxSequence) { throw new IOException("receipt sequence outside archive"); }
        return new WalReceipt(id, sequence, HexFormat.of().formatHex(digest));
    }

    /** UUID 按无符号字节顺序排序，创建和查询保持一致。 */
    private static int compare(UUID left, UUID right) {
        int high = Long.compareUnsigned(left.getMostSignificantBits(), right.getMostSignificantBits());
        return high != 0 ? high : Long.compareUnsigned(left.getLeastSignificantBits(), right.getLeastSignificantBits());
    }

    /** 头部校验同时覆盖过滤器，损坏不能让真实凭据变成假阴性。 */
    private static int metadataCrc(byte[] header, byte[] bits) {
        var crc = new CRC32C(); crc.update(header, 0, HEADER - 4); crc.update(bits); return (int) crc.getValue();
    }

    /** 将过滤器与元数据身份绑定到检查点整体摘要。 */
    private static String metadataHash(byte[] header, byte[] bits) {
        byte[] data = new byte[header.length + bits.length]; System.arraycopy(header, 0, data, 0, header.length);
        System.arraycopy(bits, 0, data, header.length, bits.length); return WalCodec.digest(data);
    }
}
