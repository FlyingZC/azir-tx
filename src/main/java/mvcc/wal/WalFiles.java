package mvcc.wal;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.zip.CRC32C;

/** 检查点文件的有界读写与目录同步，不通过非原子覆盖替代发布协议。 */
final class WalFiles {
    /** 禁止实例化文件协议工具。 */
    private WalFiles() { }

    /** 新文件必须完整写出并同步，存在同名文件时拒绝覆盖。 */
    static void writeNew(Path path, byte[] data) throws IOException {
        writeNew(path, data, WalLog.NORMAL_IO);
    }

    /** 在真实写出与刷盘位置注入故障，默认入口不增加额外文件行为。 */
    static void writeNew(Path path, byte[] data, WalLog.IoHook io) throws IOException {
        try (var channel = FileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            String name = path.getFileName().toString(); write(channel, ByteBuffer.wrap(data), name, io);
            io.beforeCheckpointForce(name); channel.force(true); io.afterCheckpointForce(name);
        }
    }

    /** 写完整个缓冲区，磁盘无进展视为错误。 */
    static void write(FileChannel channel, ByteBuffer data) throws IOException {
        while (data.hasRemaining()) { if (channel.write(data) <= 0) { throw new IOException("file write made no progress"); } }
    }

    /** 对单个检查点文件保留真实偏移和分块写出，注入不能用事后改文件替代。 */
    static void write(FileChannel channel, ByteBuffer data, String name, WalLog.IoHook io) throws IOException {
        while (data.hasRemaining()) {
            int permitted = io.checkpointWriteLimit(name, channel.position(), data.remaining());
            if (permitted < 1 || permitted > data.remaining()) { throw new IOException("invalid checkpoint write limit"); }
            int limit = data.limit(); data.limit(data.position() + permitted);
            try { if (channel.write(data) <= 0) { throw new IOException("checkpoint write made no progress"); } }
            finally { data.limit(limit); }
            io.afterCheckpointWrite(name, channel.position());
        }
    }

    /** 从指定偏移读取完整块，截断不能伪装成合法空数据。 */
    static byte[] read(FileChannel channel, long offset, int length) throws IOException {
        var data = ByteBuffer.allocate(length);
        while (data.hasRemaining()) {
            if (channel.read(data, offset + data.position()) <= 0) { throw new IOException("truncated checkpoint file"); }
        }
        return data.array();
    }

    /** 在分配之前检查文件大小，防止畸形文件导致无界内存申请。 */
    static byte[] readBounded(Path path, long maximum) throws IOException {
        long size = Files.size(path);
        if (size < 1 || size > maximum) { throw new IOException("checkpoint file size exceeds budget"); }
        return Files.readAllBytes(path);
    }

    /** 同步目录项，只有原子改名与目录同步完成后才允许删除旧代。 */
    static void forceDirectory(Path directory) throws IOException {
        try (var channel = FileChannel.open(directory, StandardOpenOption.READ)) { channel.force(true); }
    }

    /** 计算独立数据块校验，保护索引键及载荷，不仅保护返回值。 */
    static int crc(byte[] data) { return crc(data, 0, data.length); }

    /** 对指定区间计算校验，帧校验字段自身不参与计算。 */
    static int crc(byte[] data, int offset, int length) {
        var crc = new CRC32C(); crc.update(data, offset, length); return (int) crc.getValue();
    }
}
