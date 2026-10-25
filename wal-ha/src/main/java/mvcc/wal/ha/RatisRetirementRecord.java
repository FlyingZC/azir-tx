package mvcc.wal.ha;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.UUID;
import java.util.zip.CRC32C;

/** 退休进度日志保存所有权记录和后端游标，完成前不能遗失已开始的删除操作。 */
record RatisRetirementRecord(RatisGenerationRecord target, String cursor, boolean backendDone) {
    /** 不允许畸形游标导致无界元数据分配。 */
    RatisRetirementRecord { if (cursor == null || cursor.length() > 512) { throw new IllegalArgumentException("invalid retirement cursor"); } }
    /** 原子发布与目录同步，不直接覆盖正在恢复的日志。 */
    void publish(Path destination) throws IOException { publish(destination, RatisRecordFile.REAL); }
    /** 退休进度与活动指针共用真实替换协议，各自保留原格式和大小预算。 */
    void publish(Path destination, RatisRecordFile.Io io) throws IOException { RatisRecordFile.publish(destination, encode(), ".retirement-", 2048, io); }
    /** 编码全部进度与所有权字段，不改变旧日志格式。 */
    private byte[] encode() throws IOException {
        var buffer = new ByteArrayOutputStream(); var out = new DataOutputStream(buffer); byte[] owner = target.encode(); out.writeInt(0x415A5252); out.writeInt(1); out.writeInt(owner.length); out.write(owner); out.writeUTF(cursor); out.writeBoolean(backendDone); out.flush();
        out.writeInt(crc(buffer.toByteArray())); out.flush();
        return buffer.toByteArray();
    }
    /** CRC 及长度检查失败必须拒绝，不能重新选择另一代次来掩盖部分删除。 */
    static RatisRetirementRecord read(Path path) throws IOException {
        if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS) || Files.size(path) < 100 || Files.size(path) > 2048) { throw new IOException("retirement record missing or exceeds budget"); }
        byte[] bytes = Files.readAllBytes(path); if (ByteBuffer.wrap(bytes).getInt(bytes.length - 4) != crc(java.util.Arrays.copyOf(bytes, bytes.length - 4))) { throw new IOException("retirement checksum mismatch"); }
        var in = new DataInputStream(new ByteArrayInputStream(bytes, 0, bytes.length - 4)); if (in.readInt() != 0x415A5252 || in.readInt() != 1) { throw new IOException("unsupported retirement record"); }
        int length = in.readInt(); if (length < 80 || length > 512 || length > in.available()) { throw new IOException("retirement owner length invalid"); }
        byte[] owner = in.readNBytes(length); var target = RatisGenerationRecord.decode(owner); var result = new RatisRetirementRecord(target, in.readUTF(), in.readBoolean()); if (in.available() != 0) { throw new IOException("trailing retirement record"); } return result;
    }
    /** 整个进度记录参与校验。 */
    private static int crc(byte[] bytes) { var crc = new CRC32C(); crc.update(bytes, 0, bytes.length); return (int) crc.getValue(); }
}
