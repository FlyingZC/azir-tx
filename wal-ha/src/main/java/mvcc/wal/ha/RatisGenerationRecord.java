package mvcc.wal.ha;

import mvcc.wal.WalReplicaIdentity;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.UUID;
import java.util.zip.CRC32C;

/** 安装操作与活动指针共用的有界持久记录，固定接收身份、代次和快照摘要。 */
record RatisGenerationRecord(WalReplicaIdentity identity, UUID generation, long term, long index, long sequence, String hash) {
    /** 初始代次没有快照位置，安装代次必须携带完整文件摘要。 */
    RatisGenerationRecord {
        java.util.Objects.requireNonNull(identity); java.util.Objects.requireNonNull(generation); java.util.Objects.requireNonNull(hash);
        if (sequence < 0 || (term == -1 ? index != -1 || sequence != 0 || !hash.isEmpty() : term < 1 || index < 0 || !hash.matches("[a-f0-9]{64}"))) {
            throw new IllegalArgumentException("invalid generation record");
        }
    }

    /** 序列化包括接收身份，不能把其他节点活动指针接到当前投票身份。 */
    byte[] encode() throws IOException {
        var buffer = new ByteArrayOutputStream(); var out = new DataOutputStream(buffer); out.writeInt(0x415A474E); out.writeInt(1);
        uuid(out, identity.engineId()); uuid(out, identity.groupId()); out.writeUTF(identity.nodeId()); uuid(out, generation);
        out.writeLong(term); out.writeLong(index); out.writeLong(sequence); out.writeUTF(hash); out.flush(); out.writeInt(crc(buffer.toByteArray())); out.flush(); return buffer.toByteArray();
    }

    /** 对长度和 CRC 先检查，再解析不可变身份；损坏不能退回旧代次继续服务。 */
    static RatisGenerationRecord read(Path path) throws IOException {
        if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS) || Files.size(path) < 80 || Files.size(path) > 512) { throw new IOException("generation record missing or exceeds budget"); }
        return decode(Files.readAllBytes(path));
    }

    /** 嵌入式所有权记录使用相同格式，不通过临时文件解析。 */
    static RatisGenerationRecord decode(byte[] bytes) throws IOException {
        if (bytes.length < 80 || bytes.length > 512) { throw new IOException("generation record length invalid"); }
        if (ByteBuffer.wrap(bytes).getInt(bytes.length - 4) != crc(java.util.Arrays.copyOf(bytes, bytes.length - 4))) { throw new IOException("generation checksum mismatch"); }
        try {
            var in = new DataInputStream(new ByteArrayInputStream(bytes, 0, bytes.length - 4)); if (in.readInt() != 0x415A474E || in.readInt() != 1) { throw new IOException("unsupported generation record"); }
            var identity = new WalReplicaIdentity(uuid(in), uuid(in), in.readUTF()); var record = new RatisGenerationRecord(identity, uuid(in), in.readLong(), in.readLong(), in.readLong(), in.readUTF());
            if (in.available() != 0) { throw new IOException("trailing generation data"); } return record;
        } catch (IllegalArgumentException failure) { throw new IOException("invalid generation record", failure); }
    }

    /** 新记录先写私有暂存并同步，原子替换后再同步目录。 */
    void publish(Path target) throws IOException { publish(target, RatisRecordFile.REAL); }
    /** 正式与故障注入使用相同编码，重试不回滚已经完整替换的新记录。 */
    void publish(Path target, RatisRecordFile.Io io) throws IOException { RatisRecordFile.publish(target, encode(), ".record-", 512, io); }

    /** 安装记录删除结果不确定时可原样重试，并再次同步父目录。 */
    static void remove(Path target) throws IOException { remove(target, RatisRecordFile.REAL); }
    /** 已不存在也必须确认删除的持久性，不通过异常短路省略同步。 */
    static void remove(Path target, RatisRecordFile.Io io) throws IOException { RatisRecordFile.remove(target, io); }

    /** 写出必须有实际进展。 */
    static void write(FileChannel channel, ByteBuffer bytes) throws IOException { while (bytes.hasRemaining()) { if (channel.write(bytes) < 1) { throw new IOException("generation file write made no progress"); } } }

    /** 目录项持久化不能被普通 flush 替代。 */
    static void forceDirectory(Path directory) throws IOException { try (var channel = FileChannel.open(directory, StandardOpenOption.READ)) { channel.force(true); } }

    /** 固定长度 UUID 编码。 */
    private static void uuid(DataOutputStream out, UUID value) throws IOException { out.writeLong(value.getMostSignificantBits()); out.writeLong(value.getLeastSignificantBits()); }

    /** 固定长度 UUID 解码。 */
    private static UUID uuid(DataInputStream in) throws IOException { return new UUID(in.readLong(), in.readLong()); }

    /** 指针字段全部参与独立块校验。 */
    private static int crc(byte[] bytes) { var crc = new CRC32C(); crc.update(bytes, 0, bytes.length); return (int) crc.getValue(); }
}
