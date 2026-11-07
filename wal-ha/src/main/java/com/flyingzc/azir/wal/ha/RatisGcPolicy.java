package com.flyingzc.azir.wal.ha;

import com.flyingzc.azir.wal.WalReplicaIdentity;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;
import java.util.zip.CRC32C;

/** 版本回收策略绑定本机身份，重启自动恢复，缺失健康策略时拒绝不完整装配。 */
record RatisGcPolicy(WalReplicaIdentity identity, RatisGcOptions options) {
    /** 固定配置不包含数据库凭据或路径。 */
    RatisGcPolicy { Objects.requireNonNull(identity); Objects.requireNonNull(options); }
    /** 只读取有界、已校验且属于本节点的策略。 */
    static RatisGcPolicy load(Path root, WalReplicaIdentity owner) throws IOException {
        Path file = root.resolve("GC_POLICY"); if (!Files.exists(file, LinkOption.NOFOLLOW_LINKS)) { return null; }
        if (!Files.isRegularFile(file, LinkOption.NOFOLLOW_LINKS) || Files.size(file) > 512) { throw new IOException("version GC policy missing or exceeds budget"); }
        byte[] bytes = Files.readAllBytes(file); if (bytes.length < 79 || ByteBuffer.wrap(bytes).getInt(bytes.length - 4) != crc(Arrays.copyOf(bytes, bytes.length - 4))) { throw new IOException("version GC policy checksum mismatch"); }
        try {
            var in = new DataInputStream(new ByteArrayInputStream(bytes, 0, bytes.length - 4)); if (in.readInt() != 0x415A4743 || in.readInt() != 1) { throw new IOException("unsupported version GC policy"); }
            var identity = new WalReplicaIdentity(new UUID(in.readLong(), in.readLong()), new UUID(in.readLong(), in.readLong()), in.readUTF());
            var options = new RatisGcOptions(in.readLong(), in.readInt(), in.readInt(), in.readLong(), in.readLong());
            if (!identity.equals(owner) || in.available() != 0) { throw new IOException("version GC policy owner mismatch"); } return new RatisGcPolicy(identity, options);
        } catch (IllegalArgumentException malformed) { throw new IOException("invalid version GC policy", malformed); }
    }
    /** 节点独占目录锁内持久发布，既有正确策略也重新确认文件及目录。 */
    void publish(Path root) throws IOException { publish(root, RatisPolicyFile.REAL); }
    /** 包内故障注入与正式路径共用同一编码和真实发布协议。 */
    void publish(Path root, RatisPolicyFile.Io io) throws IOException {
        var previous = load(root, identity);
        if (previous != null && !previous.equals(this)) { throw new IOException("policy already configured differently"); }
        RatisPolicyFile.publish(root, RatisPolicyFile.Kind.GC, encode(), io);
    }
    /** 保留既有持久格式，不通过更改版本号逃避旧策略的正确重试。 */
    private byte[] encode() throws IOException {
        var bytes = new ByteArrayOutputStream(); var out = new DataOutputStream(bytes); out.writeInt(0x415A4743); out.writeInt(1); out.writeLong(identity.engineId().getMostSignificantBits()); out.writeLong(identity.engineId().getLeastSignificantBits()); out.writeLong(identity.groupId().getMostSignificantBits()); out.writeLong(identity.groupId().getLeastSignificantBits()); out.writeUTF(identity.nodeId());
        out.writeLong(options.intervalMillis()); out.writeInt(options.keyLimit()); out.writeInt(options.rowLimit()); out.writeLong(options.workMillis()); out.writeLong(options.staleAfterMillis()); out.flush(); out.writeInt(crc(bytes.toByteArray())); out.flush();
        return bytes.toByteArray();
    }
    /** 全部身份与配置参与校验。 */
    private static int crc(byte[] bytes) { var checksum = new CRC32C(); checksum.update(bytes); return (int) checksum.getValue(); }
}
