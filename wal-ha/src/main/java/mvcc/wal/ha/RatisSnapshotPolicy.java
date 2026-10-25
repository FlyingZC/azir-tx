package mvcc.wal.ha;

import mvcc.wal.WalReplicaIdentity;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;
import java.util.zip.CRC32C;

/** 自动镜像策略绑定本机身份，重启自动恢复，缺失健康策略时拒绝不完整装配。 */
record RatisSnapshotPolicy(WalReplicaIdentity identity, RatisSnapshotOptions options) {
    /** 固定配置不包含数据库凭据或路径。 */
    RatisSnapshotPolicy { Objects.requireNonNull(identity); Objects.requireNonNull(options); }
    /** 只读取有界、已校验且属于本节点的策略。 */
    static RatisSnapshotPolicy load(Path root, WalReplicaIdentity owner) throws IOException {
        Path file = root.resolve("SNAPSHOT_POLICY"); if (!Files.exists(file, LinkOption.NOFOLLOW_LINKS)) { return null; }
        if (!Files.isRegularFile(file, LinkOption.NOFOLLOW_LINKS) || Files.size(file) > 512) { throw new IOException("snapshot policy missing or exceeds budget"); }
        byte[] bytes = Files.readAllBytes(file); if (bytes.length < 87 || ByteBuffer.wrap(bytes).getInt(bytes.length - 4) != crc(Arrays.copyOf(bytes, bytes.length - 4))) { throw new IOException("snapshot policy checksum mismatch"); }
        try {
            var in = new DataInputStream(new ByteArrayInputStream(bytes, 0, bytes.length - 4)); if (in.readInt() != 0x415A5350 || in.readInt() != 1) { throw new IOException("unsupported snapshot policy"); }
            var identity = new WalReplicaIdentity(new UUID(in.readLong(), in.readLong()), new UUID(in.readLong(), in.readLong()), in.readUTF());
            var options = new RatisSnapshotOptions(in.readLong(), in.readLong(), in.readLong(), in.readLong(), in.readLong());
            if (!identity.equals(owner) || in.available() != 0) { throw new IOException("snapshot policy owner mismatch"); } return new RatisSnapshotPolicy(identity, options);
        } catch (IllegalArgumentException malformed) { throw new IOException("invalid snapshot policy", malformed); }
    }
    /** 节点独占目录锁内持久发布，既有正确策略也重新确认文件及目录。 */
    void publish(Path root) throws IOException { publish(root, RatisPolicyFile.REAL); }
    /** 包内故障注入与正式路径共用同一编码和真实发布协议。 */
    void publish(Path root, RatisPolicyFile.Io io) throws IOException {
        var previous = load(root, identity);
        if (previous != null && !previous.equals(this)) { throw new IOException("policy already configured differently"); }
        RatisPolicyFile.publish(root, RatisPolicyFile.Kind.SNAPSHOT, encode(), io);
    }
    /** 保留既有持久格式，不通过更改版本号逃避旧策略的正确重试。 */
    private byte[] encode() throws IOException {
        var bytes = new ByteArrayOutputStream(); var out = new DataOutputStream(bytes); out.writeInt(0x415A5350); out.writeInt(1); out.writeLong(identity.engineId().getMostSignificantBits()); out.writeLong(identity.engineId().getLeastSignificantBits()); out.writeLong(identity.groupId().getMostSignificantBits()); out.writeLong(identity.groupId().getLeastSignificantBits()); out.writeUTF(identity.nodeId());
        out.writeLong(options.intervalMillis()); out.writeLong(options.entryGap()); out.writeLong(options.maxIntervalMillis()); out.writeLong(options.logHighWaterBytes()); out.writeLong(options.preserveEntries()); out.flush(); out.writeInt(crc(bytes.toByteArray())); out.flush();
        return bytes.toByteArray();
    }
    /** 全部身份与配置参与校验。 */
    private static int crc(byte[] bytes) { var checksum = new CRC32C(); checksum.update(bytes); return (int) checksum.getValue(); }
}
