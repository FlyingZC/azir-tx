package com.flyingzc.azir.wal.ha;

import com.flyingzc.azir.wal.WalReplicaIdentity;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;
import java.util.zip.CRC32C;

/** 健康策略一经启用即持久保存，重启先关闭业务和竞选资格，实际探测通过后恢复。 */
record RatisHealthRecord(WalReplicaIdentity identity, RatisHealthOptions options) {
    /** 记录绑定本机身份，不存数据库地址、密码或证书。 */
    RatisHealthRecord { Objects.requireNonNull(identity); Objects.requireNonNull(options); }
    /** 仅允许当前节点同一份明确策略，不能靠启动参数遗漏绕过隔离。 */
    static RatisHealthRecord load(Path directory, WalReplicaIdentity identity) throws IOException {
        Path path = directory.resolve("HEALTH_POLICY"); if (!Files.exists(path, LinkOption.NOFOLLOW_LINKS)) { return null; }
        if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS) || Files.size(path) > 512) { throw new IOException("health policy missing or exceeds budget"); }
        byte[] bytes = Files.readAllBytes(path); if (bytes.length < 79 || ByteBuffer.wrap(bytes).getInt(bytes.length - 4) != crc(Arrays.copyOf(bytes, bytes.length - 4))) { throw new IOException("health policy checksum mismatch"); }
        try {
            var in = new DataInputStream(new ByteArrayInputStream(bytes, 0, bytes.length - 4)); if (in.readInt() != 0x415A4850 || in.readInt() != 1) { throw new IOException("unsupported health policy"); }
            var owner = new WalReplicaIdentity(new UUID(in.readLong(), in.readLong()), new UUID(in.readLong(), in.readLong()), in.readUTF());
            var options = new RatisHealthOptions(in.readLong(), in.readInt(), in.readInt(), in.readLong(), in.readLong());
            if (!owner.equals(identity) || in.available() != 0) { throw new IOException("health policy owner mismatch"); } return new RatisHealthRecord(owner, options);
        } catch (IllegalArgumentException malformed) { throw new IOException("invalid health policy", malformed); }
    }
    /** 节点独占目录锁内持久发布，既有正确策略也重新确认文件及目录。 */
    void publish(Path directory) throws IOException { publish(directory, RatisPolicyFile.REAL); }
    /** 包内故障注入与正式路径共用同一编码和真实发布协议。 */
    void publish(Path directory, RatisPolicyFile.Io io) throws IOException {
        var previous = load(directory, identity);
        if (previous != null && !previous.equals(this)) { throw new IOException("policy already configured differently"); }
        RatisPolicyFile.publish(directory, RatisPolicyFile.Kind.HEALTH, encode(), io);
    }
    /** 保留既有持久格式，不通过更改版本号逃避旧策略的正确重试。 */
    private byte[] encode() throws IOException {
        var bytes = new ByteArrayOutputStream(); var out = new DataOutputStream(bytes); out.writeInt(0x415A4850); out.writeInt(1); out.writeLong(identity.engineId().getMostSignificantBits()); out.writeLong(identity.engineId().getLeastSignificantBits()); out.writeLong(identity.groupId().getMostSignificantBits()); out.writeLong(identity.groupId().getLeastSignificantBits()); out.writeUTF(identity.nodeId());
        out.writeLong(options.intervalMillis()); out.writeInt(options.failures()); out.writeInt(options.recoveries()); out.writeLong(options.probeMillis()); out.writeLong(options.transferMillis()); out.flush(); out.writeInt(crc(bytes.toByteArray())); out.flush();
        return bytes.toByteArray();
    }
    /** 全部身份和策略参与校验。 */
    private static int crc(byte[] bytes) { var value = new CRC32C(); value.update(bytes); return (int) value.getValue(); }
}
