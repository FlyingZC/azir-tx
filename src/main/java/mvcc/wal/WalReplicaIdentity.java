package mvcc.wal;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.UUID;

/** 复制目录的持久身份，共享引擎和 Raft 组，各副本使用不同节点身份。 */
public record WalReplicaIdentity(UUID engineId, UUID groupId, String nodeId) {
    /** 节点标识必须能安全用于部署配置，不允许路径或控制字符。 */
    public WalReplicaIdentity {
        java.util.Objects.requireNonNull(engineId); java.util.Objects.requireNonNull(groupId);
        if (nodeId == null || !nodeId.matches("[A-Za-z0-9._-]{1,64}")) { throw new IllegalArgumentException("invalid replica node identity"); }
    }

    /** 固定格式同时绑定三种身份，CRC 保护恢复入口。 */
    byte[] encode() throws IOException {
        var bytes = new ByteArrayOutputStream(); var out = new DataOutputStream(bytes);
        out.writeInt(0x415A5250); out.writeInt(1); out.writeLong(engineId.getMostSignificantBits()); out.writeLong(engineId.getLeastSignificantBits());
        out.writeLong(groupId.getMostSignificantBits()); out.writeLong(groupId.getLeastSignificantBits()); out.writeUTF(nodeId);
        out.flush(); out.writeInt(WalFiles.crc(bytes.toByteArray())); out.flush(); return bytes.toByteArray();
    }

    /** 不跟随符号链接，身份缺失或损坏不能自动猜测另一节点。 */
    static WalReplicaIdentity read(Path root) throws IOException {
        Path path = root.resolve("REPLICA");
        if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) { throw new IOException("replica identity missing or not regular"); }
        return decode(WalFiles.readBounded(path, 256));
    }

    /** 备份清单内的源副本身份使用相同严格编码，不把目录名称当作身份。 */
    static WalReplicaIdentity decode(byte[] bytes) throws IOException {
        if (bytes.length > 256) { throw new IOException("replica identity exceeds budget"); }
        if (bytes.length < 46 || ByteBuffer.wrap(bytes).getInt(bytes.length - 4) != WalFiles.crc(bytes, 0, bytes.length - 4)) { throw new IOException("replica identity checksum mismatch"); }
        try {
            var in = new DataInputStream(new ByteArrayInputStream(bytes, 0, bytes.length - 4));
            if (in.readInt() != 0x415A5250 || in.readInt() != 1) { throw new IOException("unsupported replica identity"); }
            var result = new WalReplicaIdentity(new UUID(in.readLong(), in.readLong()), new UUID(in.readLong(), in.readLong()), in.readUTF());
            if (in.available() != 0) { throw new IOException("trailing replica identity"); } return result;
        } catch (IllegalArgumentException failure) { throw new IOException("invalid replica identity", failure); }
    }

    /** 在新目录发布身份，不覆盖已有节点身份。 */
    void persist(Path root) throws IOException {
        WalFiles.writeNew(root.resolve("REPLICA"), encode()); WalFiles.forceDirectory(root);
    }

    /** 数据库所有权指纹独立于用户表键，不泄漏部署凭据。 */
    String fingerprint() {
        return WalCodec.digest((engineId + "\u0000" + groupId + "\u0000" + nodeId).getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }
}
