package mvcc.wal.ha;

import java.io.*;
import java.util.UUID;

/** 指定副本在已应用位置之后的内部健康回复，不返回任何业务数据。 */
record RatisReplicaStatus(UUID group, String node, long appliedIndex, long publishedSequence, boolean healthy) {
    private static final int MAGIC = 0x415A5254;

    /** 请求标识与业务消息分离。 */
    static byte[] request() { return java.nio.ByteBuffer.allocate(8).putInt(MAGIC).putInt(1).array(); }

    /** 只有精确内部请求才进入状态查询。 */
    static boolean matches(byte[] bytes) { return java.util.Arrays.equals(bytes, request()); }

    /** 回复固定身份、水位和健康标记，字段长度有界。 */
    byte[] encode() throws IOException {
        var buffer = new ByteArrayOutputStream(); var out = new DataOutputStream(buffer); out.writeInt(MAGIC); out.writeInt(1); out.writeLong(group.getMostSignificantBits()); out.writeLong(group.getLeastSignificantBits());
        out.writeUTF(node); out.writeLong(appliedIndex); out.writeLong(publishedSequence); out.writeBoolean(healthy); out.flush(); return buffer.toByteArray();
    }

    /** 错误节点、损坏字段或未知版本不能被解释为健康。 */
    static RatisReplicaStatus decode(byte[] bytes) throws IOException {
        if (bytes.length < 43 || bytes.length > 256) { throw new IOException("replica status exceeds budget"); }
        var in = new DataInputStream(new ByteArrayInputStream(bytes)); if (in.readInt() != MAGIC || in.readInt() != 1) { throw new IOException("unsupported replica status"); }
        var result = new RatisReplicaStatus(new UUID(in.readLong(), in.readLong()), in.readUTF(), in.readLong(), in.readLong(), in.readBoolean());
        if (!result.node().matches("[A-Za-z0-9._-]{1,64}") || result.appliedIndex() < -1 || result.publishedSequence() < 0 || in.available() != 0) { throw new IOException("invalid replica status"); } return result;
    }
}
