package mvcc.wal.ha;

import mvcc.wal.WalReplicaIdentity;
import java.io.*;
import java.util.UUID;

/** 不修改业务状态的新鲜多数派读栅栏，每个请求有独立 nonce，不能复用旧心跳证据。 */
record RatisReadFence(long epoch, WalReplicaIdentity origin, long nonce) {
    private static final int MAGIC = 0x415A5246;

    /** 拒绝非法任期和请求号。 */
    RatisReadFence { if (epoch < 1 || nonce < 1 || origin == null) { throw new IllegalArgumentException("invalid read fence"); } }

    /** 固定小消息类型区别于业务批次。 */
    byte[] encode() throws IOException {
        var bytes = new ByteArrayOutputStream(); var out = new DataOutputStream(bytes); out.writeInt(MAGIC); out.writeInt(1); out.writeLong(epoch);
        out.writeLong(origin.engineId().getMostSignificantBits()); out.writeLong(origin.engineId().getLeastSignificantBits());
        out.writeLong(origin.groupId().getMostSignificantBits()); out.writeLong(origin.groupId().getLeastSignificantBits()); out.writeUTF(origin.nodeId()); out.writeLong(nonce); out.flush(); return bytes.toByteArray();
    }

    /** 类型探测只读取固定头，不进行业务解码。 */
    static boolean matches(byte[] bytes) { return bytes.length >= 4 && java.nio.ByteBuffer.wrap(bytes).getInt() == MAGIC; }

    /** 严格校验长度、身份和尾部。 */
    static RatisReadFence decode(byte[] bytes) throws IOException {
        if (bytes.length > 256) { throw new IOException("read fence too large"); }
        try {
            var in = new DataInputStream(new ByteArrayInputStream(bytes)); if (in.readInt() != MAGIC || in.readInt() != 1) { throw new IOException("invalid read fence format"); }
            long term = in.readLong(); var identity = new WalReplicaIdentity(new UUID(in.readLong(), in.readLong()), new UUID(in.readLong(), in.readLong()), in.readUTF());
            var result = new RatisReadFence(term, identity, in.readLong()); if (in.available() != 0) { throw new IOException("trailing read fence"); } return result;
        } catch (IllegalArgumentException failure) { throw new IOException("invalid read fence", failure); }
    }
}
