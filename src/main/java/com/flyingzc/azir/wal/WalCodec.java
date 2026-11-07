package com.flyingzc.azir.wal;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.UUID;

/** 固定版本的二进制日志编码，所有长度在分配内存前校验。 */
final class WalCodec {
    static final int MAX_FRAME = 32 * 1024 * 1024;

    /** 禁止实例化日志编码工具。 */
    private WalCodec() { }

    /** 编码完整事务，字段次序也定义内容摘要。 */
    static byte[] encode(WalCommit commit) {
        try {
            var bytes = new ByteArrayOutputStream();
            var out = new DataOutputStream(bytes);
            out.writeLong(commit.sequence());
            out.writeLong(commit.transactionId().getMostSignificantBits());
            out.writeLong(commit.transactionId().getLeastSignificantBits());
            out.writeLong(commit.startSequence());
            out.writeInt(commit.mutations().size());
            for (WalMutation mutation : commit.mutations()) {
                writeBytes(out, mutation.shard().getBytes(StandardCharsets.UTF_8));
                writeBytes(out, mutation.key().getBytes(StandardCharsets.UTF_8));
                byte[] value = mutation.value();
                out.writeInt(value == null ? -1 : value.length);
                if (value != null) { out.write(value); }
            }
            out.flush();
            return bytes.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    /** 解码并拒绝畸形字段、重复键和尾随数据。 */
    static WalCommit decode(byte[] data) throws IOException {
        try {
            var in = new DataInputStream(new ByteArrayInputStream(data));
            long sequence = in.readLong();
            UUID id = new UUID(in.readLong(), in.readLong());
            long start = in.readLong();
            int count = in.readInt();
            if (count < 1 || count > 100_000 || count > data.length / 12) {
                throw new IOException("invalid mutation count");
            }
            var writes = new ArrayList<WalMutation>();
            var keys = new java.util.HashSet<WalMvccRuntime.Key>();
            for (int i = 0; i < count; i++) {
                String shard = text(readBytes(in, 128));
                String key = text(readBytes(in, 512));
                int size = in.readInt();
                if (size < -1 || size > MAX_FRAME || size > in.available()) {
                    throw new IOException("invalid value size");
                }
                byte[] value = size == -1 ? null : in.readNBytes(size);
                if (!keys.add(new WalMvccRuntime.Key(shard, key))) { throw new IOException("duplicate key"); }
                writes.add(new WalMutation(shard, key, value));
            }
            if (in.available() != 0) { throw new IOException("trailing transaction data"); }
            return new WalCommit(id, sequence, start, writes);
        } catch (IllegalArgumentException ex) {
            throw new IOException("invalid transaction", ex);
        }
    }

    /** 写入带长度前缀的字段。 */
    private static void writeBytes(DataOutputStream out, byte[] bytes) throws IOException {
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    /** 读取有限字段，拒绝截断和异常长度。 */
    private static byte[] readBytes(DataInputStream in, int max) throws IOException {
        int size = in.readInt();
        if (size < 0 || size > max || size > in.available()) { throw new IOException("invalid field size"); }
        return in.readNBytes(size);
    }

    /** 严格校验 UTF-8，拒绝替换字符导致键身份发生变化。 */
    private static String text(byte[] bytes) throws IOException {
        try { return StandardCharsets.UTF_8.newDecoder().decode(java.nio.ByteBuffer.wrap(bytes)).toString(); }
        catch (java.nio.charset.CharacterCodingException ex) { throw new IOException("invalid UTF-8", ex); }
    }

    /** 计算内容摘要，用于持久日志与数据库前缀核对。 */
    static String digest(byte[] bytes) {
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)); }
        catch (NoSuchAlgorithmException impossible) { throw new IllegalStateException(impossible); }
    }
}
