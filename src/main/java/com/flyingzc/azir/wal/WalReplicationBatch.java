package com.flyingzc.azir.wal;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.UUID;

/** 共识层的有界完整批次，任期和来源独立于业务提交摘要，不改变历史 UUID 决议。 */
public record WalReplicationBatch(long epoch, WalReplicaIdentity origin, List<WalCommit> commits) {
    public static final int MAX_BYTES = 16 * 1024 * 1024;

    /** 拒绝空批次和非法任期，业务记录保持不可变。 */
    public WalReplicationBatch {
        java.util.Objects.requireNonNull(origin); commits = List.copyOf(commits);
        if (epoch < 1 || commits.isEmpty() || commits.size() > 1024) { throw new IllegalArgumentException("invalid replication batch"); }
    }

    /** 先检查各记录和总长度，再分配共识消息缓冲。 */
    public byte[] encode() throws IOException {
        var encoded = commits.stream().map(WalCodec::encode).toList();
        long length = 64 + origin.nodeId().length() + encoded.stream().mapToLong(bytes -> 4L + bytes.length).sum();
        if (length > MAX_BYTES) { throw new IOException("replication batch exceeds byte limit"); }
        var bytes = new ByteArrayOutputStream(); var out = new DataOutputStream(bytes);
        out.writeInt(0x415A5242); out.writeInt(1); out.writeLong(epoch);
        out.writeLong(origin.engineId().getMostSignificantBits()); out.writeLong(origin.engineId().getLeastSignificantBits());
        out.writeLong(origin.groupId().getMostSignificantBits()); out.writeLong(origin.groupId().getLeastSignificantBits()); out.writeUTF(origin.nodeId());
        out.writeInt(encoded.size()); for (byte[] data : encoded) { out.writeInt(data.length); out.write(data); } out.flush(); return bytes.toByteArray();
    }

    /** 在创建值对象之前检查总大小和每条长度，不能信任网络提供的记录计数。 */
    public static WalReplicationBatch decode(byte[] bytes) throws IOException {
        if (bytes.length < 60 || bytes.length > MAX_BYTES) { throw new IOException("replication message size invalid"); }
        try {
            var in = new DataInputStream(new ByteArrayInputStream(bytes));
            if (in.readInt() != 0x415A5242 || in.readInt() != 1) { throw new IOException("unsupported replication batch"); }
            long epoch = in.readLong(); UUID engine = new UUID(in.readLong(), in.readLong()); UUID group = new UUID(in.readLong(), in.readLong()); String node = in.readUTF();
            int count = in.readInt(); if (count < 1 || count > 1024 || count > in.available() / 40) { throw new IOException("replication record count invalid"); }
            var commits = new java.util.ArrayList<WalCommit>();
            for (int i = 0; i < count; i++) {
                int length = in.readInt(); if (length < 36 || length > in.available()) { throw new IOException("replication record truncated"); }
                commits.add(WalCodec.decode(in.readNBytes(length)));
            }
            if (in.available() != 0) { throw new IOException("trailing replication data"); }
            return new WalReplicationBatch(epoch, new WalReplicaIdentity(engine, group, node), commits);
        } catch (IllegalArgumentException failure) { throw new IOException("invalid replication batch", failure); }
    }

    /** 提案匹配用完整编码摘要，不能用业务 UUID 或概率过滤器代替。 */
    public String digest() throws IOException { return WalCodec.digest(encode()); }
}
