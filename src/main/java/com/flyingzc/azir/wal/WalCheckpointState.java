package com.flyingzc.azir.wal;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/** 可恢复的完整检查点：已发布边界、快照锚点、路由及凭据归档目录。 */
record WalCheckpointState(UUID engineId, Set<String> shards, Set<String> storedShards, long sequence,
                          long safePoint, WalReceipt boundary, List<Version> versions,
                          List<WalReceiptArchive.Descriptor> archives, byte[] archiveBloom, long retiredThrough) {
    /** 保存无后端分片的安全快照历史；解码兼容旧镜像中可丢弃的已物化头缓存。 */
    record Version(long sequence, WalMutation mutation) { }

    /** 固定快照内容，所有恢复参数都必须与同一发布边界一致。 */
    WalCheckpointState {
        shards = Set.copyOf(shards); storedShards = Set.copyOf(storedShards); versions = List.copyOf(versions);
        archives = List.copyOf(archives); archiveBloom = archiveBloom.clone();
        if (engineId == null || shards.isEmpty() || shards.size() > 1024 || !shards.containsAll(storedShards)
                || sequence < 1 || safePoint < 0 || safePoint > sequence || boundary == null
                || boundary.commitSequence() != sequence || !boundary.digest().matches("[a-f0-9]{64}") || retiredThrough < 0 || retiredThrough > sequence
                || archives.size() > 100_000 || versions.size() > 1_000_000
                || archiveBloom.length < 128 || archiveBloom.length > 16 * 1024 * 1024 || Integer.bitCount(archiveBloom.length) != 1) {
            throw new IllegalArgumentException("invalid checkpoint state");
        }
    }

    /** 返回过滤器副本，运行时的增量更新不能改变已经生成的快照。 */
    @Override public byte[] archiveBloom() { return archiveBloom.clone(); }

    /** 编码只包含恢复所需状态，不把活跃事务的本地写集写入检查点。 */
    byte[] encode() throws IOException {
        checkEncodingBudget(shards, storedShards, versions, archives.size(), archiveBloom.length);
        var buffer = new ByteArrayOutputStream(); var out = new DataOutputStream(buffer);
        // 新格式明确标记后端头已不再是恢复必需状态，旧程序必须拒绝而不能缺失冲突栅栏后继续运行。
        out.writeInt(0x415A4353); out.writeInt(2); uuid(out, engineId); strings(out, shards); strings(out, storedShards);
        out.writeLong(sequence); out.writeLong(safePoint); uuid(out, boundary.transactionId()); out.writeUTF(boundary.digest());
        out.writeLong(retiredThrough); out.writeInt(archiveBloom.length); out.write(archiveBloom);
        out.writeInt(archives.size());
        for (var archive : archives) {
            out.writeUTF(archive.fileName()); out.writeLong(archive.count()); out.writeLong(archive.minSequence());
            out.writeLong(archive.maxSequence()); out.writeLong(archive.expiresAtMillis()); out.writeUTF(archive.metadataHash());
        }
        out.writeInt(versions.size());
        for (var version : versions) {
            out.writeLong(version.sequence); out.writeUTF(version.mutation.shard()); out.writeUTF(version.mutation.key());
            byte[] value = version.mutation.value(); out.writeInt(value == null ? -1 : value.length); if (value != null) { out.write(value); }
        }
        out.flush();
        if (buffer.size() > 128 * 1024 * 1024) { throw new IOException("checkpoint image exceeds size budget"); }
        return buffer.toByteArray();
    }

    /** 文件创建和大缓冲分配之前估算完整镜像，超预算只背压维护，不把健康运行时误停机。 */
    static long checkEncodingBudget(Set<String> shards, Set<String> storedShards, List<Version> versions, int archiveCount, int bloomBytes) {
        long size = 24 + stringsSize(shards) + stringsSize(storedShards) + 32 + 66 + 8 + 4 + bloomBytes + 4 + 4 + 149L * archiveCount;
        for (var version : versions) {
            size += 8L + utfSize(version.mutation().shard()) + utfSize(version.mutation().key()) + 4 + version.mutation().valueLength();
            if (size > 128L * 1024 * 1024) { break; }
        }
        if (size > 128L * 1024 * 1024) {
            throw new WalMvccException(com.flyingzc.azir.common.ErrorCode.RESOURCE_EXHAUSTED, "checkpoint encoding budget exhausted", null, null);
        }
        return size;
    }

    /** 路由集合的编码计数，不创建中间 UTF 字节数组。 */
    private static long stringsSize(Set<String> values) { return 4L + values.stream().mapToLong(WalCheckpointState::utfSize).sum(); }

    /** DataOutputStream.writeUTF 使用 modified UTF-8，空字节和代理对必须按实际格式计费。 */
    private static long utfSize(String value) {
        long bytes = 2;
        for (int i = 0; i < value.length(); i++) { char c = value.charAt(i); bytes += c >= 1 && c <= 127 ? 1 : c <= 2047 ? 2 : 3; }
        if (bytes > 65537) { throw new IllegalArgumentException("checkpoint UTF field too long"); }
        return bytes;
    }

    /** 在整体摘要已验证后解码，所有长度还须独立检查，避免畸形状态造成无界分配。 */
    static WalCheckpointState decode(byte[] bytes) throws IOException {
        try {
            var in = new DataInputStream(new ByteArrayInputStream(bytes));
            if (in.readInt() != 0x415A4353) { throw new IOException("unsupported checkpoint image"); }
            int format = in.readInt();
            if (format != 1 && format != 2) { throw new IOException("unsupported checkpoint image"); }
            UUID engine = uuid(in); Set<String> shards = strings(in); Set<String> stored = strings(in);
            long sequence = in.readLong(); long safe = in.readLong(); UUID id = uuid(in); String digest = in.readUTF();
            long retired = in.readLong(); int length = in.readInt();
            if (length < 128 || length > 16 * 1024 * 1024 || Integer.bitCount(length) != 1 || length > in.available()) {
                throw new IOException("invalid checkpoint receipt filter");
            }
            byte[] bloom = in.readNBytes(length); int count = count(in, 100_000, 100);
            var archives = new ArrayList<WalReceiptArchive.Descriptor>(); var names = new HashSet<String>();
            for (int i = 0; i < count; i++) {
                var archive = new WalReceiptArchive.Descriptor(in.readUTF(), in.readLong(), in.readLong(), in.readLong(), in.readLong(), in.readUTF());
                if (!names.add(archive.fileName()) || archive.maxSequence() > sequence) { throw new IOException("invalid receipt catalog"); }
                archives.add(archive);
            }
            count = count(in, 1_000_000, 16); var versions = new ArrayList<Version>();
            var identities = new HashSet<String>();
            for (int i = 0; i < count; i++) {
                long version = in.readLong(); String shard = in.readUTF(); String key = in.readUTF(); int size = in.readInt();
                if (version < 1 || version > sequence || !shards.contains(shard) || size < -1 || size > 8 * 1024 * 1024
                        || size > in.available() || !identities.add(shard + "\u0000" + key + "\u0000" + version)) {
                    throw new IOException("invalid checkpoint version");
                }
                versions.add(new Version(version, new WalMutation(shard, key, size == -1 ? null : in.readNBytes(size))));
            }
            if (in.available() != 0 || !digest.matches("[a-f0-9]{64}")) { throw new IOException("trailing or invalid checkpoint state"); }
            return new WalCheckpointState(engine, shards, stored, sequence, safe, new WalReceipt(id, sequence, digest), versions, archives, bloom, retired);
        } catch (IllegalArgumentException invalid) { throw new IOException("invalid checkpoint image", invalid); }
    }

    /** 将路由集合以稳定次序写入快照。 */
    private static void strings(DataOutputStream out, Set<String> values) throws IOException {
        out.writeInt(values.size()); for (String value : values.stream().sorted().toList()) { out.writeUTF(value); }
    }

    /** 路由必须唯一且能够由正常变更对象表示。 */
    private static Set<String> strings(DataInputStream in) throws IOException {
        int count = count(in, 1024, 2); var result = new HashSet<String>();
        for (int i = 0; i < count; i++) { String shard = in.readUTF(); new WalMutation(shard, "", null);
            if (shard.contains("\u0000") || !result.add(shard)) { throw new IOException("duplicate or invalid checkpoint route"); } }
        return result;
    }

    /** 校验记录数量与剩余字节的下界。 */
    private static int count(DataInputStream in, int maximum, int minimumBytes) throws IOException {
        int count = in.readInt(); if (count < 0 || count > maximum || count > in.available() / minimumBytes) { throw new IOException("invalid checkpoint count"); }
        return count;
    }

    /** 写入完整 UUID。 */
    private static void uuid(DataOutputStream out, UUID id) throws IOException { out.writeLong(id.getMostSignificantBits()); out.writeLong(id.getLeastSignificantBits()); }

    /** 读取完整 UUID。 */
    private static UUID uuid(DataInputStream in) throws IOException { return new UUID(in.readLong(), in.readLong()); }
}
