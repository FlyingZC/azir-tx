package mvcc.wal;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/** 备份最后发布的完整清单，固定引擎、提交边界和所有文件的大小与摘要。 */
record WalBackupManifest(UUID backupId, UUID engineId, WalCheckpoint boundary, Set<String> shards,
                         Set<String> storedShards, List<Entry> files, List<Data> data, WalReplicaIdentity replica) {
    static final int MAX_BYTES = 32 * 1024 * 1024;

    /** 原有单机备份继续输出版本一，不隐式改变其磁盘格式。 */
    WalBackupManifest(UUID backupId, UUID engineId, WalCheckpoint boundary, Set<String> shards,
                      Set<String> storedShards, List<Entry> files, List<Data> data) {
        this(backupId, engineId, boundary, shards, storedShards, files, data, null);
    }

    /** 文件路径只允许本实现的固定格式，不能通过备份清单写入任意目标文件。 */
    record Entry(String path, long size, String hash) {
        /** 路径必须是相对的已知文件名，大小和摘要必须明确。 */
        Entry {
            if (path == null || !path.matches("local/(ENGINE|REPLICA|CURRENT|transactions\\.wal|receipts-[a-f0-9-]{36}\\.sst|checkpoint-[a-f0-9-]{36}/(snapshot\\.bin|tail\\.wal))|data/[0-9]{4}\\.bin")
                    || size < 1 || hash == null || !hash.matches("[a-f0-9]{64}")) { throw new IllegalArgumentException("invalid backup file entry"); }
        }
    }

    /** 一个后端数据流的路由、文件名和严格核对的总行数。 */
    record Data(String shard, String file, long rows) {
        /** 不能用路径替代分片身份，数据流只能引用固定 data 目录。 */
        Data {
            new WalMutation(shard, "", null);
            if (shard.contains("\u0000") || file == null || !file.matches("data/[0-9]{4}\\.bin") || rows < 0) { throw new IllegalArgumentException("invalid backup data catalog"); }
        }
    }

    /** 固定清单并拒绝重复路径、重复分片及不完整的数据路由。 */
    WalBackupManifest {
        new WalRestoreSpec(backupId, engineId, "manifest", boundary);
        if (replica != null && !replica.engineId().equals(engineId)) { throw new IllegalArgumentException("backup replica engine mismatch"); }
        shards = Set.copyOf(shards); storedShards = Set.copyOf(storedShards); files = List.copyOf(files); data = List.copyOf(data);
        if (shards.isEmpty() || shards.size() > 1024 || !shards.containsAll(storedShards) || files.size() > 102_000) { throw new IllegalArgumentException("invalid backup routes"); }
        for (String shard : shards) { new WalMutation(shard, "", null); if (shard.contains("\u0000")) { throw new IllegalArgumentException("invalid backup route"); } }
        var paths = new HashSet<String>(); for (var file : files) { if (!paths.add(file.path())) { throw new IllegalArgumentException("duplicate backup file"); } }
        if (paths.contains("local/REPLICA") != (replica != null)) { throw new IllegalArgumentException("backup replica identity catalog mismatch"); }
        var names = new HashSet<String>(); var dataPaths = new HashSet<String>();
        for (var stream : data) {
            if (!names.add(stream.shard()) || !dataPaths.add(stream.file()) || !paths.contains(stream.file())) { throw new IllegalArgumentException("invalid backup data routes"); }
        }
        if (!names.equals(storedShards)) { throw new IllegalArgumentException("missing backend snapshot"); }
    }

    /** 编码完整清单并附加 CRC，发布前必须先同步所有被引用文件。 */
    byte[] encode() throws IOException {
        var buffer = new ByteArrayOutputStream(); var out = new DataOutputStream(buffer);
        out.writeInt(0x415A424D); out.writeInt(replica == null ? 1 : 2); uuid(out, backupId); uuid(out, engineId);
        if (replica != null) { byte[] identity = replica.encode(); out.writeInt(identity.length); out.write(identity); }
        out.writeLong(boundary.sequence()); out.writeUTF(boundary.digest()); strings(out, shards); strings(out, storedShards);
        out.writeInt(files.size());
        for (var file : files) { out.writeUTF(file.path()); out.writeLong(file.size()); out.writeUTF(file.hash()); }
        out.writeInt(data.size()); for (var stream : data) { out.writeUTF(stream.shard()); out.writeUTF(stream.file()); out.writeLong(stream.rows()); }
        out.flush();
        if (buffer.size() > MAX_BYTES - 4) { throw new IOException("backup manifest exceeds budget"); }
        out.writeInt(WalFiles.crc(buffer.toByteArray())); out.flush(); return buffer.toByteArray();
    }

    /** 解码前先核对总体长度和 CRC，所有计数也必须受剩余字节约束。 */
    static WalBackupManifest decode(byte[] bytes) throws IOException {
        if (bytes.length < 128 || bytes.length > MAX_BYTES || ByteBuffer.wrap(bytes).getInt(bytes.length - 4) != WalFiles.crc(bytes, 0, bytes.length - 4)) {
            throw new IOException("invalid backup manifest checksum");
        }
        try {
            var in = new DataInputStream(new ByteArrayInputStream(bytes, 0, bytes.length - 4));
            if (in.readInt() != 0x415A424D) { throw new IOException("unsupported backup manifest"); }
            int version = in.readInt(); if (version != 1 && version != 2) { throw new IOException("unsupported backup manifest"); }
            UUID backup = uuid(in); UUID engine = uuid(in); WalReplicaIdentity replica = null;
            if (version == 2) {
                int length = count(in, 256, 1); replica = WalReplicaIdentity.decode(in.readNBytes(length));
            }
            var boundary = new WalCheckpoint(in.readLong(), in.readUTF());
            var shards = strings(in); var stored = strings(in); var files = new java.util.ArrayList<Entry>();
            int count = count(in, 102_000, 12);
            for (int i = 0; i < count; i++) { files.add(new Entry(in.readUTF(), in.readLong(), in.readUTF())); }
            count = count(in, 1024, 12); var data = new java.util.ArrayList<Data>();
            for (int i = 0; i < count; i++) { data.add(new Data(in.readUTF(), in.readUTF(), in.readLong())); }
            if (in.available() != 0) { throw new IOException("trailing backup manifest data"); }
            return new WalBackupManifest(backup, engine, boundary, shards, stored, files, data, replica);
        } catch (IllegalArgumentException failure) { throw new IOException("invalid backup manifest", failure); }
    }

    /** 以稳定顺序编码路由集合。 */
    private static void strings(DataOutputStream out, Set<String> values) throws IOException {
        out.writeInt(values.size()); for (String value : values.stream().sorted().toList()) { out.writeUTF(value); }
    }

    /** 读取有限且唯一的路由集合。 */
    private static Set<String> strings(DataInputStream in) throws IOException {
        int count = count(in, 1024, 2); var result = new HashSet<String>();
        for (int i = 0; i < count; i++) { if (!result.add(in.readUTF())) { throw new IOException("duplicate backup route"); } }
        return result;
    }

    /** 防止畸形数量触发大规模分配。 */
    private static int count(DataInputStream in, int maximum, int minimum) throws IOException {
        int count = in.readInt(); if (count < 0 || count > maximum || count > in.available() / minimum) { throw new IOException("invalid backup count"); }
        return count;
    }

    /** 写入固定长度 UUID。 */
    private static void uuid(DataOutputStream out, UUID value) throws IOException { out.writeLong(value.getMostSignificantBits()); out.writeLong(value.getLeastSignificantBits()); }

    /** 读取固定长度 UUID。 */
    private static UUID uuid(DataInputStream in) throws IOException { return new UUID(in.readLong(), in.readLong()); }
}
