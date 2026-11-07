package com.flyingzc.azir.wal.ha;

import com.flyingzc.azir.wal.WalBackup;
import com.flyingzc.azir.wal.WalBackupOptions;
import com.flyingzc.azir.wal.WalCheckpoint;
import org.apache.ratis.proto.RaftProtos.RaftConfigurationProto;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.server.protocol.TermIndex;
import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.security.DigestInputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/** 完整副本镜像的单文件传输格式，绑定 Raft 位置、成员配置和业务边界，不复制投票元数据。 */
final class RatisSnapshotBundle {
    private static final int MAGIC = 0x415A5253;
    private static final int END = 0x415A5245;
    private static final int MAX_FILES = 102_001;

    /** 固定元数据受整个文件 SHA256 保护，配置位置不能超过已应用位置。 */
    record Header(UUID group, TermIndex position, long configurationIndex, RaftConfigurationProto configuration,
                  UUID backupId, WalCheckpoint boundary) {
        /** 解码前后均验证有限配置、业务边界和成员集合的最低要求。 */
        Header {
            java.util.Objects.requireNonNull(group); java.util.Objects.requireNonNull(position); java.util.Objects.requireNonNull(backupId);
            java.util.Objects.requireNonNull(configuration); java.util.Objects.requireNonNull(boundary);
            if (position.getTerm() < 1 || position.getIndex() < 0 || configurationIndex < -1 || configurationIndex > position.getIndex()
                    || configuration.getPeersCount() < 1 || configuration.getSerializedSize() > 1_048_576 || configuration.getPeersCount() > 1024
                    || configuration.getOldPeersCount() > 1024 || configuration.getListenersCount() > 1024 || configuration.getOldListenersCount() > 1024
                    || boundary.sequence() < 0 || !(boundary.sequence() == 0 ? boundary.digest().isEmpty() : boundary.digest().matches("[a-f0-9]{64}"))) {
                throw new IllegalArgumentException("invalid snapshot metadata");
            }
        }
    }

    /** 工具仅由状态机装配使用，不向业务暴露日志清理或任意路径写入入口。 */
    private RatisSnapshotBundle() { }

    /** 写入新文件并 force；调用方只能在完成后原子发布为 Ratis 快照文件名。 */
    static Header pack(Path backup, Path destination, RaftGroupId group, TermIndex position,
                       long configurationIndex, RaftConfigurationProto configuration, WalBackupOptions options) throws IOException {
        var budget = new Budget(options); Path root = backup.toRealPath(); var info = WalBackup.inspect(root, options);
        if (!info.engineId().equals(group.getUuid()) || !Files.isRegularFile(root.resolve("local/REPLICA"), LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("snapshot requires this Raft group's replica backup");
        }
        var header = new Header(group.getUuid(), position, configurationIndex, configuration, info.backupId(), new WalCheckpoint(info.sequence(), info.digest()));
        var files = new java.util.ArrayList<Path>(); int inspected = 0;
        try (var stream = Files.walk(root, 3)) {
            var iterator = stream.iterator();
            while (iterator.hasNext()) {
                Path path = iterator.next(); if (path.equals(root)) { continue; } budget.check();
                if (++inspected > MAX_FILES + 103_000 || Files.isSymbolicLink(path)) { throw new IOException("snapshot tree exceeds budget or contains symbolic link"); }
                String name = root.relativize(path).toString();
                if (Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS)) {
                    if (!name.matches("local|data|local/checkpoint-[a-f0-9-]{36}")) { throw new IOException("snapshot contains unknown directory"); }
                } else {
                    if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS) || !allowed(name) || files.size() >= MAX_FILES) { throw new IOException("snapshot contains unknown or too many files"); }
                    files.add(path);
                }
            }
        }
        files.sort(java.util.Comparator.comparing(path -> root.relativize(path).toString()));
        if (files.isEmpty() || files.size() > MAX_FILES) { throw new IOException("snapshot file count exceeds budget"); }
        var hash = sha();
        try (var channel = FileChannel.open(destination, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS);
             var buffered = new BufferedOutputStream(java.nio.channels.Channels.newOutputStream(channel), 65_536);
             var digest = new DigestOutputStream(new LimitedOutput(buffered, budget), hash);
             var out = new DataOutputStream(digest)) {
            writeHeader(out, header); out.writeInt(files.size()); var buffer = new byte[65_536];
            for (Path file : files) {
                out.writeUTF(root.relativize(file).toString()); long expected = Files.size(file); out.writeLong(expected); long count = 0;
                try (var input = Files.newInputStream(file, LinkOption.NOFOLLOW_LINKS)) {
                    for (int read; (read = input.read(buffer)) != -1;) { if (read == 0) { throw new IOException("snapshot read made no progress"); } out.write(buffer, 0, read); count += read; }
                }
                if (count != expected) { throw new IOException("snapshot source changed during copy"); }
            }
            out.writeInt(END); out.flush(); byte[] checksum = hash.digest(); digest.on(false); out.write(checksum); out.flush(); channel.force(true);
        }
        forceDirectory(destination.toAbsolutePath().getParent()); return header;
    }

    /** 仅验证封装、元数据和全文件摘要；业务镜像内容还必须经过 extract 的完整格式验证。 */
    static Header inspect(Path bundle, RaftGroupId group, TermIndex position, WalBackupOptions options) throws IOException {
        return scan(bundle, null, group, position, new Budget(options));
    }

    /** 先检查整个传输文件，再解包到私有新目录，完整镜像验证后才发布目标目录。 */
    static Header extract(Path bundle, Path destination, RaftGroupId group, TermIndex position, WalBackupOptions options) throws IOException {
        var budget = new Budget(options); Path target = destination.toAbsolutePath().normalize(); Path parent = target.getParent();
        if (parent == null || !Files.isDirectory(parent, LinkOption.NOFOLLOW_LINKS) || Files.exists(target, LinkOption.NOFOLLOW_LINKS)) { throw new IOException("snapshot extraction requires a new target"); }
        Header inspected = scan(bundle, null, group, position, budget); Path stage = parent.resolve(".azir-unpack-" + UUID.randomUUID());
        Files.createDirectory(stage); forceDirectory(parent);
        try {
            Header extracted = scan(bundle, stage, group, position, budget);
            if (!inspected.equals(extracted)) { throw new IOException("snapshot metadata changed during extraction"); }
            var info = WalBackup.inspect(stage, options);
            if (!info.engineId().equals(extracted.group()) || !info.backupId().equals(extracted.backupId())
                    || info.sequence() != extracted.boundary().sequence() || !info.digest().equals(extracted.boundary().digest())
                    || !Files.isRegularFile(stage.resolve("local/REPLICA"), LinkOption.NOFOLLOW_LINKS)) { throw new IOException("snapshot business boundary mismatch"); }
            budget.check();
            try (var paths = Files.walk(stage, 3)) { for (Path path : paths.filter(Files::isDirectory).sorted(java.util.Comparator.reverseOrder()).toList()) { forceDirectory(path); } }
            if (Files.exists(target, LinkOption.NOFOLLOW_LINKS)) { throw new IOException("snapshot extraction target appeared"); }
            Files.move(stage, target, StandardCopyOption.ATOMIC_MOVE); forceDirectory(parent); return extracted;
        } catch (IOException | RuntimeException | Error failure) {
            try { discardOwnedStage(stage); } catch (IOException cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }

    /** 只清理本方法刚创建的随机私有暂存目录，不处理调用方已有目标，也不跟随符号链接。 */
    private static void discardOwnedStage(Path stage) throws IOException {
        if (!Files.exists(stage, LinkOption.NOFOLLOW_LINKS)) { return; }
        try (var paths = Files.walk(stage)) { for (Path path : paths.sorted(java.util.Comparator.reverseOrder()).toList()) { Files.delete(path); } }
        forceDirectory(stage.getParent());
    }

    /** 所有长度、路径和总字节在写出前校验，不能跟随远端文件名逃出私有恢复目录。 */
    private static Header scan(Path bundle, Path stage, RaftGroupId group, TermIndex position, Budget budget) throws IOException {
        if (!Files.isRegularFile(bundle, LinkOption.NOFOLLOW_LINKS) || Files.size(bundle) > budget.options.maxBytes()) { throw new IOException("snapshot file missing or exceeds byte budget"); }
        var hash = sha(); long bytes = 0;
        try (var input = new BufferedInputStream(Files.newInputStream(bundle, LinkOption.NOFOLLOW_LINKS), 65_536);
             var digest = new DigestInputStream(input, hash); var in = new DataInputStream(digest)) {
            Header header = readHeader(in);
            if (!header.group().equals(group.getUuid()) || !header.position().equals(position)) { throw new IOException("snapshot Raft group or position mismatch"); }
            int count = in.readInt(); if (count < 1 || count > MAX_FILES) { throw new IOException("snapshot file count exceeds budget"); }
            var names = new HashSet<String>(); var buffer = new byte[65_536];
            for (int i = 0; i < count; i++) {
                budget.check(); String name = in.readUTF(); long size = in.readLong();
                if (!allowed(name) || !names.add(name) || size < 1 || size > budget.options.maxBytes() - bytes) { throw new IOException("invalid snapshot file entry"); } bytes += size;
                Path target = stage == null ? null : stage.resolve(name);
                if (target != null) { Files.createDirectories(target.getParent()); }
                try (var out = target == null ? OutputStream.nullOutputStream() : Files.newOutputStream(target, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS)) {
                    long left = size;
                    while (left > 0) {
                        budget.check(); int read = in.read(buffer, 0, (int) Math.min(left, buffer.length));
                        if (read < 1) { throw new IOException("truncated snapshot file"); } out.write(buffer, 0, read); left -= read;
                    }
                }
                if (target != null) { try (var channel = FileChannel.open(target, StandardOpenOption.WRITE)) { channel.force(true); } }
            }
            if (!names.contains("COMPLETE") || !names.contains("local/REPLICA") || in.readInt() != END) { throw new IOException("incomplete snapshot file catalog"); }
            byte[] checksum = hash.digest(); digest.on(false);
            if (!MessageDigest.isEqual(checksum, in.readNBytes(32)) || in.read() != -1) { throw new IOException("snapshot checksum or length mismatch"); }
            budget.check(); return header;
        } catch (IllegalArgumentException failure) { throw new IOException("invalid snapshot metadata", failure); }
    }

    /** 快照只允许备份协议的固定文件集合，路径分隔符和文件名不接受用户自定义。 */
    private static boolean allowed(String name) {
        return name.matches("COMPLETE|local/(ENGINE|REPLICA|CURRENT|transactions\\.wal|receipts-[a-f0-9-]{36}\\.sst|checkpoint-[a-f0-9-]{36}/(snapshot\\.bin|tail\\.wal))|data/[0-9]{4}\\.bin");
    }

    /** 稳定编码 Raft 位置及其成员配置，配置内容与业务文件一起参与摘要。 */
    private static void writeHeader(DataOutputStream out, Header header) throws IOException {
        out.writeInt(MAGIC); out.writeInt(1); uuid(out, header.group()); out.writeLong(header.position().getTerm()); out.writeLong(header.position().getIndex());
        out.writeLong(header.configurationIndex()); byte[] configuration = header.configuration().toByteArray(); out.writeInt(configuration.length); out.write(configuration);
        uuid(out, header.backupId()); out.writeLong(header.boundary().sequence()); out.writeUTF(header.boundary().digest());
    }

    /** 配置消息在分配之前限制长度，反序列化异常不能转成默认空配置。 */
    private static Header readHeader(DataInputStream in) throws IOException {
        if (in.readInt() != MAGIC || in.readInt() != 1) { throw new IOException("unsupported snapshot bundle"); }
        UUID group = uuid(in); TermIndex position = TermIndex.valueOf(in.readLong(), in.readLong()); long configurationIndex = in.readLong(); int length = in.readInt();
        if (length < 1 || length > 1_048_576) { throw new IOException("snapshot configuration exceeds budget"); }
        byte[] bytes = in.readNBytes(length); if (bytes.length != length) { throw new IOException("truncated snapshot configuration"); }
        var configuration = RaftConfigurationProto.parseFrom(bytes); return new Header(group, position, configurationIndex, configuration, uuid(in), new WalCheckpoint(in.readLong(), in.readUTF()));
    }

    /** 固定长度 UUID 不通过目录或字符串猜测。 */
    private static void uuid(DataOutputStream out, UUID value) throws IOException { out.writeLong(value.getMostSignificantBits()); out.writeLong(value.getLeastSignificantBits()); }

    /** 读取固定长度 UUID。 */
    private static UUID uuid(DataInputStream in) throws IOException { return new UUID(in.readLong(), in.readLong()); }

    /** SHA256 缺失属于运行环境错误，不能降低为不校验。 */
    private static MessageDigest sha() throws IOException { try { return MessageDigest.getInstance("SHA-256"); } catch (java.security.NoSuchAlgorithmException failure) { throw new IOException(failure); } }

    /** 文件同步后再同步目录项，不能只依赖内存中的输出文件名。 */
    private static void forceDirectory(Path directory) throws IOException { try (var channel = FileChannel.open(directory, StandardOpenOption.READ)) { channel.force(true); } }

    /** 维护使用单调期限，所有分块在 I/O 前检查总量或期限。 */
    private static final class Budget {
        final WalBackupOptions options; final long started = System.nanoTime(); long written;
        /** 每次封装或解包独立计时。 */
        Budget(WalBackupOptions options) { this.options = java.util.Objects.requireNonNull(options); }
        /** 不通过线程中断取消共享引擎文件通道。 */
        void check() throws IOException { if (Thread.currentThread().isInterrupted() || System.nanoTime() - started >= TimeUnit.MILLISECONDS.toNanos(options.timeoutMillis())) { throw new IOException("snapshot maintenance timed out or interrupted"); } }
        /** 文件头和成员配置也计入字节总预算。 */
        void add(int size) throws IOException { check(); if (size < 0 || size > options.maxBytes() - written) { throw new IOException("snapshot bundle exceeds byte budget"); } written += size; }
    }

    /** 编码前逐块预留字节，不能写满目标磁盘后才检查上限。 */
    private static final class LimitedOutput extends FilterOutputStream {
        final Budget budget;
        /** 保存独立操作的预算。 */
        LimitedOutput(OutputStream output, Budget budget) { super(output); this.budget = budget; }
        /** 单字节头字段同样受总量限制。 */
        @Override public void write(int value) throws IOException { budget.add(1); out.write(value); }
        /** 大块直接写入底层，不退化为逐字节循环。 */
        @Override public void write(byte[] bytes, int offset, int length) throws IOException { budget.add(length); out.write(bytes, offset, length); }
    }
}
