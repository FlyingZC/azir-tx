package com.flyingzc.azir.wal.ha;

import com.flyingzc.azir.wal.WalBackupOptions;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.FileInfo;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.*;
import org.apache.ratis.statemachine.impl.SingleFileSnapshotInfo;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.util.MD5FileUtil;
import org.apache.ratis.io.MD5Hash;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.UUID;

/** 可重载的单文件快照目录，不吞掉损坏异常，不把缺失最新快照降级成旧状态。 */
final class RatisSnapshotStorage implements StateMachineStorage {
    private final RaftGroupId group;
    private final WalBackupOptions limits;
    private final Io io;
    private Path directory;
    private volatile SingleFileSnapshotInfo latest;
    private Stamp verifiedData, verifiedDigest;
    /** 已验证不可变文件的签名缓存，变化或重启时重新完整核对。 */
    private record Stamp(Object fileKey, long bytes, java.nio.file.attribute.FileTime modified) { }

    /** 使用明确组身份和有限维护预算。 */
    RatisSnapshotStorage(RaftGroupId group, WalBackupOptions limits) { this(group, limits, new Io() { }); }

    /** 故障注入只作用于包内构造，真实操作与正常装配完全一致。 */
    RatisSnapshotStorage(RaftGroupId group, WalBackupOptions limits, Io io) { this.group = group; this.limits = limits; this.io = java.util.Objects.requireNonNull(io); }

    /** 镜像大文件和传输摘要的真实 I/O 边界，不允许测试钩子代替内容校验。 */
    interface Io {
        /** 大文件逐块写入，部分失败留下私有输入。 */
        default void write(FileChannel channel, ByteBuffer bytes) throws IOException { RatisGenerationRecord.write(channel, bytes); }
        /** 路径仅用于包内区分镜像与摘要的故障边界。 */
        default void forceFile(Path path, FileChannel channel) throws IOException { channel.force(true); }
        /** 传输摘要采用 Ratis 规定的名字和格式。 */
        default void saveDigest(Path target, MD5Hash digest) throws IOException { MD5FileUtil.saveMD5File(target.toFile(), digest); }
        /** 不支持原子发布时直接失败。 */
        default void move(Path source, Path target) throws IOException { Files.move(source, target, StandardCopyOption.ATOMIC_MOVE); }
        /** 正式名称与上级目录均需持久确认。 */
        default void forceDirectory(Path directory) throws IOException { RatisGenerationRecord.forceDirectory(directory); }
    }

    /** 每次进程启动核对磁盘上的最高位置，不能依赖旧内存缓存。 */
    @Override public void init(RaftStorage storage) throws IOException {
        directory = storage.getStorageDir().getStateMachineDir().toPath(); Files.createDirectories(directory); reload();
    }

    /** Ratis 替换状态目录以后必须重新扫描，新目录异常会直接阻止安装完成。 */
    synchronized SingleFileSnapshotInfo reload() throws IOException {
        return reload(null);
    }

    /** 维护扫描与校验共用预算；正常启动仍做完整目录和内容检查。 */
    private SingleFileSnapshotInfo reload(RatisRetirementWork work) throws IOException {
        Path newest = null; TermIndex position = null; int inspected = 0;
        try (var files = Files.list(directory)) {
            var iterator = files.iterator();
            while (iterator.hasNext()) {
                Path path = iterator.next(); if (work != null) { work.scan(); } if (++inspected > (work == null ? 4096 : work.options.scanLimit())) { throw new IOException("snapshot catalog exceeds maintenance budget"); }
                if (!SimpleStateMachineStorage.SNAPSHOT_REGEX.matcher(path.getFileName().toString()).matches()) { continue; }
                if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) { throw new IOException("snapshot path is not a regular file"); }
                var candidate = SimpleStateMachineStorage.getTermIndexFromSnapshotFile(path.toFile());
                if (position == null || candidate.getIndex() > position.getIndex()) { newest = path; position = candidate; }
                else if (candidate.getIndex() == position.getIndex() && candidate.getTerm() != position.getTerm()) { throw new IOException("ambiguous snapshot term at same index"); }
            }
        }
        if (newest == null) { if (latest != null) { throw new IOException("latest snapshot disappeared"); } return null; }
        if (latest != null && position.getIndex() < latest.getIndex()) { throw new IOException("latest snapshot regressed"); }
        Path digestPath = MD5FileUtil.getDigestFileForFile(newest.toFile()).toPath(); var dataStamp = stamp(newest); var digestStamp = stamp(digestPath);
        if (latest != null && position.equals(latest.getTermIndex()) && newest.equals(latest.getFile().getPath()) && dataStamp.equals(verifiedData) && digestStamp.equals(verifiedDigest)) { return latest; }
        var verificationLimits = work == null ? limits : new WalBackupOptions(limits.maxBytes(), limits.batchKeys(), limits.batchBytes(), Math.min(limits.timeoutMillis(), work.remainingMillis()));
        RatisSnapshotBundle.inspect(newest, group, position, verificationLimits);
        var expected = MD5FileUtil.readStoredMd5ForFile(newest.toFile());
        if (expected == null || !expected.equals(md5(newest, work))) { throw new IOException("snapshot transport digest missing or mismatched"); }
        latest = new SingleFileSnapshotInfo(new FileInfo(newest, expected), position.getTerm(), position.getIndex()); verifiedData = dataStamp; verifiedDigest = digestStamp; return latest;
    }

    /** 完整文件和传输摘要均刷盘后再发布文件名，支持从独立安装副本重建状态目录。 */
    synchronized SingleFileSnapshotInfo publish(Path bundle, TermIndex position) throws IOException {
        RatisSnapshotBundle.inspect(bundle, group, position, limits);
        var previous = reload();
        if (previous != null && (position.getIndex() < previous.getIndex() || position.getIndex() == previous.getIndex() && position.getTerm() != previous.getTerm())) { throw new IOException("snapshot publication regressed or changed term at existing index"); }
        Path target = directory.resolve(SimpleStateMachineStorage.getSnapshotFileName(position.getTerm(), position.getIndex()));
        if (Files.exists(target, LinkOption.NOFOLLOW_LINKS)) {
            var current = reload();
            if (current == null || !current.getTermIndex().equals(position) || !sameContents(bundle, current.getFile().getPath())) { throw new IOException("snapshot publication collision"); }
            force(current); return current;
        }
        Path temporary = directory.resolve(".bundle-" + UUID.randomUUID());
        try (var input = FileChannel.open(bundle, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS);
             var output = FileChannel.open(temporary, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS)) {
            var buffer = ByteBuffer.allocate(65_536); long bytes = 0; long started = System.nanoTime();
            for (int read; (read = input.read(buffer)) != -1;) {
                if (read < 1 || (bytes += read) > limits.maxBytes() || System.nanoTime() - started > java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(limits.timeoutMillis())) { throw new IOException("snapshot publication exceeds budget"); }
                buffer.flip(); io.write(output, buffer); buffer.clear();
            }
            io.forceFile(temporary, output);
        }
        RatisSnapshotBundle.inspect(temporary, group, position, limits); var checksum = md5(temporary); io.saveDigest(target, checksum);
        Path digest = MD5FileUtil.getDigestFileForFile(target.toFile()).toPath();
        try (var metadata = FileChannel.open(digest, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS)) { io.forceFile(digest, metadata); }
        io.forceDirectory(directory); io.move(temporary, target); io.forceDirectory(directory); io.forceDirectory(directory.getParent());
        latest = new SingleFileSnapshotInfo(new FileInfo(target, checksum), position.getTerm(), position.getIndex()); verifiedData = stamp(target); verifiedDigest = stamp(MD5FileUtil.getDigestFileForFile(target.toFile()).toPath()); return latest;
    }

    /** Ratis 接收文件可能只关闭而未 force，应用安装前要同步文件、摘要和目录。 */
    synchronized SingleFileSnapshotInfo syncReceived() throws IOException {
        var snapshot = reload(); if (snapshot == null) { throw new IOException("received snapshot missing"); }
        force(snapshot); return snapshot;
    }

    /** 已核对文件只需再同步目录和内容，不重复读取整个大镜像。 */
    private void force(SingleFileSnapshotInfo snapshot) throws IOException {
        Path file = snapshot.getFile().getPath();
        for (Path path : java.util.List.of(file, MD5FileUtil.getDigestFileForFile(file.toFile()).toPath())) {
            try (var channel = FileChannel.open(path, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS)) { io.forceFile(path, channel); }
        }
        io.forceDirectory(directory); io.forceDirectory(directory.getParent());
    }

    /** 同一位置的正式镜像不可被另一份有效但不同的内容冒充，比较工作量有整体预算。 */
    private boolean sameContents(Path requested, Path existing) throws IOException {
        if (Files.size(requested) != Files.size(existing)) { return false; }
        long bytes = 0, began = System.nanoTime(); byte[] a = new byte[65_536], b = new byte[65_536];
        try (var first = Files.newInputStream(requested, LinkOption.NOFOLLOW_LINKS); var second = Files.newInputStream(existing, LinkOption.NOFOLLOW_LINKS)) {
            for (int count; (count = first.read(a)) != -1;) {
                if (count < 1 || (bytes += count) > limits.maxBytes() || System.nanoTime() - began > java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(limits.timeoutMillis())) { throw new IOException("snapshot collision comparison exceeds budget"); }
                int read = second.readNBytes(b, 0, count);
                if (read != count || java.util.Arrays.mismatch(a, 0, count, b, 0, read) >= 0) { return false; }
            }
            return second.read() == -1;
        }
    }

    /** 只返回已验证的持久镜像，加载失败不静默变成空快照。 */
    @Override public SnapshotInfo getLatestSnapshot() { return latest; }

    /** 传输路径相对于此目录，避免将业务根目录暴露给 Ratis 文件复制。 */
    @Override public java.io.File getSnapshotDir() { return directory == null ? null : directory.toFile(); }

    /** 本实现不格式化已有业务状态；只能接受尚无镜像的新目录。 */
    @Override public void format() throws IOException { if (latest != null) { throw new IOException("cannot format initialized snapshot storage"); } }

    /** 自动策略显式配置保留数时才清理；节点维护还会传入活动和安装边界保护。 */
    @Override public void cleanupOldSnapshots(SnapshotRetentionPolicy policy) throws IOException {
        if (policy.getNumSnapshotsRetained() <= 0) { return; }
        cleanup(new RatisRetirementWork(new RatisRetirementOptions(1, 128, 4096, 1000, Math.max(2, Math.min(32, policy.getNumSnapshotsRetained())))), java.util.Set.of());
    }

    /** 最新镜像先完整校验并同步，旧文件删除失败只留下可重试垃圾，绝不删除最新或保护边界。 */
    synchronized void cleanup(RatisRetirementWork work, java.util.Set<Long> protectedIndices) throws IOException {
        try {
            work.check(); var newest = reload(work); if (newest != null) { force(newest); }
            RatisRetirementFiles.scratch(directory, true, work); if (newest == null) { return; }
            var snapshots = new java.util.ArrayList<Path>(); var orphanDigests = new java.util.ArrayList<Path>();
            try (var entries = Files.list(directory)) { var iterator = entries.iterator(); while (iterator.hasNext()) {
                work.scan(); Path path = iterator.next(); String name = path.getFileName().toString();
                if (SimpleStateMachineStorage.SNAPSHOT_REGEX.matcher(name).matches()) { snapshots.add(path); }
                else if (SimpleStateMachineStorage.SNAPSHOT_MD5_REGEX.matcher(name).matches()) {
                    Path base = path.resolveSibling(name.substring(0, name.length() - 4));
                    if (!Files.exists(base, LinkOption.NOFOLLOW_LINKS)) { orphanDigests.add(path); }
                }
            } }
            snapshots.sort(java.util.Comparator.comparingLong((Path path) -> SimpleStateMachineStorage.getTermIndexFromSnapshotFile(path.toFile()).getIndex()).reversed());
            for (int i = work.options.retainedSnapshots(); i < snapshots.size(); i++) {
                Path path = snapshots.get(i); long index = SimpleStateMachineStorage.getTermIndexFromSnapshotFile(path.toFile()).getIndex();
                if (index == newest.getIndex() || protectedIndices.contains(index)) { continue; }
                deleteOld(path, work); deleteOld(MD5FileUtil.getDigestFileForFile(path.toFile()).toPath(), work);
            }
            for (Path path : orphanDigests) {
                String name = path.getFileName().toString(); Path base = path.resolveSibling(name.substring(0, name.length() - 4));
                if (!protectedIndices.contains(SimpleStateMachineStorage.getTermIndexFromSnapshotFile(base.toFile()).getIndex())) { deleteOld(path, work); }
            }
        } catch (RatisRetirementWork.Limit exhausted) { work.exhausted = true; }
    }

    /** 只删除列表中的固定镜像文件或摘要，链接和特殊文件拒绝。 */
    private static void deleteOld(Path path, RatisRetirementWork work) throws IOException {
        if (!Files.exists(path, LinkOption.NOFOLLOW_LINKS)) { return; }
        if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) { throw new IOException("old snapshot path is not regular"); }
        work.deleting(); Files.delete(path); work.removed++; RatisGenerationRecord.forceDirectory(path.getParent());
    }

    /** 传输 MD5 仅兼容 Ratis，业务内容另有 SHA256 和完整镜像验证。 */
    private MD5Hash md5(Path path) throws IOException {
        return md5(path, null);
    }

    /** 维护散列逐块检查共享期限，普通发布另用镜像本身的预算。 */
    private MD5Hash md5(Path path, RatisRetirementWork work) throws IOException {
        try {
            var hash = java.security.MessageDigest.getInstance("MD5"); var buffer = new byte[65_536]; long bytes = 0; long started = System.nanoTime();
            try (var input = Files.newInputStream(path, LinkOption.NOFOLLOW_LINKS)) { for (int read; (read = input.read(buffer)) != -1;) {
                if (work != null) { work.check(); }
                if (read < 1 || (bytes += read) > limits.maxBytes() || System.nanoTime() - started > java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(limits.timeoutMillis())) { throw new IOException("snapshot transport checksum exceeds budget"); } hash.update(buffer, 0, read);
            } }
            return MD5Hash.newInstance(hash.digest());
        } catch (java.security.NoSuchAlgorithmException failure) { throw new IOException(failure); }
    }

    /** 符号链接与特殊文件不能参与验证缓存；签名不包含业务内容或凭据。 */
    private static Stamp stamp(Path path) throws IOException { var attributes = Files.readAttributes(path, java.nio.file.attribute.BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS); if (!attributes.isRegularFile()) { throw new IOException("snapshot verification path is not regular"); } return new Stamp(attributes.fileKey(), attributes.size(), attributes.lastModifiedTime()); }
}
