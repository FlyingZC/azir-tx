package mvcc.wal.ha;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/** 操作者票据的不可覆盖保存，未知发布结果重试仍重新确认，文件不包含后端凭据。 */
public final class RatisReplacementTickets {
    private static final Set<Path> OWNED = ConcurrentHashMap.newKeySet();
    private static final int MAXIMUM = 16_384;
    /** 原子硬链接只创建正式名称，既有任意文件绝不被替换。 */
    interface Io extends RatisRecordFile.Io {
        /** 暂存与正式文件在同一目录，原子链接不支持时直接失败。 */
        default void link(Path source, Path target) throws IOException { Files.createLink(target, source); }
    }
    private static final Io REAL = new Io() { };
    /** 工具不能实例化。 */
    private RatisReplacementTickets() { }
    /** 保存成功以后才可开始成员变更；相同票据重复保存会重新同步。 */
    public static void save(Path file, RatisMemberReplacement ticket) throws IOException { save(file, ticket, REAL); }
    /** 包内故障注入与生产保存共用真实文件系统动作。 */
    static void save(Path file, RatisMemberReplacement ticket, Io io) throws IOException {
        Objects.requireNonNull(ticket); byte[] bytes = ticket.encode(); Path target = target(file);
        locked(target, () -> {
            if (Files.exists(target, LinkOption.NOFOLLOW_LINKS)) {
                read(target, ticket.groupId()); RatisRecordFile.confirm(target, bytes, io); clean(target, io); return null;
            }
            clean(target, io); Path temporary = target.resolveSibling(prefix(target) + UUID.randomUUID());
            try (var channel = FileChannel.open(temporary, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS)) { io.write(channel, bytes); io.forceFile(channel); }
            io.link(temporary, target); io.forceDirectory(target.getParent()); io.delete(temporary); io.forceDirectory(target.getParent()); return null;
        });
    }
    /** 续做以前校验组身份并确认文件及目录，不能依据可见但尚未确认的名称执行管理变更。 */
    public static RatisMemberReplacement load(Path file, UUID expectedGroup) throws IOException {
        Path target = target(file); return locked(target, () -> { var ticket = read(target, expectedGroup); RatisRecordFile.confirm(target, ticket.encode(), REAL); return ticket; });
    }
    /** 只接受有界普通文件，链接、损坏与外来组不触发新操作。 */
    private static RatisMemberReplacement read(Path file, UUID group) throws IOException {
        Objects.requireNonNull(group); if (!Files.isRegularFile(file, LinkOption.NOFOLLOW_LINKS) || Files.size(file) > MAXIMUM) { throw new IOException("replacement ticket missing or exceeds budget"); }
        var ticket = RatisMemberReplacement.decode(Files.readAllBytes(file)); if (!ticket.groupId().equals(group)) { throw new IOException("replacement ticket belongs to another group"); } return ticket;
    }
    /** 明确父目录必须存在，不自动创建或接管任意业务路径。 */
    private static Path target(Path file) throws IOException {
        Path absolute = Objects.requireNonNull(file).toAbsolutePath().normalize(); if (absolute.getParent() == null) { throw new IOException("explicit ticket file required"); }
        Path parent = absolute.getParent().toRealPath(); return parent.resolve(absolute.getFileName());
    }
    /** 同目录同名字只有一个协作写者，锁文件不删除，避免更换 inode 造成分裂锁。 */
    private static <T> T locked(Path target, Operation<T> body) throws IOException {
        Path lockFile = target.resolveSibling(".azir-ticket-lock-" + key(target));
        if (!OWNED.add(target)) { throw new IOException("replacement ticket already in use"); }
        try (var channel = FileChannel.open(lockFile, StandardOpenOption.CREATE, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS)) {
            try (var lock = channel.tryLock()) { if (lock == null) { throw new IOException("replacement ticket owned by another process"); } return body.run(); }
            catch (java.nio.channels.OverlappingFileLockException busy) { throw new IOException("replacement ticket lock busy", busy); }
        } finally { OWNED.remove(target); }
    }
    /** 仅清理当前票据名字对应的私有半写文件，有固定扫描与删除预算。 */
    private static void clean(Path target, Io io) throws IOException {
        int scanned = 0, removed = 0; String prefix = prefix(target);
        try (var entries = Files.list(target.getParent())) { var iterator = entries.iterator(); while (iterator.hasNext()) {
            if (++scanned > 4096) { throw new IOException("ticket staging scan budget exhausted"); } Path file = iterator.next();
            if (!file.getFileName().toString().matches(java.util.regex.Pattern.quote(prefix) + "[a-f0-9-]{36}")) { continue; }
            if (++removed > 128 || !Files.isRegularFile(file, LinkOption.NOFOLLOW_LINKS)) { throw new IOException("ticket staging invalid or exceeds budget"); }
            io.delete(file); io.forceDirectory(target.getParent());
        } }
    }
    /** 不把用户文件名直接当作私有暂存模式。 */
    private static String prefix(Path target) throws IOException { return ".azir-ticket-stage-" + key(target) + "-"; }
    /** 规范名字摘要只用于同目录锁与暂存归属，不作为认证凭据。 */
    private static String key(Path target) throws IOException { try { return HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(target.getFileName().toString().getBytes(StandardCharsets.UTF_8))); } catch (java.security.NoSuchAlgorithmException unavailable) { throw new IOException(unavailable); } }
    /** 受同一文件锁保护的有限操作。 */
    @FunctionalInterface private interface Operation<T> {
        /** 异常传播，不能把部分保存改为成功。 */
        T run() throws IOException;
    }
}
