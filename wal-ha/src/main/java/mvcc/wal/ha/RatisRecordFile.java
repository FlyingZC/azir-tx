package mvcc.wal.ha;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.UUID;

/** 安装指针及退休日志的真实原子替换与可重试删除，不把文件不存在当作目录已同步。 */
final class RatisRecordFile {
    /** 包内 I/O 注入允许在真实操作前后失败，正式路径没有全局可变故障开关。 */
    interface Io {
        /** 部分写入只能污染私有暂存。 */
        default void write(FileChannel channel, byte[] bytes) throws IOException { RatisGenerationRecord.write(channel, ByteBuffer.wrap(bytes)); }
        /** 完整内容必须先于正式名称持久化。 */
        default void forceFile(FileChannel channel) throws IOException { channel.force(true); }
        /** 始终原子替换完整旧记录，不允许降级为原地覆盖。 */
        default void replace(Path source, Path target) throws IOException { Files.move(source, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING); }
        /** 已删除的记录也允许重试，后续仍必须同步父目录。 */
        default void delete(Path target) throws IOException { Files.deleteIfExists(target); }
        /** 确认替换或删除的目录项。 */
        default void forceDirectory(Path parent) throws IOException { RatisGenerationRecord.forceDirectory(parent); }
    }
    /** 正常入口全部使用真实文件系统。 */
    static final Io REAL = new Io() { };
    /** 工具不能实例化。 */
    private RatisRecordFile() { }
    /** 调用者持有代次锁，编码与名字范围由固定协议记录决定。 */
    static void publish(Path target, byte[] bytes, String prefix, int maximum, Io io) throws IOException {
        if (bytes.length < 1 || bytes.length > maximum) { throw new IOException("mutable record exceeds encoding budget"); }
        if (Files.exists(target, LinkOption.NOFOLLOW_LINKS) && !Files.isRegularFile(target, LinkOption.NOFOLLOW_LINKS)) { throw new IOException("mutable record target is not regular"); }
        Path temporary = target.resolveSibling(prefix + UUID.randomUUID());
        try (var channel = FileChannel.open(temporary, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS)) { io.write(channel, bytes); io.forceFile(channel); }
        io.replace(temporary, target); io.forceDirectory(target.getParent());
    }
    /** 上次删除已发生但同步失败时，重试仍执行目录同步，不能只因记录消失就成功。 */
    static void remove(Path target, Io io) throws IOException {
        if (Files.exists(target, LinkOption.NOFOLLOW_LINKS) && !Files.isRegularFile(target, LinkOption.NOFOLLOW_LINKS)) { throw new IOException("mutable record deletion target is not regular"); }
        io.delete(target); io.forceDirectory(target.getParent());
    }

    /** 已有正确记录也要重新确认内容与目录，供破坏性回收前建立持久依据。 */
    static void confirm(Path target, byte[] expected, Io io) throws IOException {
        if (!Files.isRegularFile(target, LinkOption.NOFOLLOW_LINKS) || Files.size(target) != expected.length || !java.util.Arrays.equals(Files.readAllBytes(target), expected)) { throw new IOException("mutable record changed before durability confirmation"); }
        try (var channel = FileChannel.open(target, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS)) { io.forceFile(channel); }
        io.forceDirectory(target.getParent());
    }
}
