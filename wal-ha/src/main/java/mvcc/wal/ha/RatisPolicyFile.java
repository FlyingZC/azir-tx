package mvcc.wal.ha;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.Arrays;
import java.util.UUID;

/** 控制策略的统一持久发布，既有正确文件也须重新同步，不能以存在冒充上次同步成功。 */
final class RatisPolicyFile {
    /** 仅允许固定协议名字及其私有暂存前缀，不接收业务路径。 */
    enum Kind {
        HEALTH("HEALTH_POLICY", ".health-"), SNAPSHOT("SNAPSHOT_POLICY", ".snapshot-policy-"), GC("GC_POLICY", ".gc-policy-"), AUTHENTICATED("AUTHENTICATED", ".authenticated-");
        final String file, scratch;
        /** 文件名与半写暂存属于同一受控策略。 */
        Kind(String file, String scratch) { this.file = file; this.scratch = scratch; }
    }
    /** 包内 I/O 注入可让真实操作前后报错，正式入口只使用真实文件系统实现。 */
    interface Io {
        /** 部分写入也不能产生可用正式记录。 */
        default void write(FileChannel channel, byte[] bytes) throws IOException { RatisGenerationRecord.write(channel, ByteBuffer.wrap(bytes)); }
        /** 新文件与既有正确文件采用相同的持久确认。 */
        default void forceFile(FileChannel channel) throws IOException { channel.force(true); }
        /** 不支持原子移动时直接失败，不能降级为部分覆盖。 */
        default void move(Path source, Path target) throws IOException { Files.move(source, target, StandardCopyOption.ATOMIC_MOVE); }
        /** 记录名与暂存清理都需要目录持久确认。 */
        default void forceDirectory(Path root) throws IOException { RatisGenerationRecord.forceDirectory(root); }
    }
    /** 正式装配不使用任何注入或全局可变故障开关。 */
    static final Io REAL = new Io() { };
    /** 工具不允许实例化。 */
    private RatisPolicyFile() { }

    /** 节点独占目录锁内调用，任何操作失败都传播，后续重试保留并重新确认完整结果。 */
    static void publish(Path root, Kind kind, byte[] bytes, Io io) throws IOException {
        if (bytes.length < 1 || bytes.length > 512) { throw new IOException("policy encoding exceeds budget"); }
        Path target = root.resolve(kind.file);
        if (Files.exists(target, LinkOption.NOFOLLOW_LINKS)) {
            verify(target, bytes);
            try (var file = FileChannel.open(target, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS)) { io.forceFile(file); }
            io.forceDirectory(root); return;
        }
        int scanned = 0, removed = 0;
        try (var entries = Files.list(root)) { var iterator = entries.iterator(); while (iterator.hasNext()) {
            if (++scanned > 4096) { throw new IOException("policy staging scan budget exhausted"); } Path path = iterator.next();
            if (!path.getFileName().toString().matches(java.util.regex.Pattern.quote(kind.scratch) + "[a-f0-9-]{36}")) { continue; }
            if (++removed > 128 || !Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) { throw new IOException("policy staging deletion budget exhausted or invalid"); }
            Files.delete(path); io.forceDirectory(root);
        } }
        Path temporary = root.resolve(kind.scratch + UUID.randomUUID());
        try (var file = FileChannel.open(temporary, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS)) { io.write(file, bytes); io.forceFile(file); }
        io.move(temporary, target); io.forceDirectory(root);
    }
    /** 已存在文件必须字节完全一致，损坏、外来配置、链接和特殊文件都不能被覆盖。 */
    private static void verify(Path target, byte[] bytes) throws IOException {
        if (!Files.isRegularFile(target, LinkOption.NOFOLLOW_LINKS) || Files.size(target) != bytes.length || !Arrays.equals(Files.readAllBytes(target), bytes)) { throw new IOException("existing policy differs from requested immutable record"); }
    }
}
