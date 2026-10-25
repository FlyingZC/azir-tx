package mvcc.wal.ha;

import mvcc.wal.WalReplicaIdentity;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Stream;
import static org.junit.jupiter.api.Assertions.*;

/** 三类控制策略的真实部分写、同步和重命名不确定结果，不能以文件存在跳过重试同步。 */
class RatisPolicyIoTest {
    @TempDir Path directory;
    /** 故障分别发生于操作以前或真实操作完成以后，后者不能推断操作没执行。 */
    enum Failure { PARTIAL_WRITE, BEFORE_FILE_FORCE, AFTER_FILE_FORCE, BEFORE_MOVE, AFTER_MOVE, BEFORE_DIRECTORY_FORCE, AFTER_DIRECTORY_FORCE }
    /** 对全部策略和全部 I/O 边界执行同样的外部不变量。 */
    static Stream<Object[]> cases() { return Stream.of(RatisPolicyFile.Kind.HEALTH, RatisPolicyFile.Kind.SNAPSHOT, RatisPolicyFile.Kind.GC).flatMap(kind -> Arrays.stream(Failure.values()).map(failure -> new Object[]{kind, failure})); }

    /** 无完整正式记录时安全清理私有半写输入，已有完整记录时重试必须再次同步而不改内容。 */
    @ParameterizedTest @MethodSource("cases")
    void failuresNeverBecomeSuccessUntilRealFileAndDirectorySync(RatisPolicyFile.Kind kind, Failure failure) throws Exception {
        var owner = owner(); var broken = new Fault(failure); Files.writeString(directory.resolve("user-note"), "keep");
        assertThrows(IOException.class, () -> publish(kind, owner, broken)); assertTrue(broken.fired);
        Path target = directory.resolve(kind.file); boolean present = Files.exists(target); byte[] before = present ? Files.readAllBytes(target) : null;
        Object inode = present ? Files.readAttributes(target, java.nio.file.attribute.BasicFileAttributes.class).fileKey() : null;
        assertEquals(failure == Failure.AFTER_MOVE || failure == Failure.BEFORE_DIRECTORY_FORCE || failure == Failure.AFTER_DIRECTORY_FORCE, present);
        assertEquals(present, loaded(kind, owner));
        var retry = new Fault(null); publish(kind, owner, retry);
        assertTrue(retry.operations.contains("FILE_FORCE")); assertTrue(retry.operations.contains("DIRECTORY_FORCE")); assertTrue(loaded(kind, owner));
        if (present) { assertArrayEquals(before, Files.readAllBytes(target)); assertEquals(inode, Files.readAttributes(target, java.nio.file.attribute.BasicFileAttributes.class).fileKey()); assertFalse(retry.operations.contains("WRITE")); assertFalse(retry.operations.contains("MOVE")); }
        else { assertTrue(retry.operations.contains("WRITE")); assertTrue(retry.operations.contains("MOVE")); }
        assertEquals("keep", Files.readString(directory.resolve("user-note")));
        try (var files = Files.list(directory)) { assertFalse(files.anyMatch(path -> path.getFileName().toString().startsWith(kind.scratch))); }
    }

    /** 第一次重命名已发生、同步失败后，第二次同步仍失败不能因完整文件存在就返回成功。 */
    @ParameterizedTest @org.junit.jupiter.params.provider.EnumSource(value = RatisPolicyFile.Kind.class, names = {"HEALTH", "SNAPSHOT", "GC"})
    void uncertainExistingRecordMustNotBypassSecondSyncFailure(RatisPolicyFile.Kind kind) throws Exception {
        var owner = owner(); assertThrows(IOException.class, () -> publish(kind, owner, new Fault(Failure.BEFORE_DIRECTORY_FORCE))); assertTrue(loaded(kind, owner));
        var retry = new Fault(Failure.BEFORE_FILE_FORCE); assertThrows(IOException.class, () -> publish(kind, owner, retry)); assertTrue(retry.fired); assertTrue(loaded(kind, owner));
        publish(kind, owner, new Fault(null));
    }
    /** 策略身份均属于当前新建临时目录。 */
    private static WalReplicaIdentity owner() { UUID group = UUID.randomUUID(); return new WalReplicaIdentity(group, group, "node"); }
    /** 通过实际记录类编码，不使用独立仿造的策略格式。 */
    private void publish(RatisPolicyFile.Kind kind, WalReplicaIdentity owner, RatisPolicyFile.Io io) throws IOException {
        switch (kind) {
            case HEALTH -> new RatisHealthRecord(owner, RatisHealthOptions.defaults()).publish(directory, io);
            case SNAPSHOT -> new RatisSnapshotPolicy(owner, RatisSnapshotOptions.defaults()).publish(directory, io);
            case GC -> new RatisGcPolicy(owner, RatisGcOptions.defaults()).publish(directory, io);
            default -> throw new IllegalArgumentException("not a maintenance policy");
        }
    }
    /** 正式解码验证 CRC、身份、预算以及完整记录。 */
    private boolean loaded(RatisPolicyFile.Kind kind, WalReplicaIdentity owner) throws IOException {
        return switch (kind) {
            case HEALTH -> new RatisHealthRecord(owner, RatisHealthOptions.defaults()).equals(RatisHealthRecord.load(directory, owner));
            case SNAPSHOT -> new RatisSnapshotPolicy(owner, RatisSnapshotOptions.defaults()).equals(RatisSnapshotPolicy.load(directory, owner));
            case GC -> new RatisGcPolicy(owner, RatisGcOptions.defaults()).equals(RatisGcPolicy.load(directory, owner));
            default -> throw new IllegalArgumentException("not a maintenance policy");
        };
    }
    /** 单次故障包装真实文件系统，重试动作记录独立于内部策略判断。 */
    static final class Fault implements RatisPolicyFile.Io {
        final Failure failure; final List<String> operations = new ArrayList<>(); boolean fired;
        /** 无故障实例用于证明重试确实执行文件和目录同步。 */
        Fault(Failure failure) { this.failure = failure; }
        /** 精确一次故障，操作以后报错保留真实已经发生的效果。 */
        private void hit(Failure point) throws IOException { if (!fired && failure == point) { fired = true; throw new IOException("injected policy IO failure: " + point); } }
        /** 写出一半真实字节再报错，正式文件仍应不存在。 */
        @Override public void write(FileChannel channel, byte[] bytes) throws IOException { operations.add("WRITE"); if (failure == Failure.PARTIAL_WRITE) { RatisGenerationRecord.write(channel, ByteBuffer.wrap(bytes, 0, bytes.length / 2)); hit(Failure.PARTIAL_WRITE); } RatisPolicyFile.Io.super.write(channel, bytes); }
        /** 前后失败分别覆盖未同步和同步结果不确定的调用返回。 */
        @Override public void forceFile(FileChannel channel) throws IOException { operations.add("FILE_FORCE"); hit(Failure.BEFORE_FILE_FORCE); RatisPolicyFile.Io.super.forceFile(channel); hit(Failure.AFTER_FILE_FORCE); }
        /** 原子重命名真实执行，事后异常不能回滚或删除完整正式记录。 */
        @Override public void move(Path source, Path target) throws IOException { operations.add("MOVE"); hit(Failure.BEFORE_MOVE); RatisPolicyFile.Io.super.move(source, target); hit(Failure.AFTER_MOVE); }
        /** 私有暂存清理和正式发布都使用同一实际目录同步。 */
        @Override public void forceDirectory(Path root) throws IOException { operations.add("DIRECTORY_FORCE"); hit(Failure.BEFORE_DIRECTORY_FORCE); RatisPolicyFile.Io.super.forceDirectory(root); hit(Failure.AFTER_DIRECTORY_FORCE); }
    }
}
