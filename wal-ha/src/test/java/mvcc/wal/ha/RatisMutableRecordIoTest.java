package mvcc.wal.ha;

import mvcc.wal.WalReplicaIdentity;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Stream;
import static org.junit.jupiter.api.Assertions.*;

/** 活动指针与退休进度的真实部分写、原子替换、删除和目录确认故障。 */
class RatisMutableRecordIoTest {
    @TempDir Path directory;
    /** 两类记录保持各自原有格式，不能用一次测试替代另一编码。 */
    enum Kind { GENERATION, RETIREMENT }
    /** 操作前后异常均表示失败，真实副作用可能已经发生。 */
    enum Failure { PARTIAL_WRITE, BEFORE_FILE_FORCE, AFTER_FILE_FORCE, BEFORE_REPLACE, AFTER_REPLACE, BEFORE_DELETE, AFTER_DELETE, BEFORE_DIRECTORY_FORCE, AFTER_DIRECTORY_FORCE }
    /** 替换边界覆盖旧记录和新记录的完整性。 */
    static Stream<Object[]> publications() { return Arrays.stream(Kind.values()).flatMap(kind -> Stream.of(Failure.PARTIAL_WRITE, Failure.BEFORE_FILE_FORCE, Failure.AFTER_FILE_FORCE, Failure.BEFORE_REPLACE, Failure.AFTER_REPLACE, Failure.BEFORE_DIRECTORY_FORCE, Failure.AFTER_DIRECTORY_FORCE).map(failure -> new Object[]{kind, failure})); }
    /** 删除边界保留重试目录同步的必要性。 */
    static Stream<Object[]> removals() { return Arrays.stream(Kind.values()).flatMap(kind -> Stream.of(Failure.BEFORE_DELETE, Failure.AFTER_DELETE, Failure.BEFORE_DIRECTORY_FORCE, Failure.AFTER_DIRECTORY_FORCE).map(failure -> new Object[]{kind, failure})); }

    /** 任意失败后正式记录仍是完整旧值或新值，重试必须确认目标新值。 */
    @ParameterizedTest @MethodSource("publications")
    void atomicReplacementNeverExposesPartialRecord(Kind kind, Failure failure) throws Exception {
        var owner = owner(); Path target = directory.resolve(kind == Kind.GENERATION ? "ACTIVE" : "RETIRING"); publish(kind, target, owner, false, new Fault(null));
        var broken = new Fault(failure); assertThrows(IOException.class, () -> publish(kind, target, owner, true, broken)); assertTrue(broken.fired);
        assertEquals(failure == Failure.AFTER_REPLACE || failure == Failure.BEFORE_DIRECTORY_FORCE || failure == Failure.AFTER_DIRECTORY_FORCE, isNew(kind, target));
        var retry = new Fault(null); publish(kind, target, owner, true, retry); assertTrue(isNew(kind, target)); assertTrue(retry.operations.contains("FILE_FORCE")); assertTrue(retry.operations.contains("DIRECTORY_FORCE"));
    }
    /** 已经删除的记录也必须再次同步目录，第二次同步失败不能被不存在短路。 */
    @ParameterizedTest @MethodSource("removals")
    void uncertainDeletionStillRequiresDirectorySyncOnRetry(Kind kind, Failure failure) throws Exception {
        Path target = directory.resolve(kind == Kind.GENERATION ? "INSTALLING" : "RETIRING"); publish(kind, target, owner(), false, new Fault(null));
        var broken = new Fault(failure); assertThrows(IOException.class, () -> RatisGenerationRecord.remove(target, broken)); assertTrue(broken.fired);
        assertEquals(failure == Failure.BEFORE_DELETE, Files.exists(target));
        var another = new Fault(Failure.BEFORE_DIRECTORY_FORCE); assertThrows(IOException.class, () -> RatisGenerationRecord.remove(target, another)); assertTrue(another.fired); assertFalse(Files.exists(target));
        var retry = new Fault(null); RatisGenerationRecord.remove(target, retry); assertTrue(retry.operations.contains("DIRECTORY_FORCE"));
    }
    /** 可变记录也不能把链接指向的用户文件当作替换或删除目标。 */
    @Test void linksCannotRedirectRecordUpdateOrDeletion() throws Exception {
        Path user = directory.resolve("user-note"), target = directory.resolve("ACTIVE"); Files.writeString(user, "keep"); Files.createSymbolicLink(target, user);
        assertThrows(IOException.class, () -> publish(Kind.GENERATION, target, owner(), true, new Fault(null))); assertThrows(IOException.class, () -> RatisGenerationRecord.remove(target)); assertEquals("keep", Files.readString(user)); assertTrue(Files.isSymbolicLink(target));
    }
    /** 本次临时目录唯一身份。 */
    private static WalReplicaIdentity owner() { UUID group = UUID.randomUUID(); return new WalReplicaIdentity(group, group, "n0"); }
    /** 正式记录编码与发布路径决定数据，测试不手造正式字节。 */
    private static void publish(Kind kind, Path target, WalReplicaIdentity owner, boolean newer, RatisRecordFile.Io io) throws IOException {
        var record = new RatisGenerationRecord(owner, new UUID(1, 2), 3, newer && kind == Kind.GENERATION ? 11 : 10, newer && kind == Kind.GENERATION ? 2 : 1, (newer && kind == Kind.GENERATION ? "b" : "a").repeat(64));
        if (kind == Kind.GENERATION) { record.publish(target, io); } else { new RatisRetirementRecord(record, newer ? "done" : "", newer).publish(target, io); }
    }
    /** 正式解码同时校验所有权块及记录 CRC，半记录必然导致断言失败。 */
    private static boolean isNew(Kind kind, Path target) throws IOException { return kind == Kind.GENERATION ? RatisGenerationRecord.read(target).sequence() == 2 : RatisRetirementRecord.read(target).backendDone(); }

    /** 一次性故障包装真实 I/O，不用虚构文件状态模拟重命名。 */
    static final class Fault implements RatisRecordFile.Io {
        final Failure failure; final List<String> operations = new ArrayList<>(); boolean fired;
        /** 无故障对象用于观测确实发生了重新同步。 */
        Fault(Failure failure) { this.failure = failure; }
        /** 操作以后报错保留真实已经发生的状态。 */
        private void hit(Failure point) throws IOException { if (!fired && point == failure) { fired = true; throw new IOException("injected mutable record IO: " + point); } }
        /** 私有暂存写入一半后停止，正式旧文件仍完整。 */
        @Override public void write(FileChannel channel, byte[] bytes) throws IOException { operations.add("WRITE"); if (failure == Failure.PARTIAL_WRITE) { RatisGenerationRecord.write(channel, ByteBuffer.wrap(bytes, 0, bytes.length / 2)); hit(Failure.PARTIAL_WRITE); } RatisRecordFile.Io.super.write(channel, bytes); }
        /** 文件同步前后分别覆盖不同不确定结果。 */
        @Override public void forceFile(FileChannel channel) throws IOException { operations.add("FILE_FORCE"); hit(Failure.BEFORE_FILE_FORCE); RatisRecordFile.Io.super.forceFile(channel); hit(Failure.AFTER_FILE_FORCE); }
        /** 真实原子替换后异常不得把新记录当作未发生。 */
        @Override public void replace(Path source, Path target) throws IOException { operations.add("REPLACE"); hit(Failure.BEFORE_REPLACE); RatisRecordFile.Io.super.replace(source, target); hit(Failure.AFTER_REPLACE); }
        /** 真实删除以后抛错，下一次必须观察并确认其目录项。 */
        @Override public void delete(Path target) throws IOException { operations.add("DELETE"); hit(Failure.BEFORE_DELETE); RatisRecordFile.Io.super.delete(target); hit(Failure.AFTER_DELETE); }
        /** 已无文件时仍能注入第二次目录同步失败。 */
        @Override public void forceDirectory(Path parent) throws IOException { operations.add("DIRECTORY_FORCE"); hit(Failure.BEFORE_DIRECTORY_FORCE); RatisRecordFile.Io.super.forceDirectory(parent); hit(Failure.AFTER_DIRECTORY_FORCE); }
    }
}
