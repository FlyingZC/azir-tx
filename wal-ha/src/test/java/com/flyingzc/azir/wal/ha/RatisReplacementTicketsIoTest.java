package com.flyingzc.azir.wal.ha;

import org.apache.ratis.protocol.*;
import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

/** 操作者票据在真实文件系统边界失败后仍不覆盖另一操作，并能确认同一操作续做。 */
class RatisReplacementTicketsIoTest {
    @TempDir Path directory;
    /** 文件、名称和清理同步分别在操作前后注入失败。 */
    enum Failure { PARTIAL_WRITE, BEFORE_FILE_FORCE, AFTER_FILE_FORCE, BEFORE_LINK, AFTER_LINK, BEFORE_DIRECTORY_FORCE, AFTER_DIRECTORY_FORCE, BEFORE_DELETE, AFTER_DELETE }
    /** 正式文件只能不存在或完整，保存成功必定重新确认文件和目录。 */
    @ParameterizedTest @EnumSource(Failure.class)
    void uncertainPublicationCanOnlyRetryTheOriginalTicket(Failure point) throws Exception {
        var ticket = ticket(UUID.randomUUID()); Path file = directory.resolve("replacement.ticket"); var broken = new Fault(point);
        assertThrows(IOException.class, () -> RatisReplacementTickets.save(file, ticket, broken)); assertTrue(broken.fired);
        if (Files.exists(file)) { assertArrayEquals(ticket.encode(), Files.readAllBytes(file)); }
        var retry = new Fault(null); RatisReplacementTickets.save(file, ticket, retry);
        assertTrue(retry.operations.contains("FILE_FORCE")); assertTrue(retry.operations.contains("DIRECTORY_FORCE"));
        assertArrayEquals(ticket.encode(), RatisReplacementTickets.load(file, ticket.groupId()).encode());
        assertThrows(IOException.class, () -> RatisReplacementTickets.save(file, ticket(ticket.groupId())));
        assertArrayEquals(ticket.encode(), Files.readAllBytes(file));
        try (var entries = Files.list(directory)) { assertTrue(entries.noneMatch(path -> path.getFileName().toString().startsWith(".azir-ticket-stage-"))); }
    }
    /** 重试不能因正式文件已经可见而跳过再次同步；损坏、链接及错误组一律拒绝。 */
    @Test void existingTicketStillNeedsConfirmationAndOwnership() throws Exception {
        var ticket = ticket(UUID.randomUUID()); Path file = directory.resolve("ticket"); RatisReplacementTickets.save(file, ticket);
        for (var point : List.of(Failure.BEFORE_FILE_FORCE, Failure.AFTER_FILE_FORCE, Failure.BEFORE_DIRECTORY_FORCE, Failure.AFTER_DIRECTORY_FORCE)) {
            var fault = new Fault(point); assertThrows(IOException.class, () -> RatisReplacementTickets.save(file, ticket, fault)); assertTrue(fault.fired);
        }
        assertThrows(IOException.class, () -> RatisReplacementTickets.load(file, UUID.randomUUID()));
        byte[] damaged = ticket.encode(); damaged[12] ^= 1; Files.write(file, damaged); assertThrows(IOException.class, () -> RatisReplacementTickets.load(file, ticket.groupId()));
        Path user = directory.resolve("user"); Files.writeString(user, "keep"); Files.delete(file); Files.createSymbolicLink(file, user);
        assertThrows(IOException.class, () -> RatisReplacementTickets.save(file, ticket)); assertThrows(IOException.class, () -> RatisReplacementTickets.load(file, ticket.groupId())); assertEquals("keep", Files.readString(user));
        Files.delete(file); Files.write(file, new byte[16_385]); assertThrows(IOException.class, () -> RatisReplacementTickets.load(file, ticket.groupId()));
    }
    /** 真正竞争同一目标名字的不同票据只能保留获胜操作，忙者不能原地覆盖。 */
    @Test void competingWriterCannotReplaceOriginalOperation() throws Exception {
        var first = ticket(UUID.randomUUID()); var second = ticket(first.groupId()); var entered = new CountDownLatch(1); var release = new CountDownLatch(1); Path file = directory.resolve("ticket");
        try (var executor = Executors.newSingleThreadExecutor()) {
            var writer = executor.submit(() -> { RatisReplacementTickets.save(file, first, new RatisReplacementTickets.Io() {
                /** 保持文件锁直到另一线程尝试保存不同票据。 */
                @Override public void link(Path source, Path target) throws IOException { entered.countDown(); try { if (!release.await(10, TimeUnit.SECONDS)) { throw new IOException("test release timed out"); } } catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new IOException(interrupted); } RatisReplacementTickets.Io.super.link(source, target); }
            }); return null; });
            try { assertTrue(entered.await(10, TimeUnit.SECONDS)); assertThrows(IOException.class, () -> RatisReplacementTickets.save(file, second)); }
            finally { release.countDown(); }
            writer.get(10, TimeUnit.SECONDS);
        }
        assertArrayEquals(first.encode(), RatisReplacementTickets.load(file, first.groupId()).encode());
        assertThrows(IOException.class, () -> RatisReplacementTickets.save(file, second));
    }
    /** 合法票据由真实构造器产生，不绕过成员校验。 */
    private static RatisMemberReplacement ticket(UUID group) {
        var peers = new ArrayList<RaftPeer>(); for (int i = 0; i < 3; i++) { peers.add(RaftPeer.newBuilder().setId("n" + i).setAddress("localhost:" + (12300 + i)).build()); }
        UUID operation = UUID.randomUUID(); return new RatisMemberReplacement(operation, group, new RatisMembershipView(5, peers, List.of()), peers.getFirst().getId(), RaftPeer.newBuilder().setId("r-" + operation).setAddress("localhost:12310").setStartupRole(RaftPeerRole.LISTENER).build());
    }
    /** 所有副作用调用真实 I/O，一次性故障不伪造正式文件状态。 */
    static final class Fault implements RatisReplacementTickets.Io {
        final Failure failure; final List<String> operations = new ArrayList<>(); boolean fired;
        /** 无故障实例记录重试是否真的同步。 */
        Fault(Failure failure) { this.failure = failure; }
        /** 操作以后失败保留已经发生的真实效果。 */
        private void hit(Failure point) throws IOException { if (!fired && point == failure) { fired = true; throw new IOException("injected ticket IO: " + point); } }
        /** 部分写只污染私有暂存，不能影响正式名称。 */
        @Override public void write(FileChannel channel, byte[] bytes) throws IOException { if (failure == Failure.PARTIAL_WRITE) { RatisGenerationRecord.write(channel, ByteBuffer.wrap(bytes, 0, bytes.length / 2)); hit(Failure.PARTIAL_WRITE); } RatisReplacementTickets.Io.super.write(channel, bytes); }
        /** 文件同步前后均不能返回保存成功。 */
        @Override public void forceFile(FileChannel channel) throws IOException { operations.add("FILE_FORCE"); hit(Failure.BEFORE_FILE_FORCE); RatisReplacementTickets.Io.super.forceFile(channel); hit(Failure.AFTER_FILE_FORCE); }
        /** 原子链接之后出错，下一次应确认原正式文件。 */
        @Override public void link(Path source, Path target) throws IOException { hit(Failure.BEFORE_LINK); RatisReplacementTickets.Io.super.link(source, target); hit(Failure.AFTER_LINK); }
        /** 暂存清理失败不能令调用方误以为保存完成。 */
        @Override public void delete(Path file) throws IOException { hit(Failure.BEFORE_DELETE); RatisReplacementTickets.Io.super.delete(file); hit(Failure.AFTER_DELETE); }
        /** 目录同步无论在操作前后失败都需要重试。 */
        @Override public void forceDirectory(Path parent) throws IOException { operations.add("DIRECTORY_FORCE"); hit(Failure.BEFORE_DIRECTORY_FORCE); RatisReplacementTickets.Io.super.forceDirectory(parent); hit(Failure.AFTER_DIRECTORY_FORCE); }
    }
}
