package com.flyingzc.azir.control;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.util.Objects;

/** 协议命名锁的会话边界；加锁或解锁回复不确定时销毁会话，不能带锁归还连接池。 */
public final class MysqlNamedLock {
    /** 禁止实例化命名锁工具。 */
    private MysqlNamedLock() { }

    /** 同一执行作用域的持锁资源，资源语法自动把释放失败附加到原始异常。 */
    public static final class Lease implements AutoCloseable {
        private final Connection connection;
        private final String name;
        private boolean closed;
        /** 只由成功取得数据库锁的工厂建立，不能伪造持锁资源。 */
        private Lease(Connection connection, String name) { this.connection = connection; this.name = name; }
        /** 同一作用域重复关闭无副作用，失败释放已由底层销毁会话。 */
        @Override public void close() throws SQLException {
            if (!closed) { closed = true; release(connection, name); }
        }
    }

    /** 建立可用于 try-with-resources 的锁，持锁资源分配失败也不能遗留数据库锁。 */
    public static Lease lock(Connection connection, String name, int waitSeconds) throws SQLException {
        try { acquire(connection, name, waitSeconds); return new Lease(connection, name); }
        catch (SQLException | RuntimeException | Error failure) { discard(connection, failure); throw failure; }
    }

    /** 只允许有限等待，任何失败都视为服务端可能已经持锁，不猜测网络请求是否到达。 */
    public static void acquire(Connection connection, String name, int waitSeconds) throws SQLException {
        requireName(name); if (waitSeconds < 0 || waitSeconds > 3600) { throw new IllegalArgumentException("invalid named lock wait budget"); }
        try (var q = connection.prepareStatement("select get_lock(?,?)")) {
            if (!connection.getAutoCommit()) { throw new SQLException("protocol authority lock requires auto-commit"); }
            q.setString(1, name); q.setInt(2, waitSeconds);
            try (var r = q.executeQuery()) {
                if (!r.next()) { throw new SQLException("protocol named lock result missing"); }
                int result = r.getInt(1); if (r.wasNull()) { throw new SQLException("protocol named lock acquisition failed"); }
                if (result != 1) { throw new SQLTimeoutException("protocol named lock wait exhausted", "HYT00"); }
            }
        } catch (SQLException | RuntimeException | Error failure) { discard(connection, failure); throw failure; }
    }

    /** 只接受本会话成功释放，空结果和非所有者都必须销毁会话，不能继续复用其余锁。 */
    public static void release(Connection connection, String name) throws SQLException {
        requireName(name);
        try (var q = connection.prepareStatement("select release_lock(?)")) {
            q.setString(1, name); try (var r = q.executeQuery()) {
                if (!r.next() || r.getInt(1) != 1 || r.wasNull()) { throw new SQLException("protocol named lock ownership lost"); }
            }
        } catch (SQLException | RuntimeException | Error failure) { discard(connection, failure); throw failure; }
    }

    /** 销毁原始会话并保留清理错误，连接关闭由持有者的 finally 或资源作用域完成。 */
    public static void discard(Connection connection, Throwable original) {
        try { connection.abort(Runnable::run); }
        catch (SQLException | RuntimeException | Error cleanup) { if (cleanup != original) { original.addSuppressed(cleanup); } }
    }

    /** 协议锁名以固定前缀和 UUID 构造，拒绝无效长度而不在异常中输出具体身份。 */
    private static void requireName(String name) {
        Objects.requireNonNull(name); if (name.isEmpty() || name.length() > 64) { throw new IllegalArgumentException("invalid named lock name"); }
    }
}
