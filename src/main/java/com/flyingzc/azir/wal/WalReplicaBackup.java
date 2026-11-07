package com.flyingzc.azir.wal;

import java.io.IOException;

/** 已固定本地镜像和后端读视图的复制备份，只能完成一次，期间允许源提交继续推进。 */
public final class WalReplicaBackup implements AutoCloseable {
    private final WalBackup.Prepared prepared;
    private final Runnable released;
    private boolean running, closed;
    /** 只能由持有决议和物化保护的运行时创建。 */
    WalReplicaBackup(WalBackup.Prepared prepared, Runnable released) { this.prepared = prepared; this.released = released; }
    /** 固定的业务边界不随源的新提交变化。 */
    public WalCheckpoint boundary() { return prepared.boundary(); }
    /** 在维护线程执行完整导出、验证与发布，失败不会影响已经确认的源事务。 */
    public WalBackup.Info complete() throws IOException {
        synchronized (this) { if (running || closed) { throw new IOException("replica backup already running or closed"); } running = true; }
        Throwable failure = null;
        try { return prepared.complete(); }
        catch (IOException | RuntimeException | Error problem) { failure = problem; throw problem; }
        finally {
            try { synchronized (this) { running = false; close(); } }
            catch (IOException cleanup) { if (failure == null) { throw cleanup; } failure.addSuppressed(cleanup); }
        }
    }
    /** 正在导出的视图不能被另一线程提前关闭；调用方应等待维护任务结束。 */
    @Override public synchronized void close() throws IOException {
        if (closed) { return; } if (running) { throw new IOException("replica backup export still running"); }
        try { prepared.close(); } finally { closed = true; released.run(); }
    }
}
