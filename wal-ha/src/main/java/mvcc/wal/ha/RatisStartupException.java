package mvcc.wal.ha;

import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;

/** 构造失败也保留仅清理句柄，不能把未完成释放的节点或后端当成已经关闭。 */
public class RatisStartupException extends IOException {
    private Closeable pending;

    /** 只暴露清理能力，不允许调用未完成构造对象的事务入口。 */
    public RatisStartupException(Throwable failure, Closeable pending) {
        super("startup failed; resource cleanup remains pending", failure);
        this.pending = Objects.requireNonNull(pending);
    }

    /** 清理成功后才清空句柄，失败或中断仍可沿原阶段重试。 */
    public synchronized void retryCleanup() throws IOException {
        if (pending != null) { pending.close(); pending = null; }
    }

    /** 控制池或同目录重开必须等待清理真正完成。 */
    public synchronized boolean cleanupPending() { return pending != null; }

    /** 合并内层未完成清理与外层自有资源，不跳过内层依赖或覆盖原始启动异常。 */
    static IOException afterFailure(Throwable failure, Closeable release) {
        Closeable cleanup = failure instanceof RatisStartupException nested
                ? new RatisCloseSequence(nested::retryCleanup, release) : release;
        try { cleanup.close(); }
        catch (Throwable blocked) {
            var pending = new RatisStartupException(failure, cleanup);
            if (blocked != failure) { pending.addSuppressed(blocked); }
            return pending;
        }
        if (failure instanceof IOException io) { return io; }
        if (failure instanceof RuntimeException runtime) { throw runtime; }
        if (failure instanceof Error error) { throw error; }
        return new IOException("startup failed after resource cleanup", failure);
    }
}
