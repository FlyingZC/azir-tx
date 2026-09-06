package mvcc.gc;

/**
 * 安全点存储接口，定义 GC 可清理版本的时间戳边界。
 */
public interface SafePointStore {
    /**
     * 返回当前安全点时间戳。
     */
    long currentSafePoint();

    /**
     * 尝试将安全点推进到候选时间戳。
     */
    long advanceTo(long candidateSafePoint);
}
