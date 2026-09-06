package mvcc.tso;

/**
 * 时间戳分配器接口，为 MVCC 事务提供单调递增时间戳。
 */
public interface TimestampOracle {
    /**
     * 分配下一个全局单调递增时间戳。
     */
    long nextTimestamp();
}
