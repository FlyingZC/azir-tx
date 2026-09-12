package mvcc.txn;

/**
 * 事务提交协议类型，描述提交阶段应走的协议路径。
 */
public enum TransactionProtocolType {
    /**
     * 只读快照事务路径。
     */
    READ_ONLY_SNAPSHOT,

    /**
     * 单分片一阶段提交路径。
     */
    SINGLE_SHARD_1PC,

    /**
     * 跨分片异步提交路径。
     */
    CROSS_SHARD_ASYNC_COMMIT,

    /**
     * 跨分片同步两阶段提交路径。
     */
    CROSS_SHARD_SYNC_2PC,

    /**
     * 热点键串行化提交路径。
     */
    HOT_KEY_SERIALIZED,

    /**
     * 大批量写入提交路径。
     */
    LARGE_WRITE_BATCH
}
