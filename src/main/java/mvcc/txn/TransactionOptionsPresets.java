package mvcc.txn;

/**
 * 事务选项预设工厂。
 */
public final class TransactionOptionsPresets {
    /**
     * 构造 TransactionOptionsPresets 实例。
     */
    private TransactionOptionsPresets() {
    }

    /**
     * 返回接近 MySQL 行为的事务选项。
     */
    public static TransactionOptions mysqlLike() {
        return TransactionOptions.mysqlLikeDefaults();
    }

    /**
     * 返回失败优先的配置。
     */
    public static TransactionOptions failFast() {
        return TransactionOptions.failFastDefaults();
    }
}
