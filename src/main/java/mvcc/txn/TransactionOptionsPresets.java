package mvcc.txn;

public final class TransactionOptionsPresets {
    private TransactionOptionsPresets() {
    }

    public static TransactionOptions mysqlLike() {
        return TransactionOptions.mysqlLikeDefaults();
    }

    public static TransactionOptions failFast() {
        return TransactionOptions.defaults();
    }
}
