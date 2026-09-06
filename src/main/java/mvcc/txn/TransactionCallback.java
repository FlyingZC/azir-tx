package mvcc.txn;

import mvcc.api.Transaction;

/**
 * 事务回调接口。
 */
@FunctionalInterface
public interface TransactionCallback<T> {
    /**
     * 在事务中执行用户回调。
     */
    T doInTransaction(Transaction tx);
}
