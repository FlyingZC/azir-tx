package com.flyingzc.azir.txn;

import com.flyingzc.azir.api.Transaction;

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
