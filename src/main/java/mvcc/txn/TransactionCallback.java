package mvcc.txn;

import mvcc.api.Transaction;

@FunctionalInterface
public interface TransactionCallback<T> {
    T doInTransaction(Transaction tx);
}
