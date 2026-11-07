package com.flyingzc.azir.txn;

import com.flyingzc.azir.api.Transaction;

import java.util.Objects;
import java.util.Set;

/**
 * 事务分类器，根据参与分片和事务选项选择提交协议。
 */
public final class TransactionClassifier {
    /**
     * 按提交阶段需求分类事务。
     */
    public TransactionClassification classifyForCommit(Transaction tx, TransactionOptions options) {
        Objects.requireNonNull(tx, "tx");
        Objects.requireNonNull(options, "options");
        Set<String> participants = tx.participants();
        if (participants.isEmpty()) {
            return new TransactionClassification(TransactionProtocolType.READ_ONLY_SNAPSHOT, participants);
        }
        if (participants.size() == 1) {
            return new TransactionClassification(TransactionProtocolType.SINGLE_SHARD_1PC, participants);
        }
        if (options.crossShardCommitPolicy().asyncCommit()) {
            return new TransactionClassification(TransactionProtocolType.CROSS_SHARD_ASYNC_COMMIT, participants);
        }
        return new TransactionClassification(TransactionProtocolType.CROSS_SHARD_SYNC_2PC, participants);
    }
}
