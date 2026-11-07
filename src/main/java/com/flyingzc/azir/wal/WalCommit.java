package com.flyingzc.azir.wal;

import java.util.List;
import java.util.Objects;
import java.util.UUID;

/** 完整事务日志项，所有分片变更属于同一个提交序号。 */
public record WalCommit(UUID transactionId, long sequence, long startSequence, List<WalMutation> mutations) {
    /** 固定写集并校验提交顺序。 */
    public WalCommit {
        Objects.requireNonNull(transactionId, "transactionId");
        mutations = List.copyOf(mutations);
        if (sequence < 1 || startSequence < 0 || startSequence >= sequence || mutations.isEmpty()) {
            throw new IllegalArgumentException("invalid commit record");
        }
        var keys = new java.util.HashSet<WalMvccRuntime.Key>();
        if (mutations.size() > 100_000 || mutations.stream().anyMatch(mutation ->
                !keys.add(new WalMvccRuntime.Key(mutation.shard(), mutation.key())))) {
            throw new IllegalArgumentException("duplicate or excessive mutation keys");
        }
    }

    /** 返回稳定内容摘要，用于数据库检查点与重放核对。 */
    public String digest() { return WalCodec.digest(WalCodec.encode(this)); }
}
