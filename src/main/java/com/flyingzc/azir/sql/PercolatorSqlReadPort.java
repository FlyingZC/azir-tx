package com.flyingzc.azir.sql;

import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.MvccException;
import com.flyingzc.azir.percolator.PercolatorTransaction;
import com.flyingzc.azir.storage.KeyRange;
import java.util.Optional;

/** Calcite 与独立 Percolator 内核之间的同语句点读、分页与缓冲写入端口。 */
final class PercolatorSqlReadPort implements SqlTransactionPort {
    private final PercolatorTransaction transaction;
    private final PercolatorTransaction.ReadView statement;

    /** 每条 SQL 只分配一次 ReadView，JOIN 重扫和分页不会刷新 RC 时间戳。 */
    PercolatorSqlReadPort(PercolatorTransaction transaction) {
        this.transaction = java.util.Objects.requireNonNull(transaction, "transaction");
        statement = transaction.statement();
    }

    /** 使用原 startTs 进行诊断，不改变 primary 的决议职责。 */
    @Override
    public long diagnosticTransactionId() { return transaction.startTs(); }

    /** 点查复用语句固定边界并覆盖本事务缓冲写。 */
    @Override
    public Optional<byte[]> get(String shardId, String key) { return statement.get(shardId, key); }

    /** 不可见 guard 和墓碑仍参与扫描预算；游标来自原始候选键。 */
    @Override
    public Page scanPage(String shardId, KeyRange range, String afterKeyExclusive, int limit) {
        var page = statement.scanPage(shardId, range, afterKeyExclusive, limit);
        return new Page(page.entries().stream().map(entry -> {
            byte[] value = entry.value();
            return new KeyValue(entry.key(), value, value == null);
        }).toList(), page.nextKey());
    }

    /** 插入时先按语句边界查重，提交时仍由 Percolator 写冲突检测防止并发覆盖。 */
    @Override
    public void insert(String shardId, String key, byte[] value) {
        if (get(shardId, key).isPresent()) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE, "duplicate SQL key: " + key, false, transaction.startTs());
        }
        put(shardId, key, value);
    }

    /** 缓冲 SQL 非空值，直至事务提交阶段才进行协议预写。 */
    @Override
    public void put(String shardId, String key, byte[] value) { transaction.put(shardId, key, value); }

    /** 删除在提交时形成墓碑版本，保持旧 RR 快照。 */
    @Override
    public void delete(String shardId, String key) { transaction.delete(shardId, key); }
}
