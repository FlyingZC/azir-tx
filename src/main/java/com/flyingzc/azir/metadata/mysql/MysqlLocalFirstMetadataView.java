package com.flyingzc.azir.metadata.mysql;

import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.IsolationLevel;
import com.flyingzc.azir.common.MvccException;
import com.flyingzc.azir.metadata.TransactionMetadataStore;
import com.flyingzc.azir.metadata.TxRecord;
import com.flyingzc.azir.publication.mysql.MysqlLocalFirstPublicationLedger;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/** 分片读侧只读解析本地先提交时间线的实时中心决议，写入必须走槽原子协议。 */
public final class MysqlLocalFirstMetadataView implements TransactionMetadataStore {
    private final MysqlLocalFirstPublicationLedger ledger;

    /** 绑定同一中心控制库，不缓存 ACTIVE、缺失或终态行。 */
    public MysqlLocalFirstMetadataView(MysqlLocalFirstPublicationLedger ledger) {
        this.ledger = Objects.requireNonNull(ledger, "ledger");
    }

    /** 前台预写凭据只能用于创建它的同一中心账本实例。 */
    public boolean usesLedger(MysqlLocalFirstPublicationLedger candidate) {
        return ledger == candidate;
    }

    /** 分片物理回滚必须先穿过中心事务行锁及数据库期限栅栏。 */
    public void assertIntentAbortAllowed(long txId) {
        ledger.assertIntentAbortAllowed(txId);
    }

    /** 恢复和读侧期限使用控制数据库时钟。 */
    @Override
    public long currentTimeMillis() {
        return ledger.currentDatabaseTimeMillis();
    }

    /** 每次读取真实持久状态，未决意向只被识别而不对其他事务可见。 */
    @Override
    public Optional<TxRecord> get(long txId) {
        return ledger.metadataRecord(txId);
    }

    /** 分片 GC 保留未决意向和仍需本地恢复的 COMMITTED 下界。 */
    @Override
    public Optional<Long> oldestActiveStartTs() {
        return ledger.oldestUnfinalizedStartTs();
    }

    /** 新事务只能由带快照期限的控制账本入口创建。 */
    @Override
    public TxRecord create(long txId, long startTs, IsolationLevel isolationLevel) {
        throw unsupported(txId);
    }

    /** PREPARING 不能绕开持久参与者与时间槽协议直接创建。 */
    @Override
    public TxRecord createPreparing(long txId, long startTs, IsolationLevel isolationLevel,
                                    Set<String> participants) {
        throw unsupported(txId);
    }

    /** COMMITTED 只能与同一分段槽在中心事务中原子落盘。 */
    @Override
    public TxRecord createCommitted(long txId, long startTs, IsolationLevel isolationLevel,
                                    Set<String> participants, long commitTs) {
        throw unsupported(txId);
    }

    /** 普通元数据状态改写不能替代跨分片中心决议。 */
    @Override
    public TxRecord markPreparing(long txId, Set<String> participants) {
        throw unsupported(txId);
    }

    /** 提交时间必须由新账本唯一槽确定。 */
    @Override
    public TxRecord markCommitted(long txId, long commitTs) {
        throw unsupported(txId);
    }

    /** 参与者列表不能在只读解析器上被重写。 */
    @Override
    public TxRecord markCommitted(long txId, Set<String> participants, long commitTs) {
        throw unsupported(txId);
    }

    /** 单分片直接提交使用本地候选证明，不走通用状态改写。 */
    @Override
    public TxRecord markSingleShardCommitted(long txId, String participant, long commitTs) {
        throw unsupported(txId);
    }

    /** 参与者全部物理 fencing 后才允许中心写 ABORTED。 */
    @Override
    public TxRecord markAborted(long txId) {
        throw unsupported(txId);
    }

    /** 最终化必须校验全部分片持久终态，不能用默认 no-op。 */
    @Override
    public void markFinalized(long txId) {
        throw unsupported(txId);
    }

    /** 中心 GC 只走新账本槽、元数据和分片证明的既定顺序。 */
    @Override
    public int purgeFinalized(long safePoint, long finalizedBeforeMillis, int limit) {
        throw unsupported(null);
    }

    /** 防止普通元数据 API 绕开分段原子提交和恢复栅栏。 */
    private MvccException unsupported(Long txId) {
        return new MvccException(ErrorCode.ILLEGAL_STATE,
                "local-first metadata view does not authorize direct state mutation", false, txId);
    }
}
