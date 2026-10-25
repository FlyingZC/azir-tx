package mvcc.wal.ha;

import mvcc.common.ErrorCode;
import mvcc.wal.*;
import java.util.Optional;
import java.util.UUID;

/** 不迁移、不盲重试的事务会话，未知结果保留原 UUID 且禁止假回滚。 */
public final class RatisTransaction implements AutoCloseable {
    /** 会话的公开结果状态，ABANDONED 表示未提交写集的本地清理未完成。 */
    public enum State { ACTIVE, DECIDING, COMMITTED, ROLLED_BACK, UNKNOWN, ABANDONED }
    /** 业务回调仅能执行事务操作，不能自行提交一半后让外层误回滚。 */
    public interface Operations {
        /** 可在提交前写入同事务业务幂等标记，未知后仍能关联原 UUID。 */
        UUID id();
        /** 新语句建立真实多数派屏障，RR 仍保持固定事务边界。 */
        ReadView statement();
        /** 每次便利点读是一条新语句，多键同语句须显式复用 ReadView。 */
        default Optional<byte[]> get(String shard, String key) { return statement().get(shard, key); }
        /** 缓冲本事务写入。 */
        void put(String shard, String key, byte[] value);
        /** 缓冲墓碑。 */
        void delete(String shard, String key);
    }
    /** 同一语句的多键、跨片读取使用同一个固定全局水位。 */
    public static final class ReadView {
        private final RatisTransaction owner; private final WalReadSnapshot snapshot;
        /** 只由当前活动事务创建。 */
        private ReadView(RatisTransaction owner, WalReadSnapshot snapshot) { this.owner = owner; this.snapshot = snapshot; }
        /** 水位可观测，不允许修改或给其他事务使用。 */
        public long sequence() { return snapshot.sequence(); }
        /** 事务关闭或进入决议后不能继续读。 */
        public Optional<byte[]> get(String shard, String key) { return owner.read(snapshot, shard, key); }
    }
    private final RatisWalNode node; private final WalMvccRuntime original; private final WalTransaction transaction;
    private State state = State.ACTIVE; private WalReceipt receipt;
    private final Operations operations = new Operations() {
        /** 原事务号不因回调视图而改变。 */
        @Override public UUID id() { return RatisTransaction.this.id(); }
        /** 业务接口委托同一会话。 */
        @Override public ReadView statement() { return RatisTransaction.this.statement(); }
        /** 不暴露底层事务或内核给业务回调。 */
        @Override public void put(String shard, String key, byte[] value) { RatisTransaction.this.put(shard, key, value); }
        /** 墓碑与普通写保持相同归属。 */
        @Override public void delete(String shard, String key) { RatisTransaction.this.delete(shard, key); }
    };
    /** 包内入口保留原内核，原有裸事务 API 不变。 */
    RatisTransaction(RatisWalNode node, WalMvccRuntime original, WalTransaction transaction) { this.node = node; this.original = original; this.transaction = transaction; }
    /** 提交之前即可保存稳定事务号，未知后按该号查询。 */
    public UUID id() { return transaction.id(); }
    /** 公开当前会话状态，不据此推断另一节点缺失结果为回滚。 */
    public synchronized State state() { return state; }
    /** 回调只能获取操作视图。 */
    public Operations operations() { return operations; }
    /** 显式新语句，同一 ReadView 可读多个分片。 */
    public synchronized ReadView statement() { active(); return new ReadView(this, node.beginReadStatement(transaction)); }
    /** 便利点读仍建立新语句，符合 RC 的可见性合同。 */
    public synchronized Optional<byte[]> get(String shard, String key) { return statement().get(shard, key); }
    /** 根据当前语句边界读取自己写集和已提交版本。 */
    private synchronized Optional<byte[]> read(WalReadSnapshot snapshot, String shard, String key) { active(); return node.get(transaction, shard, key, snapshot); }
    /** 原写集由内核防御复制，角色和容量校验继续生效。 */
    public synchronized void put(String shard, String key, byte[] value) { active(); node.put(transaction, shard, key, value); }
    /** 删除直到实际共识提交后才可见。 */
    public synchronized void delete(String shard, String key) { active(); node.delete(transaction, shard, key); }
    /** 不自动重提同一准备批次，除明确冲突/中止外的失败保守归为 UNKNOWN。 */
    public synchronized WalReceipt commit() {
        if (state == State.COMMITTED) { return receipt; } active(); state = State.DECIDING;
        try { receipt = node.commit(transaction); state = State.COMMITTED; return receipt; }
        catch (RuntimeException failure) {
            if (failure instanceof WalMvccException wal && (wal.errorCode() == ErrorCode.WRITE_CONFLICT || wal.errorCode() == ErrorCode.TRANSACTION_ABORTED)) { state = State.ROLLED_BACK; throw failure; }
            state = State.UNKNOWN; throw new WalMvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN, "commit unresolved; retain original UUID and resolve before business retry", id(), failure);
        } catch (Error failure) { state = State.UNKNOWN; throw failure; }
    }
    /** 只允许尚未开始决议的会话清理原内核本地写集。 */
    public synchronized void rollback() {
        if (state == State.ROLLED_BACK) { return; }
        if (state == State.UNKNOWN || state == State.DECIDING) { throw new WalMvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN, "cannot roll back unresolved commit", id(), null); }
        active();
        var lifecycle = original.lifecycle();
        if (lifecycle.closed() || lifecycle.fatal() != null) { state = State.ROLLED_BACK; return; }
        try { original.rollback(transaction); state = State.ROLLED_BACK; } catch (RuntimeException failure) { state = State.ABANDONED; throw failure; }
    }
    /** 关闭未知提交不执行回滚；活动会话的清理失败不能被悄悄吞掉。 */
    @Override public synchronized void close() { if (state == State.ACTIVE) { rollback(); } }
    /** 终态句柄不能给新事务或新代次复用。 */
    private void active() { if (state != State.ACTIVE) { throw new IllegalStateException("transaction session is " + state); } }
}
