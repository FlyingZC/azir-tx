package mvcc.wal;

/** 绑定事务的语句快照，用于同一条语句的多键、跨片和分页读取。 */
public final class WalReadSnapshot {
    final WalTransaction transaction;
    final long sequence;

    /** 仅允许运行时在有效事务上创建快照。 */
    WalReadSnapshot(WalTransaction transaction, long sequence) {
        this.transaction = transaction;
        this.sequence = sequence;
    }

    /** 返回该语句固定的全局读取水位。 */
    public long sequence() { return sequence; }
}
