package mvcc.txn;

/**
 * 锁冲突处理模式枚举。
 */
public enum ConflictMode {
    FAIL_FAST,
    WAIT
}
