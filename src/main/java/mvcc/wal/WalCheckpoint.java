package mvcc.wal;

/** 一个后端已原子应用的全局日志前缀及其末项摘要。 */
public record WalCheckpoint(long sequence, String digest) {
    /** 校验空前缀和非空前缀的摘要表示。 */
    public WalCheckpoint {
        if (sequence < 0 || digest == null || (sequence == 0 ? !digest.isEmpty() : digest.length() != 64)) {
            throw new IllegalArgumentException("invalid materialization checkpoint");
        }
    }
}
