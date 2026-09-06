package mvcc.storage;

import mvcc.common.IsolationLevel;

/**
 * 读请求上下文，携带事务和快照时间戳。
 */
public record ReadContext(long txId, long readTs, IsolationLevel isolationLevel) {
}
