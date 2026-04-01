package mvcc.storage;

import mvcc.common.IsolationLevel;

public record ReadContext(long txId, long readTs, IsolationLevel isolationLevel) {
}
