package mvcc.wal;

import java.util.UUID;

/** 可从 WAL 恢复的持久提交凭据，不依赖调用方仍持有事务对象。 */
public record WalReceipt(UUID transactionId, long commitSequence, String digest) { }
