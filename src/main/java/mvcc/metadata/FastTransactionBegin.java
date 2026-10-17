package mvcc.metadata;

import mvcc.common.IsolationLevel;
import mvcc.tso.TimestampOracle;

import java.util.Optional;

/** 可选择使用同一控制面权威合并时间戳和快照登记的事务开始能力。 */
public interface FastTransactionBegin {
    /** 权威不匹配时返回空结果；执行已开始后发生错误必须抛出，禁止退回路径重复开始。 */
    Optional<BeginReservation> tryBegin(TimestampOracle timestampOracle, IsolationLevel isolationLevel,
                                        long timeoutMillis, boolean persistMetadata, Runnable beforePublish);
}
