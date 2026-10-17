package mvcc.metadata;

import mvcc.tso.TimestampOracle;

import java.util.OptionalLong;
import java.util.Set;
import java.util.function.LongConsumer;

/** 可选择在同一控制面操作中分配时间戳并持久化跨分片提交决议。 */
public interface FastTransactionCommit {
    /** 权威不匹配时返回空结果；开始执行后的异常必须由调用方按决议未知处理。 */
    OptionalLong tryCommit(TimestampOracle timestampOracle, long txId, Set<String> participants,
                           Runnable beforePublish, LongConsumer afterCommitted);
}
