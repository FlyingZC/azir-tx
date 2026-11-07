package com.flyingzc.azir.tso.mysql;

import com.flyingzc.azir.tso.TimestampOracle;

import javax.sql.DataSource;
import java.util.concurrent.atomic.LongAdder;

/**
 * 保留旧号段分配器 API 的兼容入口，实际逐次委托全局持久化序列。
 * 独立客户端号段会破坏跨实例实时顺序，故不再缓存号段；每次调用都有数据库事务开销。
 * batchSize 只为源码兼容保留，不代表当前存在批量性能优化。
 */
public final class MysqlSegmentTimestampOracle implements TimestampOracle, MysqlTimestampAuthority {
    private final MysqlTimestampOracle delegate;
    private final LongAdder allocations = new LongAdder();

    /** 校验旧批量参数并创建严格持久化分配器。 */
    public MysqlSegmentTimestampOracle(DataSource dataSource, int batchSize) {
        if (batchSize < 1) {
            throw new IllegalArgumentException("batchSize must be >= 1");
        }
        delegate = new MysqlTimestampOracle(dataSource);
    }

    /** 兼容入口与底层逐次发号器使用同一个控制池。 */
    @Override
    public boolean usesDataSource(DataSource dataSource) {
        return delegate.usesDataSource(dataSource);
    }

    /** 初始化同数据库的 global 序列，不申请本地号段。 */
    public void initialize(long initialValue) {
        delegate.initialize(initialValue);
    }

    /** 从所有客户端共享的持久化序列逐次取号，并复用发布屏障连接。 */
    @Override
    public long nextTimestamp() {
        long timestamp = delegate.nextTimestamp();
        allocations.increment();
        return timestamp;
    }

    /** 保留旧统计方法名；现在返回成功完成的单次数据库分配数，每段大小恒为一。 */
    public long segmentAllocations() {
        return allocations.sum();
    }
}
