package mvcc.sql;

import mvcc.backend.TransactionBackend;
import mvcc.common.IsolationLevel;
import java.util.Objects;

/** 公共事务 SPI 的 SQL 适配器，源码后端不依赖 Calcite 包内实现。 */
public final class BackendSqlEngine implements SqlEngine {
    private final TransactionBackend backend;
    private final ShardResolver resolver;
    private final SqlValueCodec codec;
    private final IsolationLevel isolation;
    private final SqlExecutionOptions options;

    /** 装配后端及 SQL 路由和预算，不接管后端的生命周期。 */
    public BackendSqlEngine(TransactionBackend backend, ShardResolver resolver, SqlValueCodec codec,
                              IsolationLevel isolation, SqlExecutionOptions options) {
        this.backend = Objects.requireNonNull(backend, "backend");
        this.resolver = Objects.requireNonNull(resolver, "resolver");
        this.codec = Objects.requireNonNull(codec, "codec");
        this.isolation = Objects.requireNonNull(isolation, "isolation");
        this.options = Objects.requireNonNull(options, "options");
    }

    /** 创建默认预算的 SQL 引擎，主键计划和完整关系计划共享事务内核。 */
    public static BackendSqlEngine calcite(TransactionBackend backend, ShardResolver resolver,
                                             SqlValueCodec codec, IsolationLevel isolation) {
        return new BackendSqlEngine(backend, resolver, codec, isolation, SqlExecutionOptions.defaults());
    }

    /** 创建独立业务会话，不持有跨会话可变 SQL 事务。 */
    @Override
    public SqlSession openSession() {
        return new BackendSqlSession(backend, new HybridSqlPlanner(), resolver, codec, isolation, options);
    }
}
