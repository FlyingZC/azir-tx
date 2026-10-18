package mvcc.sql;

import mvcc.common.IsolationLevel;
import mvcc.txn.TransactionManager;

import java.util.Objects;

/**
 * Azir SQL 引擎，负责创建绑定事务管理器和 SQL 组件的会话。
 */
public final class AzirSqlEngine implements SqlEngine {
    private final TransactionManager transactionManager;
    private final SqlPlanner planner;
    private final ShardResolver shardResolver;
    private final SqlValueCodec valueCodec;
    private final IsolationLevel defaultIsolationLevel;
    private final SqlStorageKeyCodec keyCodec;
    private final SqlExecutionOptions executionOptions;

    /**
     * 构造 Azir SQL 引擎。
     */
    public AzirSqlEngine(TransactionManager transactionManager,
                         SqlPlanner planner,
                         ShardResolver shardResolver,
                         SqlValueCodec valueCodec,
                         IsolationLevel defaultIsolationLevel) {
        this(transactionManager, planner, shardResolver, valueCodec, defaultIsolationLevel, SqlExecutionOptions.defaults());
    }

    /**
     * 构造可配置分页大小与结果预算的引擎，保留计划器可插拔接口。
     */
    public AzirSqlEngine(TransactionManager transactionManager,
                         SqlPlanner planner,
                         ShardResolver shardResolver,
                         SqlValueCodec valueCodec,
                         IsolationLevel defaultIsolationLevel,
                         SqlExecutionOptions executionOptions) {
        this.transactionManager = transactionManager;
        this.planner = planner;
        this.shardResolver = shardResolver;
        this.valueCodec = valueCodec;
        this.defaultIsolationLevel = defaultIsolationLevel;
        this.keyCodec = new SqlStorageKeyCodec();
        this.executionOptions = Objects.requireNonNull(executionOptions, "executionOptions");
    }

    /**
     * 创建使用 Calcite 计划器的 Azir SQL 引擎。
     */
    public static AzirSqlEngine calcite(TransactionManager transactionManager,
                                        ShardResolver shardResolver,
                                        SqlValueCodec valueCodec,
                                        IsolationLevel defaultIsolationLevel) {
        return calcite(transactionManager, shardResolver, valueCodec, defaultIsolationLevel, SqlExecutionOptions.defaults());
    }

    /**
     * 创建配置分页大小和结果预算的 Calcite 引擎。
     */
    public static AzirSqlEngine calcite(TransactionManager transactionManager,
                                        ShardResolver shardResolver,
                                        SqlValueCodec valueCodec,
                                        IsolationLevel defaultIsolationLevel,
                                        SqlExecutionOptions executionOptions) {
        return new AzirSqlEngine(
                transactionManager,
                new CalciteSqlPlanner(),
                shardResolver,
                valueCodec,
                defaultIsolationLevel,
                executionOptions
        );
    }

    /**
     * 创建新的 SQL 会话。
     */
    @Override
    public SqlSession openSession() {
        return new AzirSqlSession(
                transactionManager,
                planner,
                shardResolver,
                valueCodec,
                keyCodec,
                defaultIsolationLevel,
                executionOptions
        );
    }
}
