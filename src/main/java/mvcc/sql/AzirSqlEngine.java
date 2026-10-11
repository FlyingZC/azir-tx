package mvcc.sql;

import mvcc.common.IsolationLevel;
import mvcc.txn.TransactionManager;

/**
 * Azir SQL 引擎，负责创建绑定事务管理器和 SQL 组件的会话。
 */
public final class AzirSqlEngine {
    private final TransactionManager transactionManager;
    private final SqlPlanner planner;
    private final ShardResolver shardResolver;
    private final SqlValueCodec valueCodec;
    private final IsolationLevel defaultIsolationLevel;
    private final SqlStorageKeyCodec keyCodec;

    /**
     * 构造 Azir SQL 引擎。
     */
    public AzirSqlEngine(TransactionManager transactionManager,
                         SqlPlanner planner,
                         ShardResolver shardResolver,
                         SqlValueCodec valueCodec,
                         IsolationLevel defaultIsolationLevel) {
        this.transactionManager = transactionManager;
        this.planner = planner;
        this.shardResolver = shardResolver;
        this.valueCodec = valueCodec;
        this.defaultIsolationLevel = defaultIsolationLevel;
        this.keyCodec = new SqlStorageKeyCodec();
    }

    /**
     * 创建使用 Calcite 计划器的 Azir SQL 引擎。
     */
    public static AzirSqlEngine calcite(TransactionManager transactionManager,
                                        ShardResolver shardResolver,
                                        SqlValueCodec valueCodec,
                                        IsolationLevel defaultIsolationLevel) {
        return new AzirSqlEngine(
                transactionManager,
                new CalciteSqlPlanner(),
                shardResolver,
                valueCodec,
                defaultIsolationLevel
        );
    }

    /**
     * 创建新的 SQL 会话。
     */
    public SqlSession openSession() {
        return new AzirSqlSession(
                transactionManager,
                planner,
                shardResolver,
                valueCodec,
                keyCodec,
                defaultIsolationLevel
        );
    }
}
