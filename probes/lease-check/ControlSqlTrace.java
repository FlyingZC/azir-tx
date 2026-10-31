import mvcc.common.IsolationLevel;
import mvcc.metadata.mysql.MysqlTransactionMetadataStore;
import mvcc.storage.mysql.MysqlDataSourceFactory;
import mvcc.storage.mysql.MysqlStorageAdapter;
import mvcc.tso.mysql.MysqlTimestampOracle;
import mvcc.txn.TransactionManager;
import mvcc.txn.TransactionOptions;
import com.zaxxer.hikari.HikariDataSource;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.util.Map;

/** 在隔离 MySQL 上记录一笔完整事务的控制 SQL；general log 仅用于计数，不用于吞吐测量。 */
public final class ControlSqlTrace {
    /** 工具类不允许实例化。 */
    private ControlSqlTrace() {
    }

    /** 初始化专用库后记录单片或跨片事务，退出前关闭本次启用的 general log。 */
    public static void main(String[] args) throws Exception {
        boolean cross = args.length > 0 && args[0].equals("cross");
        String base = System.getenv("AZIR_TRACE_JDBC_BASE");
        String password = System.getenv("AZIR_TX_PASSWORD");
        try (var control = (HikariDataSource) MysqlDataSourceFactory.create(base + "trace_control", "root", password);
             var first = (HikariDataSource) MysqlDataSourceFactory.create(base + "trace_shard_a", "root", password);
             var second = (HikariDataSource) MysqlDataSourceFactory.create(base + "trace_shard_b", "root", password)) {
            var metadata = new MysqlTransactionMetadataStore(control);
            var tso = new MysqlTimestampOracle(control);
            var a = new MysqlStorageAdapter("a", first, metadata);
            var b = new MysqlStorageAdapter("b", second, metadata);
            tso.initialize(100);
            metadata.initialize();
            a.initialize();
            b.initialize();
            try (var manager = new TransactionManager(tso, metadata, TransactionOptions.defaults(),
                    Map.of("a", a, "b", b));
                 Connection admin = control.getConnection();
                 var statement = admin.createStatement()) {
                statement.execute("set global log_output = 'TABLE'");
                statement.execute("truncate table mysql.general_log");
                statement.execute("set global general_log = 'ON'");
                try {
                    manager.execute(IsolationLevel.REPEATABLE_READ, tx -> {
                        byte[] value = "trace-value".getBytes(StandardCharsets.UTF_8);
                        manager.put(tx, "a", "trace:" + tx.txId(), value);
                        if (cross) {
                            manager.put(tx, "b", "trace:" + tx.txId(), value);
                        }
                        return null;
                    });
                } finally {
                    statement.execute("set global general_log = 'OFF'");
                }
                try (var rows = statement.executeQuery("""
                        select command_type, argument from mysql.general_log
                        where command_type in ('Query', 'Execute') order by event_time
                        """)) {
                    while (rows.next()) {
                        System.out.println(rows.getString(1) + "\t"
                                + rows.getString(2).replace('\n', ' ').replace('\r', ' '));
                    }
                }
            }
        }
    }

}
