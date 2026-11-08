/*
 * Copyright 2026 Azir TX contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.flyingzc.azir.e2e;

import com.flyingzc.azir.common.IsolationLevel;
import com.flyingzc.azir.metadata.mysql.MysqlTransactionMetadataStore;
import com.flyingzc.azir.storage.mysql.MysqlDataSourceFactory;
import com.flyingzc.azir.storage.mysql.MysqlStorageAdapter;
import com.flyingzc.azir.tso.mysql.MysqlTimestampOracle;
import com.flyingzc.azir.txn.TransactionManager;
import com.flyingzc.azir.txn.TransactionOptions;
import com.zaxxer.hikari.HikariDataSource;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.util.Map;

/**
 * A complete transaction control SQL;general log is recorded on MySQL for count only and not for throughput measurements.
 */
public final class ControlSqlTrace {

    /**
     * Prevent instantiation of this utility class.
     */
    private ControlSqlTrace() {
    }

    /**
     * Document single or cross-shard transaction after initializing the special library, close this enabled general log before you quit.
     *
     * @param args args
     * @throws Exception if the operation cannot complete
     */
    public static void main(final String[] args) throws Exception {
        boolean cross = args.length > 0 && args[0].equals("cross");
        String base = System.getenv("AZIR_TRACE_JDBC_BASE");
        String password = System.getenv("AZIR_TX_PASSWORD");
        try (
                var control = (HikariDataSource) MysqlDataSourceFactory.create(base + "trace_control", "root", password);
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
            try (
                    var manager = new TransactionManager(tso, metadata, TransactionOptions.defaults(),
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
