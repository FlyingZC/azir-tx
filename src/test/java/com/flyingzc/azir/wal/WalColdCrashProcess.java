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

package com.flyingzc.azir.wal;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.flyingzc.azir.common.IsolationLevel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import static com.flyingzc.azir.wal.WalMvccRuntimeTest.*;

/**
 * Cold Cache recovery independent crash process, only two created by the connection father test.
 */
public final class WalColdCrashProcess {

    /**
     * The tool of the independent process does not allow for exemplification.
     */
    private WalColdCrashProcess() {
    }

    /**
     * Checking point confirmed data first, then stopping at commit, checkpoint or physical collection border pending forced killing.
     *
     * @param args args
     * @throws AssertionError if the operation cannot complete
     * @throws Exception if the operation cannot complete
     */
    public static void main(final String[] args) throws Exception {
        Path root = Path.of(args[0]);
        String target = args[1];
        var armed = new AtomicBoolean();
        try (var a = pool(args[2]); var b = pool(args[3])) {
            var stores = Map.<String, WalMaterializedStore>of("a", new MysqlWalMaterializedStore(wrapped(a, armed, target, root)),
                    "b", new MysqlWalMaterializedStore(wrapped(b, armed, target, root)));
            try (var runtime = WalMvccRuntime.openForTest(root, Set.of("a", "b"), stores, options(), stage -> {
                if (armed.get() && stage.name().equals(target)) {
                    signal(root);
                }
            })) {
                var seed = runtime.begin(IsolationLevel.REPEATABLE_READ);
                for (int i = 0; i < 16; i++) {
                    runtime.put(seed, "a", "key:" + i, bytes("old:" + i));
                    runtime.put(seed, "b", "key:" + i, bytes("old:" + i));
                }
                runtime.commit(seed);
                runtime.flush();
                runtime.checkpoint();
                Files.writeString(root.resolve("seed-ack"), seed.id().toString());
                boolean committing = target.equals("AFTER_FORCE") || target.equals("BEFORE_PUBLISH") || target.equals("AFTER_PUBLISH");
                var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
                for (String shard : stores.keySet()) {
                    if (target.equals("GC_AFTER_FIRST_DELETE")) {
                        runtime.delete(tx, shard, "key:0");
                        runtime.delete(tx, shard, "key:1");
                    } else {
                        runtime.put(tx, shard, "key:0", bytes("new"));
                    }
                }
                Files.writeString(root.resolve("transaction-id"), tx.id().toString());
                armed.set(committing);
                runtime.commit(tx);
                Files.writeString(root.resolve("new-ack"), tx.id().toString());
                runtime.flush();
                armed.set(true);
                if (target.equals("GC_AFTER_FIRST_DELETE")) {
                    runtime.collectGarbage(32);
                } else {
                    runtime.checkpoint();
                }
                throw new AssertionError("crash boundary not reached");
            }
        }
    }

    /**
     * Use two carriers, small memory and zero-heat cache recovery to avoid the use of large machines or heat caches to mask cold path problems.
     *
     * @return the operation result
     */
    static WalOptions options() {
        return new WalOptions(32, 64, 8192, 32, 8, 1, 8L * 1024 * 1024, 32_768, 60_000, 1024, 50).withHotCacheBytes(0);
    }

    /**
     * Connection uses only the parent to test the public test account number of the container and does not read the user receipt.
     *
     * @param url url
     * @return the operation result
     */
    private static HikariDataSource pool(final String url) {
        var config = new HikariConfig();
        config.setJdbcUrl(url);
        config.setUsername("wal_test");
        config.setPassword("wal_test");
        config.setMaximumPoolSize(3);
        config.setMinimumIdle(0);
        config.setConnectionTimeout(5000);
        config.setValidationTimeout(1000);
        return new HikariDataSource(config);
    }

    /**
     * The first physical deletion was suspended after actual execution; the killing should result in local MySQL transaction as a whole rollback.
     *
     * @param pool pool
     * @param armed armed
     * @param target target
     * @param root root
     * @return the operation result
     */
    private static javax.sql.DataSource wrapped(final HikariDataSource pool, final AtomicBoolean armed, final String target, final Path root) {
        return (javax.sql.DataSource) java.lang.reflect.Proxy.newProxyInstance(WalColdCrashProcess.class.getClassLoader(),
                new Class<?>[]{javax.sql.DataSource.class}, (p, method, args) -> {
                    Object result = delegated(pool, method, args);
                    if (!method.getName().equals("getConnection")) {
                        return result;
                    }
                    return java.lang.reflect.Proxy.newProxyInstance(WalColdCrashProcess.class.getClassLoader(), new Class<?>[]{java.sql.Connection.class},
                            (c, operation, values) -> {
                                Object statement = delegated(result, operation, values);
                                if (!operation.getName().equals("prepareStatement") || !((String) values[0]).startsWith("delete from azir_wal_version")) {
                                    return statement;
                                }
                                return java.lang.reflect.Proxy.newProxyInstance(WalColdCrashProcess.class.getClassLoader(), new Class<?>[]{java.sql.PreparedStatement.class},
                                        (s, action, parameters) -> {
                                            Object returned = delegated(statement, action, parameters);
                                            if (armed.get() && target.equals("GC_AFTER_FIRST_DELETE") && action.getName().equals("executeUpdate")) {
                                                signal(root);
                                            }
                                            return returned;
                                        });
                            });
                });
    }

    /**
     * Keeps the original JDBC anomaly so that failure does trigger the protocol path.
     *
     * @param target target
     * @param method method
     * @param args args
     * @return the operation result
     * @throws Throwable if the operation cannot complete
     */
    private static Object delegated(final Object target, final java.lang.reflect.Method method, final Object[] args) throws Throwable {
        try {
            return method.invoke(target, args);
        } catch (final java.lang.reflect.InvocationTargetException failure) {
            throw failure.getCause();
        }
    }

    /**
     * Only test-ready markers are issued, and normal shutdown is not a substitute for real collapse.
     *
     * @param root root
     * @throws IllegalStateException if the operation cannot complete
     */
    private static void signal(final Path root) {
        try {
            Files.writeString(root.resolve("ready"), "ready");
            new CountDownLatch(1).await();
        } catch (final Exception failure) {
            throw new IllegalStateException(failure);
        }
    }
}
