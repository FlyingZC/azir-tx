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
import java.util.concurrent.CountDownLatch;
import static com.flyingzc.azir.wal.WalMvccRuntimeTest.*;

/**
 * Real backup and recovery crash process, only connection parent test explicitly created source and recovery database.
 */
public final class WalBackupCrashProcess {

    /**
     * Sub-process tools do not allow for exemplification.
     */
    private WalBackupCrashProcess() {
    }

    /**
     * Stops after the specified file or database has actually been written and is forced to kill by a parent process.
     *
     * @param args args
     * @throws AssertionError if the operation cannot complete
     * @throws Exception if the operation cannot complete
     */
    public static void main(final String[] args) throws Exception {
        Path root = Path.of(args[0]);
        String boundary = args[1];
        try (var a = pool(args[2]); var b = pool(args[3]); var targetA = pool(args[4]); var targetB = pool(args[5])) {
            if (boundary.contains("BACKUP")) {
                var sources = Map.<String, WalMaterializedStore>of("a", new MysqlWalMaterializedStore(a), "b", new MysqlWalMaterializedStore(b));
                try (var runtime = WalMvccRuntime.openForTest(root.resolve("source"), sources.keySet(), sources, WalOptions.defaults().withHotCacheBytes(0), stage -> {
                    if (stage.name().equals(boundary)) {
                        signal(root);
                    }
                })) {
                    var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
                    for (int i = 0; i < 20; i++) {
                        for (String shard : sources.keySet()) {
                            runtime.put(tx, shard, "k" + i, bytes("v:" + i));
                        }
                    }
                    runtime.commit(tx);
                    Files.writeString(root.resolve("seed-id"), tx.id().toString());
                    runtime.backup(root.resolve("backup"), limits());
                }
            } else {
                var targets = Map.<String, WalMaterializedStore>of("a", new MysqlWalMaterializedStore(targetA), "b", new MysqlWalMaterializedStore(targetB));
                WalBackup.restoreForTest(root.resolve("backup"), root.resolve("restored"), targets, WalOptions.defaults().withHotCacheBytes(0), limits(), stage -> {
                    if (stage.name().equals(boundary)) {
                        signal(root);
                    }
                });
            }
            throw new AssertionError("backup or restore crash boundary not reached");
        }
    }

    /**
     * Small batch allows the second shard part of recovery to be observed with certainty.
     *
     * @return the operation result
     */
    static WalBackupOptions limits() {
        return new WalBackupOptions(32L * 1024 * 1024, 4, 8192, 60_000);
    }

    /**
     * Up to two connection tests per pool, not reading user database receipt.
     *
     * @param url url
     * @return the operation result
     */
    private static HikariDataSource pool(final String url) {
        var config = new HikariConfig();
        config.setJdbcUrl(url);
        config.setUsername("wal_test");
        config.setPassword("wal_test");
        config.setMaximumPoolSize(2);
        config.setMinimumIdle(0);
        config.setConnectionTimeout(5000);
        config.setValidationTimeout(1000);
        return new HikariDataSource(config);
    }

    /**
     * Maintaining the process after marking the actual boundary is viable and normal exit cannot be a substitute for collapse.
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
