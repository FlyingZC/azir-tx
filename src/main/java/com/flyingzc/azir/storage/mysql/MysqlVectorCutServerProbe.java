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

package com.flyingzc.azir.storage.mysql;

import com.zaxxer.hikari.HikariDataSource;
import com.flyingzc.azir.publication.mysql.MysqlLocalFirstPublicationLedger;

import javax.sql.DataSource;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * Independent JVM failure is injected with a dedicated loop-to-face service without initializing or resetting database.
 */
public final class MysqlVectorCutServerProbe {

    /**
     * The probe entrance does not store inter-process variability.
     */
    private MysqlVectorCutServerProbe() {
    }

    /**
     * It must be visible to enable and provide three initialized database with the only loop-back port.
     *
     * @param args args
     * @throws Exception if the operation cannot complete
     * @throws IllegalArgumentException if the operation cannot complete
     * @throws IllegalStateException if the operation cannot complete
     */
    public static void main(final String[] args) throws Exception {
        if (!Boolean.parseBoolean(System.getenv("AZIR_TX_VECTOR_TEST_SERVER"))
                || args.length < 7 || args.length > 8) {
            throw new IllegalArgumentException("vector cut server probe requires explicit test flag and endpoint arguments");
        }
        String username = args[3];
        String password = args[4];
        int port = Integer.parseInt(args[5]);
        long leaseMillis = Long.parseLong(args[6]);
        try (
                HikariDataSource center = pool(args[0], username, password);
                HikariDataSource first = pool(args[1], username, password);
                HikariDataSource second = pool(args[2], username, password)) {
            Map<String, DataSource> routes = Map.of("s1", first, "s2", second);
            var ledger = new MysqlLocalFirstPublicationLedger(center);
            var leases = new MysqlVectorCutLeaderLease(center, routes, 4);
            try (
                    var runtime = new MysqlVectorCutLeaderRuntime(center, routes, 4,
                            ledger::currentPublished, leases, "127.0.0.1:" + port,
                            leaseMillis, 200)) {
                if (!runtime.start()) {
                    throw new IllegalStateException("vector cut probe found another active leader");
                }
                String transport = args.length == 8 ? args[7] : "http";
                if ("tcp".equals(transport)) {
                    try (var server = new MysqlVectorCutTcpServer(runtime, port)) {
                        server.start();
                        announceReady(runtime);
                    }
                } else if ("http".equals(transport)) {
                    try (var server = new MysqlVectorCutHttpServer(runtime, port)) {
                        server.start();
                        announceReady(runtime);
                    }
                } else {
                    throw new IllegalArgumentException("unknown vector cut test transport");
                }
            }
        }
    }

    /**
     * The physics connection of the three stand-alone JDBC pool simulation distribution service.
     *
     * @param url url
     * @param username username
     * @param password password
     * @return the operation result
     */
    private static HikariDataSource pool(final String url, final String username, final String password) {
        return (HikariDataSource) MysqlDataSourceFactory.create(url, username, password);
    }

    /**
     * Only after the listening has been successful and lease is still valid will it be reported to the father process.
     *
     * @param runtime runtime
     * @throws InterruptedException if the operation cannot complete
     */
    private static void announceReady(final MysqlVectorCutLeaderRuntime runtime) throws InterruptedException {
        System.out.println("VECTOR_SERVER_READY generation="
                + runtime.currentView().generation());
        System.out.flush();
        new CountDownLatch(1).await();
    }
}
