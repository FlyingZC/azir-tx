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

package com.flyingzc.azir.wal.ha;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Read-only probe for in-house monitoring only, read caches only and do not perform SQL, transaction or majority network detection.
 */
final class RatisNodeHealthServer implements AutoCloseable {

    private final HttpServer server;

    private final ThreadPoolExecutor workers;

    private final RatisNodeHost host;

    /**
     * Only a digital return address must be bound, with clear ceilings for threads, waiting queues and HTTP connection.
     *
     * @param config config
     * @param host host
     * @throws IOException if the operation cannot complete
     */
    RatisNodeHealthServer(final RatisNodeConfiguration config, final RatisNodeHost host) throws IOException {
        requireBudgets();
        this.host = host;
        byte[] address = config.healthAddress.equals("127.0.0.1") ? new byte[]{127, 0, 0, 1}
                : new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1};
        server = HttpServer.create(new InetSocketAddress(InetAddress.getByAddress(address), config.healthPort), 16);
        workers = new ThreadPoolExecutor(2, 2, 0, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(8),
                Thread.ofPlatform().daemon().name("wal-health-http-", 0).factory(), new ThreadPoolExecutor.AbortPolicy());
        try {
            server.setExecutor(workers);
            server.createContext("/", this::respond);
            server.start();
        } catch (final RuntimeException | Error failure) {
            server.stop(0);
            workers.shutdown();
            throw failure;
        }
    }

    /**
     * The independent process sets budget from JVM start parameters and this component does not modify the global HTTP properties of the host process.
     *
     * @throws IllegalArgumentException if the operation cannot complete
     */
    static void requireBudgets() {
        for (var entry : Map.of("jdk.httpserver.maxConnections", 32, "sun.net.httpserver.maxIdleConnections", 8,
                "sun.net.httpserver.maxReqHeaders", 32, "sun.net.httpserver.maxReqTime", 5,
                "sun.net.httpserver.maxRspTime", 5, "sun.net.httpserver.idleInterval", 5).entrySet()) {
            String raw = System.getProperty(entry.getKey());
            int value;
            try {
                value = raw == null ? -1 : Integer.parseInt(raw);
            } catch (final NumberFormatException invalid) {
                throw new IllegalArgumentException("explicit bounded health HTTP JVM settings required");
            }
            if (value < 1 || value > entry.getValue()) {
                throw new IllegalArgumentException("explicit bounded health HTTP JVM settings required");
            }
        }
        if (!"0".equals(System.getProperty("sun.net.httpserver.drainAmount"))) {
            throw new IllegalArgumentException("health HTTP must not drain request bodies");
        }
    }

    /**
     * Accurate route only accepts GET/HEAD, refuses the requesting body and does not reveal the wrong cause chain or database address.
     *
     * @param exchange exchange
     * @throws IOException if the operation cannot complete
     */
    private void respond(final HttpExchange exchange) throws IOException {
        try (exchange) {
            String method = exchange.getRequestMethod();
            String path = exchange.getRequestURI().getRawPath();
            int code;
            String body;
            if (!method.equals("GET") && !method.equals("HEAD")) {
                exchange.getResponseHeaders().set("Allow", "GET, HEAD");
                code = 405;
                body = "{\"error\":\"method_not_allowed\"}";
            } else if (exchange.getRequestURI().getRawQuery() != null || exchange.getRequestURI().getRawFragment() != null
                    || exchange.getRequestHeaders().containsKey("Transfer-Encoding")
                    || exchange.getRequestHeaders().containsKey("Content-Length") && !exchange.getRequestHeaders().get("Content-Length").equals(java.util.List.of("0"))) {
                code = 400;
                body = "{\"error\":\"unsupported_request\"}";
            } else {
                var status = host.status();
                boolean ok = switch (path) {
                    case "/live" -> status.live();
                    case "/ready" -> status.ready();
                    case "/writable" -> status.writableHint();
                    case "/status" -> true;
                    default -> false;
                };
                boolean known = java.util.Set.of("/live", "/ready", "/writable", "/status").contains(path);
                code = !known ? 404 : ok ? 200 : 503;
                body = known ? status.json() : "{\"error\":\"not_found\"}";
            }
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            var headers = exchange.getResponseHeaders();
            headers.set("Content-Type", "application/json; charset=utf-8");
            headers.set("Cache-Control", "no-store");
            headers.set("X-Content-Type-Options", "nosniff");
            headers.set("Connection", "close");
            if (method.equals("HEAD")) {
                headers.set("Content-Length", Integer.toString(bytes.length));
                exchange.sendResponseHeaders(code, -1);
            } else {
                exchange.sendResponseHeaders(code, bytes.length);
                exchange.getResponseBody().write(bytes);
            }
        }
    }

    /**
     * Stop listening and wait for probe workers to exit, timeout keep the handle to close again.
     *
     * @throws IOException if the operation cannot complete
     */
    @Override
    public void close() throws IOException {
        server.stop(0);
        workers.shutdown();
        try {
            if (!workers.awaitTermination(10, TimeUnit.SECONDS)) {
                throw new IOException("health HTTP workers did not drain; retry close");
            }
        } catch (final InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IOException("health HTTP close interrupted", interrupted);
        }
    }
}
