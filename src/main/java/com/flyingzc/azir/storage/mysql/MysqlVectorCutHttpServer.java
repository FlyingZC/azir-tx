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

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Only read-only tangent test endpoints that bind back to the ring address are not available when lease is disabled.
 */
final class MysqlVectorCutHttpServer implements AutoCloseable {

    private final MysqlVectorCutLeaderRuntime runtime;

    private final HttpServer server;

    private final ExecutorService requests = Executors.newVirtualThreadPerTaskExecutor();

    private boolean started;

    /**
     * The caller must first take the leader lease when endpoints are created to launch the release undisclosedly.
     *
     * @param runtime runtime
     * @param port port
     * @throws IOException if the operation cannot complete
     * @throws IllegalArgumentException if the operation cannot complete
     */
    MysqlVectorCutHttpServer(final MysqlVectorCutLeaderRuntime runtime, final int port) throws IOException {
        this.runtime = Objects.requireNonNull(runtime, "runtime");
        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException("invalid vector cut HTTP port");
        }
        this.server = HttpServer.create(new InetSocketAddress("127.0.0.1", port), 32);
        server.createContext("/cut", this::serveCut);
        server.setExecutor(requests);
    }

    /**
     * To obtain the actual binding of the port, the test will use the port zero to be distributed by the system.
     *
     * @return the operation result
     */
    int port() {
        return server.getAddress().getPort();
    }

    /**
     * The response to the client can only start when lease is already in place and the face reconstruction has been completed.
     *
     * @throws IllegalStateException if the operation cannot complete
     */
    synchronized void start() {
        if (started || !runtime.isServing()) {
            throw new IllegalStateException("vector cut HTTP endpoint requires an active leader");
        }
        server.start();
        started = true;
    }

    /**
     * Each request is checked for eligibility twice at running time and returns the algebra and complete vector.
     *
     * @param exchange exchange
     * @throws IOException if the operation cannot complete
     */
    private void serveCut(final HttpExchange exchange) throws IOException {
        try {
            if (!"GET".equals(exchange.getRequestMethod())
                    || !"/cut".equals(exchange.getRequestURI().getPath())) {
                exchange.sendResponseHeaders(405, -1);
                return;
            }
            try {
                byte[] body = MysqlVectorCutWire.encode(runtime.currentView());
                exchange.getResponseHeaders().set("Content-Type", "application/octet-stream");
                exchange.getResponseHeaders().set("Cache-Control", "no-store");
                exchange.sendResponseHeaders(200, body.length);
                exchange.getResponseBody().write(body);
            } catch (final RuntimeException ex) {
                exchange.sendResponseHeaders(503, -1);
            }
        } finally {
            exchange.close();
        }
    }

    /**
     * Stops new requests and waits for the return loop that has begun to read out.
     */
    @Override
    public synchronized void close() {
        runtime.close();
        if (started) {
            server.stop(0);
            started = false;
        }
        requests.close();
    }
}
