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

package com.flyingzc.azir.operations;

import com.flyingzc.azir.maintenance.MaintenanceDiagnostics;
import com.flyingzc.azir.maintenance.MaintenanceService;
import java.util.LinkedHashMap;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.Filter;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

import java.util.function.Supplier;

/**
 * Read-only Transport interface: Prometheus, Survival and Readiness Check; Freezing, recovery and Switching are not open through HTTP.
 */
public final class BackendOperationsServer implements AutoCloseable {

    private final HttpServer server;

    private final ThreadPoolExecutor executor;

    private final MysqlProtocolAdmin admin;

    private final Supplier<MaintenanceDiagnostics> diagnostics;

    private final Supplier<MaintenanceService.Health> health;

    private final java.util.concurrent.atomic.AtomicBoolean stopRequested = new java.util.concurrent.atomic.AtomicBoolean();

    private final long createdAt = System.nanoTime();

    private final long maxMaintenanceSilenceMillis;

    /**
     * Visible wiretapping of listening addresses, use of limited task force columns and non-automatic listening to external networks.
     *
     * @param address address
     * @param admin admin
     * @param diagnostics diagnostics
     * @param health health
     */
    public BackendOperationsServer(final InetSocketAddress address, final MysqlProtocolAdmin admin,
                                   final Supplier<MaintenanceDiagnostics> diagnostics, final Supplier<MaintenanceService.Health> health) {
        this(address, admin, diagnostics, health, java.time.Duration.ofSeconds(60));
    }

    /**
     * Based on the maintenance cycle ' s configuration of silent thresholds, background is stuck or not permanently ready when first completed.
     *
     * @param address address
     * @param admin admin
     * @param diagnostics diagnostics
     * @param health health
     * @param maxMaintenanceSilence max maintenance silence
     * @throws IllegalArgumentException if the operation cannot complete
     * @throws IllegalStateException if the operation cannot complete
     */
    public BackendOperationsServer(final InetSocketAddress address, final MysqlProtocolAdmin admin,
                                   final Supplier<MaintenanceDiagnostics> diagnostics, final Supplier<MaintenanceService.Health> health,
                                   final java.time.Duration maxMaintenanceSilence) {
        long silence = maxMaintenanceSilence.toMillis();
        if (silence < 1 || silence > 86_400_000) {
            throw new IllegalArgumentException("invalid maintenance silence threshold");
        }
        maxMaintenanceSilenceMillis = silence;
        admin.requireWritable();
        this.admin = Objects.requireNonNull(admin);
        this.diagnostics = Objects.requireNonNull(diagnostics);
        this.health = Objects.requireNonNull(health);
        executor = new ThreadPoolExecutor(2, 2, 0, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(32),
                Thread.ofPlatform().daemon().name("azir-operations-http-", 0).factory(), new ThreadPoolExecutor.AbortPolicy());
        try {
            server = HttpServer.create(address, 16);
        } catch (final IOException failure) {
            executor.shutdownNow();
            throw new IllegalStateException("operations HTTP bind failed", failure);
        }
        try {
            server.setExecutor(executor);
            context("/health/live", exchange -> respond(exchange, 200, "text/plain; charset=utf-8", "alive\n"));
            context("/health/ready", exchange -> {
                String body;
                int code;
                try {
                    body = readiness() ? "ready\n" : "not ready\n";
                    code = body.startsWith("ready") ? 200 : 503;
                } catch (final RuntimeException failure) {
                    body = "storage unavailable\n";
                    code = 503;
                }
                respond(exchange, code, "text/plain; charset=utf-8", body);
            });
            context("/metrics", exchange -> {
                String text;
                int code;
                try {
                    text = prometheus();
                    code = 200;
                } catch (final RuntimeException failure) {
                    text = "azir_backend_ready 0\nazir_backend_observation_error 1\n";
                    code = 503;
                }
                respond(exchange, code, "text/plain; version=0.0.4; charset=utf-8", text);
            });
            server.start();
        } catch (final RuntimeException | Error failure) {
            // HTTP must also immediately return listening resources when the port has been bound but initialization of the processor or thread failed.
            try {
                server.stop(0);
            } catch (final RuntimeException | Error cleanup) {
                if (failure != cleanup) {
                    failure.addSuppressed(cleanup);
                }
            }
            try {
                executor.shutdownNow();
            } catch (final RuntimeException | Error cleanup) {
                if (failure != cleanup) {
                    failure.addSuppressed(cleanup);
                }
            }
            throw failure;
        }
    }

    /**
     * The fixed management path is only accurately matched, and the HTTP layer does not allow cache readiness.
     *
     * @param path path
     * @param handler handler
     */
    private void context(final String path, final HttpHandler handler) {
        var context = server.createContext(path, handler);
        context.getFilters().add(new ReadOnlyFilter(path));
    }

    /**
     * HEAD returns the same status and response head as GET, but does not send a response.
     *
     * @param exchange exchange
     * @param code code
     * @param contentType content type
     * @param body body
     * @throws IOException if the operation cannot complete
     */
    private static void respond(final HttpExchange exchange, final int code, final String contentType, final String body) throws IOException {
        byte[] text = body.getBytes(StandardCharsets.UTF_8);
        try (exchange) {
            exchange.getResponseHeaders().set("Content-Type", contentType);
            boolean head = exchange.getRequestMethod().equals("HEAD");
            exchange.sendResponseHeaders(code, head ? -1 : text.length);
            if (!head) {
                exchange.getResponseBody().write(text);
            }
        }
    }

    /**
     * Returns the actual listening port to support testing for randomly available ports.
     *
     * @return the operation result
     */
    public int port() {
        return server.getAddress().getPort();
    }

    /**
     * Frozen, partially generation inconsistent, expired client or background failed to report.
     *
     * @return the operation result
     */
    public boolean readiness() {
        if (stopRequested.get()) {
            return false;
        }
        var states = admin.status();
        var control = states.get("control");
        if (!control.namespace().equals(admin.state().namespace()) || control.generation() != admin.state().generation()
                || states.values().stream().anyMatch(s -> !s.mode().equals("ACTIVE") || !s.namespace().equals(control.namespace()) || s.generation() != control.generation())) {
            return false;
        }
        var work = health.get();
        if (work != null) {
            if (work.lastError() != null) {
                return false;
            }
            long age = work.lastFinished() == null ? TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - createdAt)
                    : Math.max(0, java.time.Duration.between(work.lastFinished(), java.time.Instant.now()).toMillis());
            if (age > maxMaintenanceSilenceMillis) {
                return false;
            }
        }
        diagnostics.get();
        return true;
    }

    /**
     * Export a fixed indicator that does not contain business keys, payload, database URL or receipt.
     *
     * @return the operation result
     */
    public String prometheus() {
        var status = admin.state().status();
        var work = health.get();
        boolean active = status.mode().equals("ACTIVE") && status.generation() == admin.state().generation();
        var values = new LinkedHashMap<String, Number>();
        values.put("ready", readiness() ? 1 : 0);
        values.put("generation", status.generation());
        values.put("retired_before", status.retiredBefore());
        values.put("frozen", active ? 0 : 1);
        if (active) {
            var data = diagnostics.get();
            values.put("safe_point", data.safePoint());
            values.put("oldest_active_snapshot", data.oldestActiveSnapshot());
            values.put("active_snapshots", data.activeSnapshots());
            values.put("expired_snapshots", data.expiredSnapshots());
            values.put("pending_items", data.pendingItems());
        }
        if (work != null) {
            values.put("maintenance_successes_total", work.successes());
            values.put("maintenance_failures_total", work.failures());
            values.put("maintenance_last_finished_seconds", work.lastFinished() == null ? 0 : work.lastFinished().getEpochSecond());
        }
        var text = new StringBuilder();
        values.forEach((name, value) -> text.append("azir_backend_").append(name).append("{backend=\"").append(admin.state().prefix()).append("\",namespace=\"")
                .append(status.namespace()).append("\"} ").append(value).append('\n'));
        return text.toString();
    }

    /**
     * Stop listening and wait for the end of limited request threads without closing operations database pool.
     *
     * @throws IllegalStateException if the operation cannot complete
     */
    @Override
    public void close() {
        if (stopRequested.compareAndSet(false, true)) {
            server.stop(1);
        }
        executor.shutdownNow();
        try {
            if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                throw new IllegalStateException("operations requests did not stop");
            }
        } catch (final InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(failure);
        }
    }

    /**
     * Uniform read-only contract, error path and writing method rejected before accessing database.
     */
    private static final class ReadOnlyFilter extends Filter {

        private final String path;

        /**
         * Fixed registered path, which does not match the open extra route with a prefix.
         *
         * @param path path
         */
        ReadOnlyFilter(final String path) {
            this.path = path;
        }

        /**
         * Provides the server with a filter description that does not contain configuration and receipt.
         *
         * @return the operation result
         */
        @Override
        public String description() {
            return "只读且禁止缓存的事务运维接口";
        }

        /**
         * The validate method and path are followed by a health or indicator query.
         *
         * @param exchange exchange
         * @param chain chain
         * @throws IOException if the operation cannot complete
         */
        @Override
        public void doFilter(final HttpExchange exchange, final Chain chain) throws IOException {
            exchange.getResponseHeaders().set("Cache-Control", "no-store");
            if (!exchange.getRequestURI().getRawPath().equals(path)) {
                respond(exchange, 404, "text/plain; charset=utf-8", "not found\n");
                return;
            }
            if (!Set.of("GET", "HEAD").contains(exchange.getRequestMethod())) {
                exchange.getResponseHeaders().set("Allow", "GET, HEAD");
                respond(exchange, 405, "text/plain; charset=utf-8", "method not allowed\n");
                return;
            }
            chain.doFilter(exchange);
        }
    }
}
