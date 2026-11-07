package com.flyingzc.azir.storage.mysql;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/** 仅绑定回环地址的只读切面实验端点，租约失效时返回不可用。 */
final class MysqlVectorCutHttpServer implements AutoCloseable {
    private final MysqlVectorCutLeaderRuntime runtime;
    private final HttpServer server;
    private final ExecutorService requests = Executors.newVirtualThreadPerTaskExecutor();
    private boolean started;

    /** 端点创建不隐式启动发布运行时，调用方须先取得领导者租约。 */
    MysqlVectorCutHttpServer(MysqlVectorCutLeaderRuntime runtime, int port) throws IOException {
        this.runtime = Objects.requireNonNull(runtime, "runtime");
        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException("invalid vector cut HTTP port");
        }
        this.server = HttpServer.create(new InetSocketAddress("127.0.0.1", port), 32);
        server.createContext("/cut", this::serveCut);
        server.setExecutor(requests);
    }

    /** 取得实际绑定端口，测试可使用端口零由系统分配。 */
    int port() {
        return server.getAddress().getPort();
    }

    /** 只有已持租约且切面重建完成后才能开始响应客户端。 */
    synchronized void start() {
        if (started || !runtime.isServing()) {
            throw new IllegalStateException("vector cut HTTP endpoint requires an active leader");
        }
        server.start();
        started = true;
    }

    /** 每次请求都经运行时二次检查资格，并返回代数与完整向量。 */
    private void serveCut(HttpExchange exchange) throws IOException {
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
            } catch (RuntimeException ex) {
                exchange.sendResponseHeaders(503, -1);
            }
        } finally {
            exchange.close();
        }
    }

    /** 停止新请求并等待已开始的回环读取退出。 */
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
