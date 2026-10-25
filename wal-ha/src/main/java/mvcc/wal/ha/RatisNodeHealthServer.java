package mvcc.wal.ha;

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

/** 仅供本机监控的只读探针，仅读取缓存，不执行 SQL、事务或多数派网络探测。 */
final class RatisNodeHealthServer implements AutoCloseable {
    private final HttpServer server;
    private final ThreadPoolExecutor workers;
    private final RatisNodeHost host;

    /** 仅绑定数字回环地址，线程、等待队列和 HTTP 连接都必须有明确上限。 */
    RatisNodeHealthServer(RatisNodeConfiguration config, RatisNodeHost host) throws IOException {
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
        } catch (RuntimeException | Error failure) {
            server.stop(0);
            workers.shutdown();
            throw failure;
        }
    }

    /** 独立进程从 JVM 启动参数设置预算，本组件不修改宿主进程的全局 HTTP 属性。 */
    static void requireBudgets() {
        for (var entry : Map.of("jdk.httpserver.maxConnections", 32, "sun.net.httpserver.maxIdleConnections", 8,
                "sun.net.httpserver.maxReqHeaders", 32, "sun.net.httpserver.maxReqTime", 5,
                "sun.net.httpserver.maxRspTime", 5, "sun.net.httpserver.idleInterval", 5).entrySet()) {
            String raw = System.getProperty(entry.getKey());
            int value;
            try { value = raw == null ? -1 : Integer.parseInt(raw); }
            catch (NumberFormatException invalid) { throw new IllegalArgumentException("explicit bounded health HTTP JVM settings required"); }
            if (value < 1 || value > entry.getValue()) { throw new IllegalArgumentException("explicit bounded health HTTP JVM settings required"); }
        }
        if (!"0".equals(System.getProperty("sun.net.httpserver.drainAmount"))) { throw new IllegalArgumentException("health HTTP must not drain request bodies"); }
    }

    /** 精确路由只接受 GET/HEAD，拒绝请求体，不暴露错误原因链或数据库地址。 */
    private void respond(HttpExchange exchange) throws IOException {
        try (exchange) {
            String method = exchange.getRequestMethod();
            String path = exchange.getRequestURI().getRawPath();
            int code;
            String body;
            if (!method.equals("GET") && !method.equals("HEAD")) {
                exchange.getResponseHeaders().set("Allow", "GET, HEAD");
                code = 405; body = "{\"error\":\"method_not_allowed\"}";
            } else if (exchange.getRequestURI().getRawQuery() != null || exchange.getRequestURI().getRawFragment() != null
                    || exchange.getRequestHeaders().containsKey("Transfer-Encoding")
                    || exchange.getRequestHeaders().containsKey("Content-Length") && !exchange.getRequestHeaders().get("Content-Length").equals(java.util.List.of("0"))) {
                code = 400; body = "{\"error\":\"unsupported_request\"}";
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

    /** 停止监听并等待探针工作者退出，超时保持句柄可再次关闭。 */
    @Override public void close() throws IOException {
        server.stop(0);
        workers.shutdown();
        try {
            if (!workers.awaitTermination(10, TimeUnit.SECONDS)) { throw new IOException("health HTTP workers did not drain; retry close"); }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IOException("health HTTP close interrupted", interrupted);
        }
    }
}
