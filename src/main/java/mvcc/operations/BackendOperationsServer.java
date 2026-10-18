package mvcc.operations;

import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.Filter;
import mvcc.maintenance.*;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Supplier;

/** 只读运维接口：Prometheus、存活与就绪检查；冻结、恢复和切换不通过 HTTP 开放。 */
public final class BackendOperationsServer implements AutoCloseable {
    private final HttpServer server;
    private final ThreadPoolExecutor executor;
    private final MysqlProtocolAdmin admin;
    private final Supplier<MaintenanceDiagnostics> diagnostics;
    private final Supplier<MaintenanceService.Health> health;
    private final java.util.concurrent.atomic.AtomicBoolean stopRequested = new java.util.concurrent.atomic.AtomicBoolean();
    private final long createdAt = System.nanoTime();
    private final long maxMaintenanceSilenceMillis;

    /** 显式绑定监听地址，使用有限工作队列，不自动监听外部网络。 */
    public BackendOperationsServer(InetSocketAddress address, MysqlProtocolAdmin admin,
                                   Supplier<MaintenanceDiagnostics> diagnostics, Supplier<MaintenanceService.Health> health) {
        this(address, admin, diagnostics, health, java.time.Duration.ofSeconds(60));
    }
    /** 根据维护周期配置静默阈值，后台卡住或尚未首次完成时不能永久显示就绪。 */
    public BackendOperationsServer(InetSocketAddress address, MysqlProtocolAdmin admin,
                                   Supplier<MaintenanceDiagnostics> diagnostics, Supplier<MaintenanceService.Health> health,
                                   java.time.Duration maxMaintenanceSilence) {
        long silence = maxMaintenanceSilence.toMillis();
        if (silence < 1 || silence > 86_400_000) { throw new IllegalArgumentException("invalid maintenance silence threshold"); }
        maxMaintenanceSilenceMillis = silence;
        admin.requireWritable();
        this.admin = Objects.requireNonNull(admin); this.diagnostics = Objects.requireNonNull(diagnostics); this.health = Objects.requireNonNull(health);
        executor = new ThreadPoolExecutor(2, 2, 0, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(32),
                Thread.ofPlatform().daemon().name("azir-operations-http-", 0).factory(), new ThreadPoolExecutor.AbortPolicy());
        try { server = HttpServer.create(address, 16); }
        catch (IOException failure) { executor.shutdownNow(); throw new IllegalStateException("operations HTTP bind failed", failure); }
        try {
            server.setExecutor(executor);
            context("/health/live", exchange -> respond(exchange, 200, "text/plain; charset=utf-8", "alive\n"));
            context("/health/ready", exchange -> {
                String body; int code;
                try { body = readiness() ? "ready\n" : "not ready\n"; code = body.startsWith("ready") ? 200 : 503; }
                catch (RuntimeException failure) { body = "storage unavailable\n"; code = 503; }
                respond(exchange, code, "text/plain; charset=utf-8", body);
            });
            context("/metrics", exchange -> {
                String text; int code;
                try { text = prometheus(); code = 200; }
                catch (RuntimeException failure) { text = "azir_backend_ready 0\nazir_backend_observation_error 1\n"; code = 503; }
                respond(exchange, code, "text/plain; version=0.0.4; charset=utf-8", text);
            });
            server.start();
        } catch (RuntimeException | Error failure) {
            // HTTP 已绑定端口但处理器或线程初始化失败时，也必须立即归还监听资源。
            try { server.stop(0); } catch (RuntimeException | Error cleanup) { if (failure != cleanup) { failure.addSuppressed(cleanup); } }
            try { executor.shutdownNow(); } catch (RuntimeException | Error cleanup) { if (failure != cleanup) { failure.addSuppressed(cleanup); } }
            throw failure;
        }
    }
    /** 固定管理路径只接受精确匹配，HTTP 层不允许缓存就绪状态。 */
    private void context(String path, HttpHandler handler) {
        var context = server.createContext(path, handler); context.getFilters().add(new ReadOnlyFilter(path));
    }
    /** HEAD 返回与 GET 相同状态及响应头，但不发送响应体。 */
    private static void respond(HttpExchange exchange, int code, String contentType, String body) throws IOException {
        byte[] text = body.getBytes(StandardCharsets.UTF_8);
        try (exchange) {
            exchange.getResponseHeaders().set("Content-Type", contentType);
            boolean head = exchange.getRequestMethod().equals("HEAD");
            exchange.sendResponseHeaders(code, head ? -1 : text.length);
            if (!head) { exchange.getResponseBody().write(text); }
        }
    }
    /** 统一只读契约，错误路径和写方法在访问数据库之前就被拒绝。 */
    private static final class ReadOnlyFilter extends Filter {
        private final String path;
        /** 固定注册的路径，不能使用前缀匹配开放额外路由。 */
        ReadOnlyFilter(String path) { this.path = path; }
        /** 给服务器提供不含配置和凭据的过滤器描述。 */
        @Override public String description() { return "只读且禁止缓存的事务运维接口"; }
        /** 校验方法和路径后才执行健康或指标查询。 */
        @Override public void doFilter(HttpExchange exchange, Chain chain) throws IOException {
            exchange.getResponseHeaders().set("Cache-Control", "no-store");
            if (!exchange.getRequestURI().getRawPath().equals(path)) { respond(exchange, 404, "text/plain; charset=utf-8", "not found\n"); return; }
            if (!Set.of("GET", "HEAD").contains(exchange.getRequestMethod())) {
                exchange.getResponseHeaders().set("Allow", "GET, HEAD"); respond(exchange, 405, "text/plain; charset=utf-8", "method not allowed\n"); return;
            }
            chain.doFilter(exchange);
        }
    }
    /** 返回实际监听端口，支持测试使用随机可用端口。 */
    public int port() { return server.getAddress().getPort(); }
    /** 冻结、局部代次不一致、过期客户端或后台失败时不能报告就绪。 */
    public boolean readiness() {
        if (stopRequested.get()) { return false; }
        var states = admin.status(); var control = states.get("control");
        if (!control.namespace().equals(admin.state().namespace()) || control.generation() != admin.state().generation() || states.values().stream().anyMatch(s -> !s.mode().equals("ACTIVE") || !s.namespace().equals(control.namespace()) || s.generation() != control.generation())) { return false; }
        var work = health.get();
        if (work != null) {
            if (work.lastError() != null) { return false; }
            long age = work.lastFinished() == null ? TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - createdAt) : Math.max(0, java.time.Duration.between(work.lastFinished(), java.time.Instant.now()).toMillis());
            if (age > maxMaintenanceSilenceMillis) { return false; }
        }
        diagnostics.get(); return true;
    }
    /** 导出不含业务键、载荷、数据库 URL 或凭据的固定指标。 */
    public String prometheus() {
        var status = admin.state().status(); var work = health.get();
        boolean active = status.mode().equals("ACTIVE") && status.generation() == admin.state().generation();
        var values = new LinkedHashMap<String, Number>(); values.put("ready", readiness() ? 1 : 0);
        values.put("generation", status.generation()); values.put("retired_before", status.retiredBefore()); values.put("frozen", active ? 0 : 1);
        if (active) {
            var data = diagnostics.get(); values.put("safe_point", data.safePoint()); values.put("oldest_active_snapshot", data.oldestActiveSnapshot());
            values.put("active_snapshots", data.activeSnapshots()); values.put("expired_snapshots", data.expiredSnapshots()); values.put("pending_items", data.pendingItems());
        }
        if (work != null) {
            values.put("maintenance_successes_total", work.successes()); values.put("maintenance_failures_total", work.failures());
            values.put("maintenance_last_finished_seconds", work.lastFinished() == null ? 0 : work.lastFinished().getEpochSecond());
        }
        var text = new StringBuilder();
        values.forEach((name, value) -> text.append("azir_backend_").append(name).append("{backend=\"").append(admin.state().prefix()).append("\",namespace=\"")
                .append(status.namespace()).append("\"} ").append(value).append('\n'));
        return text.toString();
    }
    /** 停止监听并等待有限请求线程结束，不关闭业务数据库池。 */
    @Override public void close() {
        if (stopRequested.compareAndSet(false, true)) { server.stop(1); }
        executor.shutdownNow();
        try { if (!executor.awaitTermination(10, TimeUnit.SECONDS)) { throw new IllegalStateException("operations requests did not stop"); } }
        catch (InterruptedException failure) { Thread.currentThread().interrupt(); throw new IllegalStateException(failure); }
    }
}
