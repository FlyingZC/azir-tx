package mvcc.storage.mysql;

import mvcc.common.ErrorCode;
import mvcc.common.MvccException;

import javax.sql.DataSource;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/** 只读远端切面客户端，缓存当前端点并校验中心代数与响应路由。 */
final class MysqlVectorCutRemoteClient {
    private final MysqlVectorCutLeaderLease leases;
    private final List<String> routes;
    private final int stripes;
    private final HttpClient client = HttpClient.newBuilder()
            .connectTimeout(Duration.ofMillis(500)).build();
    private volatile Endpoint cached;

    /** 缓存期限早于中心租约结束，过期或请求失败再重新发现。 */
    private record Endpoint(String ownerId, long generation, long validUntilNanos) {
        /** 已过期端点不得被后续新事务复用。 */
        boolean valid() {
            return validUntilNanos - System.nanoTime() > 0;
        }
    }

    /** 使用同一布局配置构造发现器，读响应中的分片顺序必须完全一致。 */
    MysqlVectorCutRemoteClient(DataSource center, Map<String, DataSource> shards, int stripes) {
        Objects.requireNonNull(shards, "shards");
        this.leases = new MysqlVectorCutLeaderLease(center, shards, stripes);
        this.routes = shards.keySet().stream().sorted().toList();
        this.stripes = stripes;
    }

    /** 一次 RC 语句取得远端当前切面；旧端点失效时重新发现一次。 */
    MysqlVectorCutLeaderRuntime.PublishedView currentView() {
        for (int attempt = 0; attempt < 2; attempt++) {
            Endpoint endpoint = resolve();
            try {
                HttpRequest request = HttpRequest.newBuilder(URI.create("http://" + endpoint.ownerId() + "/cut"))
                        .timeout(Duration.ofSeconds(2)).GET().build();
                HttpResponse<byte[]> response = client.send(request, HttpResponse.BodyHandlers.ofByteArray());
                if (response.statusCode() == 200) {
                    var view = MysqlVectorCutWire.decode(response.body(), routes, stripes);
                    if (view.generation() == endpoint.generation() && endpoint.valid()) {
                        return view;
                    }
                }
                invalidate(endpoint);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new MvccException(ErrorCode.STORAGE_UNAVAILABLE,
                        "remote vector cut read interrupted", true, null, ex);
            } catch (IOException | IllegalArgumentException ex) {
                invalidate(endpoint);
            }
        }
        throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                "no current vector cut leader answered with the expected generation", true, null);
    }

    /** 端点缓存只覆盖租约的保守前段，避免每条 RC 都读控制库。 */
    private synchronized Endpoint resolve() {
        Endpoint current = cached;
        if (current != null && current.valid()) {
            return current;
        }
        long started = System.nanoTime();
        var leader = leases.discover().orElseThrow(() ->
                new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                        "vector cut has no active leader", true, null));
        requireLoopbackEndpoint(leader.ownerId());
        long margin = Math.max(25, leader.remainingMillis() / 5);
        if (leader.remainingMillis() <= margin) {
            throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    "vector cut leader is too close to expiry", true, null);
        }
        current = new Endpoint(leader.ownerId(), leader.generation(),
                started + TimeUnit.MILLISECONDS.toNanos(leader.remainingMillis() - margin));
        cached = current;
        return current;
    }

    /** 回环实验只允许数字 IPv4 回环地址和有效端口，避免错误租约指向外部服务。 */
    private static void requireLoopbackEndpoint(String ownerId) {
        if (!ownerId.startsWith("127.0.0.1:")) {
            throw new IllegalArgumentException("vector cut owner is not a loopback endpoint");
        }
        String portText = ownerId.substring("127.0.0.1:".length());
        try {
            int port = Integer.parseInt(portText);
            if (port < 1 || port > 65535) {
                throw new IllegalArgumentException("vector cut owner port is invalid");
            }
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException("vector cut owner port is invalid", ex);
        }
    }

    /** 只清理本次失败的旧端点，保留其他线程刚刷新的新领导者。 */
    private synchronized void invalidate(Endpoint endpoint) {
        if (cached == endpoint) {
            cached = null;
        }
    }
}
