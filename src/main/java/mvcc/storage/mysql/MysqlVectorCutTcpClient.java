package mvcc.storage.mysql;

import mvcc.common.ErrorCode;
import mvcc.common.MvccException;

import javax.sql.DataSource;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** 回环 TCP 切面客户端，按中心租约代数发现并验证每个二进制响应。 */
final class MysqlVectorCutTcpClient implements AutoCloseable {
    private final MysqlVectorCutLeaderLease leases;
    private final List<String> routes;
    private final int stripes;
    private final ConcurrentLinkedQueue<Channel> idle = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean closed = new AtomicBoolean();
    private volatile Endpoint cached;

    /** 端点在中心期限前保守失效，不能无限复用旧领导者。 */
    private record Endpoint(String ownerId, long generation, long validUntilNanos) {
        /** 只接受本地单调时钟仍处于保守租期内的地址。 */
        boolean valid() {
            return validUntilNanos - System.nanoTime() > 0;
        }
    }

    /** 一条复用连接同一时刻只借给一个读请求，旧代数连接直接销毁。 */
    private record Channel(Endpoint endpoint, Socket socket,
                           DataInputStream input, DataOutputStream output) implements AutoCloseable {
        /** 关闭底层连接，不把损坏的帧留给下一次读取。 */
        @Override
        public void close() {
            try {
                socket.close();
            } catch (IOException ignored) {
                // 连接销毁失败只影响本次网络资源回收，不使旧代数重新可用。
            }
        }
    }

    /** 固定读侧路由布局，与中心持久租约和响应体均须完全一致。 */
    MysqlVectorCutTcpClient(DataSource center, Map<String, DataSource> shards, int stripes) {
        this(center, shards, stripes, false);
    }

    /** 正式读侧发现正式租约表，不接受容量实验领导者。 */
    static MysqlVectorCutTcpClient forProduct(DataSource center,
                                               Map<String, DataSource> shards, int stripes) {
        return new MysqlVectorCutTcpClient(center, shards, stripes, true);
    }

    /** 两套中心租约共享代数校验和有界连接池。 */
    private MysqlVectorCutTcpClient(DataSource center, Map<String, DataSource> shards,
                                    int stripes, boolean product) {
        Objects.requireNonNull(shards, "shards");
        leases = product ? MysqlVectorCutLeaderLease.forProduct(center, shards, stripes)
                : new MysqlVectorCutLeaderLease(center, shards, stripes);
        routes = shards.keySet().stream().sorted().toList();
        this.stripes = stripes;
    }

    /** 单次请求读完整切面；旧端点拒绝或连接失败后再发现一次。 */
    MysqlVectorCutLeaderRuntime.PublishedView currentView() {
        for (int attempt = 0; attempt < 2; attempt++) {
            Endpoint endpoint = resolve();
            Channel channel = null;
            boolean reusable = false;
            try {
                channel = borrow(endpoint);
                channel.output().writeByte(1);
                channel.output().flush();
                int size = channel.input().readInt();
                if (size >= 1 && size <= 1024 * 1024) {
                    byte[] body = channel.input().readNBytes(size);
                    var view = MysqlVectorCutWire.decode(body, routes, stripes);
                    if (view.generation() == endpoint.generation() && endpoint.valid()) {
                        reusable = true;
                        return view;
                    }
                }
                invalidate(endpoint);
            } catch (IOException | IllegalArgumentException ex) {
                invalidate(endpoint);
            } finally {
                if (channel != null) {
                    if (reusable) {
                        recycle(channel, endpoint);
                    } else {
                        channel.close();
                    }
                }
            }
        }
        throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                "no current TCP vector cut leader answered with the expected generation", true, null);
    }

    /** 优先复用当前代数的空闲连接，避免每条 RC 都建立 TCP 短连接。 */
    private Channel borrow(Endpoint endpoint) throws IOException {
        Channel channel;
        while ((channel = idle.poll()) != null) {
            if (!channel.socket().isClosed()
                    && channel.endpoint().generation() == endpoint.generation()
                    && channel.endpoint().ownerId().equals(endpoint.ownerId())) {
                return channel;
            }
            channel.close();
        }
        Socket socket = new Socket();
        try {
            socket.setTcpNoDelay(true);
            int port = Integer.parseInt(endpoint.ownerId().substring("127.0.0.1:".length()));
            socket.connect(new InetSocketAddress("127.0.0.1", port), 500);
            socket.setSoTimeout(2_000);
            return new Channel(endpoint, socket,
                    new DataInputStream(socket.getInputStream()),
                    new DataOutputStream(socket.getOutputStream()));
        } catch (IOException | RuntimeException ex) {
            socket.close();
            throw ex;
        }
    }

    /** 仅把仍属于当前租约的完整帧连接放回有界空闲池。 */
    private void recycle(Channel channel, Endpoint endpoint) {
        Endpoint latest = cached;
        if (closed.get() || !endpoint.valid() || latest == null
                || latest.generation() != endpoint.generation()
                || !latest.ownerId().equals(endpoint.ownerId()) || idle.size() >= 64) {
            channel.close();
        } else {
            idle.offer(channel);
        }
    }

    /** 仅在缓存过期时读中心租约，并以请求开始时间计算保守本地截止。 */
    private synchronized Endpoint resolve() {
        if (closed.get()) {
            throw new IllegalStateException("TCP vector cut client is closed");
        }
        Endpoint current = cached;
        if (current != null && current.valid()) {
            return current;
        }
        long started = System.nanoTime();
        var leader = leases.discover().orElseThrow(() ->
                new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                        "TCP vector cut has no active leader", true, null));
        requireLoopbackEndpoint(leader.ownerId());
        long margin = Math.max(25, leader.remainingMillis() / 5);
        if (leader.remainingMillis() <= margin) {
            throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    "TCP vector cut leader is too close to expiry", true, null);
        }
        current = new Endpoint(leader.ownerId(), leader.generation(),
                started + TimeUnit.MILLISECONDS.toNanos(leader.remainingMillis() - margin));
        cached = current;
        return current;
    }

    /** 实验端点只允许合法的数字回环 IPv4 地址与端口。 */
    private static void requireLoopbackEndpoint(String ownerId) {
        if (!ownerId.startsWith("127.0.0.1:")) {
            throw new IllegalArgumentException("TCP vector cut owner is not a loopback endpoint");
        }
        try {
            int port = Integer.parseInt(ownerId.substring("127.0.0.1:".length()));
            if (port < 1 || port > 65535) {
                throw new IllegalArgumentException("TCP vector cut owner port is invalid");
            }
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException("TCP vector cut owner port is invalid", ex);
        }
    }

    /** 清理本次失败的旧端点，不覆盖其他读取线程已刷新出的新地址。 */
    private synchronized void invalidate(Endpoint endpoint) {
        if (cached == endpoint) {
            cached = null;
            Channel channel;
            while ((channel = idle.poll()) != null) {
                channel.close();
            }
        }
    }

    /** 关闭所有空闲 TCP 连接，借出连接归还时会自行销毁。 */
    @Override
    public void close() {
        closed.set(true);
        Channel channel;
        while ((channel = idle.poll()) != null) {
            channel.close();
        }
    }
}
