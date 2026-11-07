package com.flyingzc.azir.wal.ha;

import org.apache.ratis.thirdparty.io.grpc.*;
import org.apache.ratis.thirdparty.io.grpc.netty.NettyServerBuilder;
import org.apache.ratis.thirdparty.io.netty.channel.Channel;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelOption;
import org.apache.ratis.thirdparty.io.netty.channel.socket.nio.*;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/** 在 TLS 握手之前限制已接受 TCP 连接，并限制节点全部服务的活动 RPC 总量。 */
final class RatisTransportBudget {
    private final RatisTransportLimits limits;
    private final Semaphore connections, calls;
    private final AtomicInteger connectionCount = new AtomicInteger(), callCount = new AtomicInteger(), peakConnections = new AtomicInteger(), peakCalls = new AtomicInteger();
    private final AtomicLong rejectedConnections = new AtomicLong(), rejectedCalls = new AtomicLong();
    /** 一个节点的全部服务共用同一预算，不能按连接分别放大总量。 */
    RatisTransportBudget(RatisTransportLimits limits) { this.limits = java.util.Objects.requireNonNull(limits); connections = new Semaphore(limits.connections()); calls = new Semaphore(limits.calls()); }

    /** NIO 接收器先限连接，原有官方 TLS、HTTP2 和消息编解码继续保留。 */
    NettyServerBuilder configure(NettyServerBuilder builder) {
        return builder.channelFactory(() -> new Listener(this)).withOption(ChannelOption.SO_BACKLOG, limits.listenBacklog())
                .maxConcurrentCallsPerConnection(limits.streamsPerConnection()).maxInboundMetadataSize(limits.metadataBytes())
                .handshakeTimeout(limits.handshakeMillis(), TimeUnit.MILLISECONDS).maxConnectionIdle(limits.idleMillis(), TimeUnit.MILLISECONDS)
                .maxConnectionAge(limits.connectionAgeMillis(), TimeUnit.MILLISECONDS).maxConnectionAgeGrace(30, TimeUnit.SECONDS).intercept(new Calls());
    }
    /** 只读取内存计数，不等待网络或业务状态锁。 */
    RatisWalNode.TransportStats stats() { return new RatisWalNode.TransportStats(connectionCount.get(), peakConnections.get(), rejectedConnections.get(), callCount.get(), peakCalls.get(), rejectedCalls.get()); }
    /** 超额连接在创建 TLS 管道以前拒绝。 */
    private boolean acquireConnection() { if (!connections.tryAcquire()) { rejectedConnections.incrementAndGet(); return false; } peakConnections.accumulateAndGet(connectionCount.incrementAndGet(), Math::max); return true; }
    /** 真实套接字关闭以后才能释放名额。 */
    private void releaseConnection() { connectionCount.decrementAndGet(); connections.release(); }
    /** 整个节点的 RPC 共用名额，不让多个连接绕过上限。 */
    private boolean acquireCall() { if (!calls.tryAcquire()) { rejectedCalls.incrementAndGet(); return false; } peakCalls.accumulateAndGet(callCount.incrementAndGet(), Math::max); return true; }

    /** 入站监听器在 accept 边界限量，不依赖已经成功 TLS 握手的 transportReady。 */
    private static final class Listener extends NioServerSocketChannel {
        private final RatisTransportBudget owner;
        /** 与官方 NIO 事件循环使用同类通道。 */
        Listener(RatisTransportBudget owner) { this.owner = owner; }
        /** 未通过预算的原始套接字立即关闭，不注册到子事件循环或分配 SSL 引擎。 */
        @Override protected int doReadMessages(List<Object> messages) throws Exception {
            var socket = javaChannel().accept(); if (socket == null) { return 0; }
            if (!owner.acquireConnection()) { socket.close(); return 0; }
            try { messages.add(new Connection(this, socket, owner)); return 1; }
            catch (Throwable failure) { owner.releaseConnection(); socket.close(); throw failure; }
        }
    }
    /** 每个已接受连接释放一次名额，注册失败的强制关闭也经过 doClose。 */
    private static final class Connection extends NioSocketChannel {
        private final RatisTransportBudget owner; private final AtomicBoolean released = new AtomicBoolean();
        /** 只封装已取得名额的内核套接字。 */
        Connection(Channel parent, java.nio.channels.SocketChannel socket, RatisTransportBudget owner) { super(parent, socket); this.owner = owner; }
        /** 正常关闭、握手失败与关闭前取消都不能重复释放。 */
        @Override protected void doClose() throws Exception { try { super.doClose(); } finally { if (released.compareAndSet(false, true)) { owner.releaseConnection(); } } }
    }
    /** 调用名额覆盖认证拦截器与实际服务，拒绝和取消都可安全释放。 */
    private final class Calls implements ServerInterceptor {
        /** 先限制总调用数，再允许解析或执行本节点服务。 */
        @Override public <Q, S> ServerCall.Listener<Q> interceptCall(ServerCall<Q, S> call, Metadata headers, ServerCallHandler<Q, S> next) {
            if (!acquireCall()) { call.close(Status.RESOURCE_EXHAUSTED.withDescription("WAL RPC budget exhausted"), new Metadata()); return new ServerCall.Listener<>() { }; }
            var released = new AtomicBoolean(); Runnable release = () -> { if (released.compareAndSet(false, true)) { callCount.decrementAndGet(); calls.release(); } };
            var guarded = new ForwardingServerCall.SimpleForwardingServerCall<Q, S>(call) {
                /** 服务主动拒绝也必须归还名额，不依赖客户端配合关闭。 */
                @Override public void close(Status status, Metadata trailers) { try { super.close(status, trailers); } finally { release.run(); } }
            };
            try { return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(next.startCall(guarded, headers)) {
                /** 客户端取消或连接断开归还名额。 */
                @Override public void onCancel() { try { super.onCancel(); } finally { release.run(); } }
                /** 正常调用终止同样幂等归还。 */
                @Override public void onComplete() { try { super.onComplete(); } finally { release.run(); } }
            }; } catch (Throwable failure) { release.run(); throw failure; }
        }
    }
}
