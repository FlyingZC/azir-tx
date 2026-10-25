package mvcc.wal.ha;

import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.proto.grpc.RaftServerProtocolServiceGrpc;
import org.apache.ratis.thirdparty.io.grpc.*;
import org.apache.ratis.thirdparty.io.grpc.netty.*;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
import org.apache.ratis.thirdparty.io.netty.handler.ssl.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

/** 真实套接字和 HTTP2 调用验证资源预算，本专项服务不冒充真实 Raft 共识。 */
class RatisTransportBudgetTest {
    @TempDir Path directory;
    private final List<org.apache.ratis.thirdparty.io.netty.channel.nio.NioEventLoopGroup> groups = new ArrayList<>();
    /** 事件循环由验收显式拥有，服务关闭以后逐个排空。 */
    @org.junit.jupiter.api.AfterEach void closeGroups() { for (var group : groups) { group.shutdownGracefully(0, 5, TimeUnit.SECONDS).syncUninterruptibly(); } }

    /** 不发送任何 TLS 数据的连接也要受总量限制，握手超时后名额能够再次使用。 */
    @Test void unauthenticatedTcpConnectionsAreCappedBeforeTls() throws Exception {
        var budget = new RatisTransportBudget(new RatisTransportLimits(2, 4, 8, 8192, 16, 1000, 5000, 60_000));
        var pki = new RatisTlsTestCertificates(directory); var certificate = pki.issue(UUID.randomUUID(), "n0", "localhost:19001"); var server = server(budget, certificate, new Service()).start();
        try (var first = new java.net.Socket("127.0.0.1", server.getPort()); var second = new java.net.Socket("127.0.0.1", server.getPort())) {
            await(() -> budget.stats().connections() == 2);
            try (var rejected = new java.net.Socket("127.0.0.1", server.getPort())) {
                await(() -> budget.stats().rejectedConnections() >= 1); rejected.setSoTimeout(2000);
                try { assertEquals(-1, rejected.getInputStream().read()); } catch (java.net.SocketException reset) { /* 内核拒绝可表现为 EOF 或复位。 */ }
            }
            await(() -> budget.stats().connections() == 0); assertEquals(2, budget.stats().peakConnections());
            var channel = channel(pki, server.getPort());
            try { assertTrue(RaftServerProtocolServiceGrpc.newBlockingStub(channel).withDeadlineAfter(5, TimeUnit.SECONDS).requestVote(RaftProtos.RequestVoteRequestProto.getDefaultInstance()).getServerReply().getSuccess()); }
            finally { channel.shutdownNow(); assertTrue(channel.awaitTermination(5, TimeUnit.SECONDS)); }
        } finally { server.shutdownNow(); assertTrue(server.awaitTermination(5, TimeUnit.SECONDS)); }
        await(() -> budget.stats().connections() == 0);
    }

    /** 不同 TLS 连接也共享调用预算，正常结束和取消都必须归还名额。 */
    @Test void rpcCapIsGlobalAndCancellationReleasesExactlyOnce() throws Exception {
        var budget = new RatisTransportBudget(new RatisTransportLimits(8, 2, 8, 8192, 16, 3000, 5000, 60_000)); var service = new Service();
        var pki = new RatisTlsTestCertificates(directory); var certificate = pki.issue(UUID.randomUUID(), "n0", "localhost:19001"); var server = server(budget, certificate, service).start(); var a = channel(pki, server.getPort()); var b = channel(pki, server.getPort());
        try {
            var first = RaftServerProtocolServiceGrpc.newStub(a).appendEntries(new Sink()); first.onNext(RaftProtos.AppendEntriesRequestProto.getDefaultInstance());
            var second = RaftServerProtocolServiceGrpc.newStub(b).appendEntries(new Sink()); second.onNext(RaftProtos.AppendEntriesRequestProto.getDefaultInstance()); await(() -> budget.stats().calls() == 2);
            var blocked = assertThrows(StatusRuntimeException.class, () -> RaftServerProtocolServiceGrpc.newBlockingStub(b).withDeadlineAfter(5, TimeUnit.SECONDS).requestVote(RaftProtos.RequestVoteRequestProto.getDefaultInstance())); assertEquals(Status.Code.RESOURCE_EXHAUSTED, blocked.getStatus().getCode());
            first.onCompleted(); await(() -> budget.stats().calls() == 1);
            assertTrue(RaftServerProtocolServiceGrpc.newBlockingStub(a).withDeadlineAfter(5, TimeUnit.SECONDS).requestVote(RaftProtos.RequestVoteRequestProto.getDefaultInstance()).getServerReply().getSuccess());
            second.onError(Status.CANCELLED.asRuntimeException()); await(() -> budget.stats().calls() == 0); assertEquals(2, budget.stats().peakCalls()); assertEquals(1, budget.stats().rejectedCalls());
        } finally { a.shutdownNow(); b.shutdownNow(); assertTrue(a.awaitTermination(5, TimeUnit.SECONDS)); assertTrue(b.awaitTermination(5, TimeUnit.SECONDS)); server.shutdownNow(); assertTrue(server.awaitTermination(5, TimeUnit.SECONDS)); }
        await(() -> budget.stats().connections() == 0); assertEquals(0, budget.stats().calls());
    }
    /** 错误预算不能在启动后变成无限连接配置。 */
    @Test void invalidLimitsAreRejected() { assertThrows(IllegalArgumentException.class, () -> new RatisTransportLimits(0, 1, 1, 8192, 16, 1000, 1000, 1000)); assertThrows(IllegalArgumentException.class, () -> new RatisTransportLimits(1, 1, 1, 8192, 16, 0, 1000, 1000)); }
    /** 实际 TLS 服务使用 JDK ALPN，与正式节点的预算组件相同。 */
    private Server server(RatisTransportBudget budget, RatisTlsTestCertificates.Entry certificate, Service service) throws Exception {
        var boss = new org.apache.ratis.thirdparty.io.netty.channel.nio.NioEventLoopGroup(1); var worker = new org.apache.ratis.thirdparty.io.netty.channel.nio.NioEventLoopGroup(1); groups.add(boss); groups.add(worker);
        var ssl = GrpcSslContexts.configure(SslContextBuilder.forServer(certificate.certificateFile().toFile(), certificate.keyFile().toFile()), SslProvider.JDK).build();
        return budget.configure(NettyServerBuilder.forAddress(new java.net.InetSocketAddress("127.0.0.1", 0)).bossEventLoopGroup(boss).workerEventLoopGroup(worker)).sslContext(ssl).addService(service).build();
    }
    /** 客户端只信任本次测试 CA，不跳过服务器主机名验证。 */
    private static ManagedChannel channel(RatisTlsTestCertificates pki, int port) throws Exception { var ssl = GrpcSslContexts.configure(SslContextBuilder.forClient(), SslProvider.JDK).trustManager(pki.ca.toFile()).build(); return NettyChannelBuilder.forAddress("localhost", port).sslContext(ssl).build(); }
    /** 用实际观察条件等待释放，不能以固定睡眠代替名额核对。 */
    private static void await(java.util.function.BooleanSupplier condition) throws Exception { long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(10); while (!condition.getAsBoolean() && System.nanoTime() < until) { TimeUnit.MILLISECONDS.sleep(10); } assertTrue(condition.getAsBoolean()); }
    /** 只提供有界回复和持有流，验证预算而非事务实现。 */
    private static final class Service extends RaftServerProtocolServiceGrpc.RaftServerProtocolServiceImplBase {
        /** 正常回复证明释放预算以后不是永久拒绝所有流量。 */
        @Override public void requestVote(RaftProtos.RequestVoteRequestProto request, StreamObserver<RaftProtos.RequestVoteReplyProto> observer) { observer.onNext(RaftProtos.RequestVoteReplyProto.newBuilder().setServerReply(RaftProtos.RaftRpcReplyProto.newBuilder().setSuccess(true)).build()); observer.onCompleted(); }
        /** 客户端主动结束以前保留实际调用占用。 */
        @Override public StreamObserver<RaftProtos.AppendEntriesRequestProto> appendEntries(StreamObserver<RaftProtos.AppendEntriesReplyProto> observer) { return new StreamObserver<>() {
            /** 接收任意有限测试消息，不执行共识。 */
            @Override public void onNext(RaftProtos.AppendEntriesRequestProto value) { }
            /** 取消由 gRPC 负责调用预算释放。 */
            @Override public void onError(Throwable failure) { }
            /** 正常结束触发服务关闭。 */
            @Override public void onCompleted() { observer.onCompleted(); }
        }; }
    }
    /** 客户端流回复无需持有无界结果队列。 */
    private static final class Sink implements StreamObserver<RaftProtos.AppendEntriesReplyProto> {
        /** 不保留响应体。 */
        @Override public void onNext(RaftProtos.AppendEntriesReplyProto value) { }
        /** 预期取消不会终止测试主线程。 */
        @Override public void onError(Throwable failure) { }
        /** 结束由预算统计独立验证。 */
        @Override public void onCompleted() { }
    }
}
