package mvcc.wal.ha;

import mvcc.common.IsolationLevel;
import mvcc.wal.*;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.proto.grpc.RaftServerProtocolServiceGrpc;
import org.apache.ratis.protocol.*;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.io.grpc.*;
import org.apache.ratis.thirdparty.io.grpc.netty.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.*;

/** 真实双向 TLS 集群、伪装拒绝、快照追赶、成员替换与认证模式重启专项。 */
class RatisTlsClusterTest {
    @TempDir Path directory;

    /** 正常复制与身份拒绝同时验证，不能靠拒绝所有流量获得安全测试绿灯。 */
    @Test void authenticatedReplicationRejectsImpersonationAndPlaintextDowngrade() throws Exception {
        try (var cluster = new Cluster(directory)) {
            int leader = cluster.leader(); var serving = cluster.nodes.get(leader); var receipt = write(serving, new byte[]{1}); cluster.awaitAll(1);
            int source = (leader + 1) % 3; var target = cluster.peers.get(leader); var identity = cluster.entries.get(source);
            try (var client = client(cluster.group, cluster.security.get(source))) {
                var reply = client.io().sendStaleRead(Message.valueOf(ByteString.copyFrom(RatisReplicaStatus.request())), 0, target.getId()); assertTrue(reply.isSuccess()); assertEquals(target.getId().toString(), RatisReplicaStatus.decode(reply.getMessage().getContent().toByteArray()).node());
                assertEquals(Status.Code.PERMISSION_DENIED, assertThrows(StatusRuntimeException.class, () -> client.getGroupManagementApi(target.getId()).list()).getStatus().getCode());
                assertThrows(java.io.IOException.class, () -> client.io().sendReadOnly(Message.valueOf("business-data")));
            }
            var rogue = cluster.pki.issue(cluster.id, "rogue", cluster.freeAddress()); var unrelated = new RatisTlsTestCertificates(directory.resolve("foreign-ca")); var foreign = unrelated.issue(cluster.id, "foreign", cluster.freeAddress());
            rejectedVote(cluster, target, null, null, source, cluster.id, target.getId().toString(), false);
            rejectedVote(cluster, target, cluster.pki, null, source, cluster.id, target.getId().toString(), false);
            rejectedVote(cluster, target, cluster.pki, rogue, source, cluster.id, target.getId().toString(), false);
            rejectedVote(cluster, target, cluster.pki, foreign, source, cluster.id, target.getId().toString(), false);
            rejectedVote(cluster, target, unrelated, identity, source, cluster.id, target.getId().toString(), false);
            // 已登记但不在实际配置中的节点，以及合法证书伪造发送者、组或接收者，分别拒绝。
            rejectedVote(cluster, target, cluster.pki, cluster.outsider, source, cluster.id, target.getId().toString(), true);
            rejectedVote(cluster, target, cluster.pki, identity, (source + 1) % 3, cluster.id, target.getId().toString(), true);
            rejectedVote(cluster, target, cluster.pki, identity, source, UUID.randomUUID(), target.getId().toString(), true);
            rejectedVote(cluster, target, cluster.pki, identity, source, cluster.id, "wrong-receiver", true);
            assertEquals(leader, cluster.leader()); assertEquals(2, write(serving, new byte[]{2}).commitSequence()); cluster.awaitAll(2);
            // CA、SAN 主机名和指纹都正确，但登记端点对调时，出站仍必须拒绝。
            var swapped = new ArrayList<RatisTlsSecurity.PeerTrust>();
            for (int i = 0; i < 3; i++) { var entry = cluster.entries.get(i); swapped.add(new RatisTlsSecurity.PeerTrust(entry.id(), i == source ? entry.address() : cluster.entries.get(3 - source - i).address(), entry.trust().certificateSha256())); }
            var badTargets = RatisTlsSecurity.load(cluster.id, identity.id(), identity.identity(), RatisTlsTestCertificates.PASSWORD, cluster.pki.trust, RatisTlsTestCertificates.PASSWORD, swapped);
            try (var client = client(cluster.group, badTargets)) { assertThrows(java.io.IOException.class, () -> client.io().sendStaleRead(Message.valueOf(ByteString.copyFrom(RatisReplicaStatus.request())), 0, target.getId())); }
            cluster.stop(leader); int next = cluster.leader(); assertPair(cluster.nodes.get(next), new byte[]{2}); assertEquals(WalCommitLookup.Status.COMMITTED, cluster.nodes.get(next).lookupCommit(receipt.transactionId()).status());
            var plain = RaftGroup.valueOf(cluster.group.getGroupId(), cluster.peers.stream().map(peer -> RaftPeer.newBuilder(peer).setAddress("127.0.0.1:" + RatisEndpoint.parse(peer.getAddress()).port()).build()).toList());
            assertThrows(java.io.IOException.class, () -> RatisWalNode.openGenerationalLoopback(cluster.root(leader), cluster.peers.get(leader).getId(), plain, Set.of("a", "b"), WalReplicaBackendFactory.localOnly(), options(), limits()));
            cluster.start(leader); cluster.awaitAll(2); assertEquals(3, write(cluster.nodes.get(next), new byte[]{3}).commitSequence());
        }
    }

    /** TLS 覆盖真实大镜像安装、内部健康探针和管理切主，原始远程管理不需要开放。 */
    @Test void snapshotReplacementAndRestartRemainAuthenticated() throws Exception {
        try (var cluster = new Cluster(directory)) {
            int leader = cluster.leader(), removed = (leader + 1) % 3; var serving = cluster.nodes.get(leader); WalReceipt receipt = null; byte[] expected = null;
            for (int i = 1; i <= 10; i++) { expected = new byte[480 * 1024]; Arrays.fill(expected, (byte) i); var result = write(serving, expected); if (receipt == null) { receipt = result; } }
            cluster.awaitAll(10); cluster.stop(removed); long snapshot = serving.snapshot(); serving.purgeForTest(snapshot); assertTrue(serving.firstLogIndexForTest() > 2);
            var plan = serving.planReplacement(cluster.peers.get(removed).getId(), cluster.freeAddress(), 10_000); var fresh = cluster.pki.issue(cluster.id, plan.replacement().getId().toString(), plan.replacement().getAddress());
            assertThrows(java.io.IOException.class, () -> serving.beginReplacement(plan, 10_000));
            for (var policy : cluster.security) { policy.authorizePeer(fresh.trust()); }
            var freshSecurity = RatisTlsSecurity.load(cluster.id, fresh.id(), fresh.identity(), RatisTlsTestCertificates.PASSWORD, cluster.pki.trust, RatisTlsTestCertificates.PASSWORD, cluster.security.get(leader).trustedPeers());
            try (var joined = RatisWalNode.openAuthenticated(directory.resolve("replacement"), plan.replacement().getId(), plan.joiningGroup(), Set.of("a", "b"), WalReplicaBackendFactory.localOnly(), options(), limits(), freshSecurity)) {
                assertEquals(RatisWalNode.ReplacementPhase.LEARNER_ADDED, serving.beginReplacement(plan, 30_000).phase());
                long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(60); while ((joined.appliedSequence() < 10 || joined.snapshotIndex() < snapshot || !joined.ready()) && System.nanoTime() < until) { TimeUnit.MILLISECONDS.sleep(30); }
                assertTrue(joined.ready()); assertTrue(joined.snapshotIndex() >= snapshot); assertEquals(10, joined.appliedSequence()); assertTrue(joined.durableSnapshotAckIndex() >= snapshot);
                assertEquals(RatisWalNode.ReplacementPhase.COMPLETE, serving.completeReplacement(plan, 30_000).phase()); serving.transferLeadership(plan.replacement().getId(), 30_000); cluster.awaitLeader(joined);
                assertPair(joined, expected); assertEquals(WalCommitLookup.Status.COMMITTED, joined.lookupCommit(receipt.transactionId()).status()); assertEquals(11, write(joined, new byte[]{9}).commitSequence()); joined.snapshot();
                rejectedVote(cluster, plan.replacement(), cluster.pki, cluster.entries.get(removed), removed, cluster.id, fresh.id(), true);
            }
            try (var restarted = RatisWalNode.openAuthenticated(directory.resolve("replacement"), plan.replacement().getId(), plan.joiningGroup(), Set.of("a", "b"), WalReplicaBackendFactory.localOnly(), options(), limits(), freshSecurity)) {
                long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(60); while (restarted.appliedSequence() < 11 && System.nanoTime() < until) { TimeUnit.MILLISECONDS.sleep(30); }
                int current = cluster.leader(); cluster.nodes.get(current).transferLeadership(plan.replacement().getId(), 30_000); cluster.awaitLeader(restarted); assertPair(restarted, new byte[]{9}); assertEquals(WalCommitLookup.Status.COMMITTED, restarted.lookupCommit(receipt.transactionId()).status()); assertEquals(12, write(restarted, new byte[]{10}).commitSequence());
            }
        }
    }

    /** 两张证书重叠后滚动重启，去掉旧指纹以后旧私钥不能重新连入当前成员。 */
    @Test void certificateRotationOverlapsPinsThenRejectsOldKey() throws Exception {
        try (var cluster = new Cluster(directory)) {
            int leader = cluster.leader(), rotating = (leader + 1) % 3; var receipt = write(cluster.nodes.get(leader), new byte[]{1}); cluster.awaitAll(1);
            var previous = cluster.entries.get(rotating); Path old = directory.resolve("old-identity"); Files.createDirectories(old);
            Path cert = Files.copy(previous.certificateFile(), old.resolve("node.pem")), key = Files.copy(previous.keyFile(), old.resolve("node.key"));
            var oldCredential = new RatisTlsTestCertificates.Entry(previous.id(), previous.address(), previous.identity(), cert, key, previous.certificate());
            var fresh = cluster.pki.issue(cluster.id, previous.id(), previous.address()); assertNotEquals(previous.trust().certificateSha256(), fresh.trust().certificateSha256());
            var pins = new HashSet<>(previous.trust().certificateSha256()); pins.addAll(fresh.trust().certificateSha256()); var overlap = new RatisTlsSecurity.PeerTrust(fresh.id(), fresh.address(), pins);
            for (var policy : cluster.security) { policy.authorizePeer(overlap); }
            cluster.stop(rotating); assertEquals(2, write(cluster.nodes.get(leader), new byte[]{2}).commitSequence());
            cluster.security.set(rotating, RatisTlsSecurity.load(cluster.id, fresh.id(), fresh.identity(), RatisTlsTestCertificates.PASSWORD, cluster.pki.trust, RatisTlsTestCertificates.PASSWORD, cluster.security.get(leader).trustedPeers()));
            cluster.start(rotating); cluster.awaitAll(2); for (var policy : cluster.security) { policy.authorizePeer(fresh.trust()); }
            rejectedVote(cluster, cluster.peers.get(leader), cluster.pki, oldCredential, rotating, cluster.id, cluster.peers.get(leader).getId().toString(), false);
            assertEquals(3, write(cluster.nodes.get(leader), new byte[]{3}).commitSequence()); cluster.awaitAll(3);
            cluster.nodes.get(leader).transferLeadership(cluster.peers.get(rotating).getId(), 30_000); cluster.awaitLeader(cluster.nodes.get(rotating)); assertPair(cluster.nodes.get(rotating), new byte[]{3});
            assertEquals(WalCommitLookup.Status.COMMITTED, cluster.nodes.get(rotating).lookupCommit(receipt.transactionId()).status());
        }
    }

    /** 高任期请求被拒绝后不能影响正在运行的集群，RPC 错误不能只靠客户端超时判断身份保护。 */
    private static void rejectedVote(Cluster cluster, RaftPeer target, RatisTlsTestCertificates pki, RatisTlsTestCertificates.Entry certificate, int declaredSource, UUID group, String receiver, boolean permission) throws Exception {
        var endpoint = RatisEndpoint.parse(target.getAddress()); var builder = NettyChannelBuilder.forAddress(endpoint.host(), endpoint.port());
        if (pki == null) { builder.usePlaintext(); }
        else { var ssl = GrpcSslContexts.configure(org.apache.ratis.thirdparty.io.netty.handler.ssl.SslContextBuilder.forClient(), org.apache.ratis.thirdparty.io.netty.handler.ssl.SslProvider.JDK).trustManager(pki.ca.toFile()); if (certificate != null) { ssl.keyManager(certificate.certificateFile().toFile(), certificate.keyFile().toFile()); } builder.sslContext(ssl.build()); }
        var channel = builder.build();
        try {
            var rpc = RaftProtos.RaftRpcRequestProto.newBuilder().setRequestorId(ByteString.copyFromUtf8("n" + declaredSource)).setReplyId(ByteString.copyFromUtf8(receiver)).setRaftGroupId(org.apache.ratis.util.ProtoUtils.toRaftGroupIdProtoBuilder(RaftGroupId.valueOf(group)));
            var request = RaftProtos.RequestVoteRequestProto.newBuilder().setServerRequest(rpc).setCandidateTerm(1000).setCandidateLastEntry(RaftProtos.TermIndexProto.newBuilder().setTerm(0).setIndex(0)).build();
            var failure = assertThrows(StatusRuntimeException.class, () -> RaftServerProtocolServiceGrpc.newBlockingStub(channel).withDeadlineAfter(3, TimeUnit.SECONDS).requestVote(request));
            if (permission) { assertEquals(Status.Code.PERMISSION_DENIED, failure.getStatus().getCode()); }
            else { assertTrue(Set.of(Status.Code.UNAVAILABLE, Status.Code.UNAUTHENTICATED).contains(failure.getStatus().getCode()), failure.getStatus().toString()); }
        } finally { channel.shutdownNow(); assertTrue(channel.awaitTermination(5, TimeUnit.SECONDS)); }
    }
    /** 认证客户端只用于内部状态和拒绝测试，关闭自动重试以保留真实拒绝结果。 */
    private static RaftClient client(RaftGroup group, RatisTlsSecurity security) { var properties = new RaftProperties(); org.apache.ratis.client.RaftClientConfigKeys.Rpc.setRequestTimeout(properties, org.apache.ratis.util.TimeDuration.valueOf(3, TimeUnit.SECONDS)); return RaftClient.newBuilder().setRaftGroup(group).setProperties(properties).setParameters(security.parameters()).setRetryPolicy(org.apache.ratis.retry.RetryPolicies.noRetry()).build(); }
    /** 每次事务写入两片同一载荷，用于检查完整快照和永久凭据。 */
    private static WalReceipt write(RatisWalNode node, byte[] value) { var tx = node.begin(IsolationLevel.REPEATABLE_READ); node.put(tx, "a", "k", value); node.put(tx, "b", "k", value); return node.commit(tx); }
    /** 单一 RR 语句核对两片完整字节。 */
    private static void assertPair(RatisWalNode node, byte[] expected) { var tx = node.begin(IsolationLevel.REPEATABLE_READ); var view = node.beginReadStatement(tx); for (String shard : List.of("a", "b")) { assertArrayEquals(expected, node.get(tx, shard, "k", view).orElseThrow()); } node.rollback(tx); }
    /** 业务内存与等待均受限，不进行吞吐压测。 */
    private static WalOptions options() { return WalOptions.defaults().withTimeout(10_000).withHotCacheBytes(0); }
    /** 快照总量有明确限制。 */
    private static WalBackupOptions limits() { return new WalBackupOptions(32L * 1024 * 1024, 8, 1024 * 1024, 60_000); }

    /** 三个真实 Ratis 服务使用各自的 TLS 密钥和存储目录。 */
    private static final class Cluster implements AutoCloseable {
        final Path directory; final UUID id = UUID.randomUUID(); final List<RaftPeer> peers = new ArrayList<>(); final List<RatisWalNode> nodes = new ArrayList<>(); final List<RatisTlsSecurity> security = new ArrayList<>(); final List<RatisTlsTestCertificates.Entry> entries = new ArrayList<>(); final RatisTlsTestCertificates pki; final RatisTlsTestCertificates.Entry outsider; final RaftGroup group;
        /** DNS 端点验证正式装配，而非依赖明文回环专用入口。 */
        Cluster(Path directory) throws Exception {
            this.directory = directory; pki = new RatisTlsTestCertificates(directory.resolve("pki"));
            for (int i = 0; i < 3; i++) { String address = freeAddress(); peers.add(RaftPeer.newBuilder().setId("n" + i).setAddress(address).build()); entries.add(pki.issue(id, "n" + i, address)); nodes.add(null); }
            outsider = pki.issue(id, "outsider", freeAddress()); var trust = new ArrayList<RatisTlsSecurity.PeerTrust>(); for (var entry : entries) { trust.add(entry.trust()); } trust.add(outsider.trust());
            group = RaftGroup.valueOf(RaftGroupId.valueOf(id), peers);
            try { for (int i = 0; i < 3; i++) { var entry = entries.get(i); security.add(RatisTlsSecurity.load(id, entry.id(), entry.identity(), RatisTlsTestCertificates.PASSWORD, pki.trust, RatisTlsTestCertificates.PASSWORD, trust)); start(i); } }
            catch (Exception failure) { try { close(); } catch (Exception close) { failure.addSuppressed(close); } throw failure; }
        }
        /** 每个节点目录独立，AUTHENTICATED 标记也独立保存。 */
        Path root(int node) { return directory.resolve("n" + node); }
        /** 重新打开沿用稳定身份和自己的 TLS 凭据。 */
        void start(int node) throws Exception { nodes.set(node, RatisWalNode.openAuthenticated(root(node), peers.get(node).getId(), group, Set.of("a", "b"), WalReplicaBackendFactory.localOnly(), options(), limits(), security.get(node))); }
        /** 只关闭本测试持有的节点。 */
        void stop(int node) throws Exception { var old = nodes.set(node, null); if (old != null) { old.close(); } }
        /** 观察真实选举状态，不能把列表顺序当作领导者。 */
        int leader() throws Exception { long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(30); while (System.nanoTime() < until) { for (int i = 0; i < nodes.size(); i++) { if (nodes.get(i) != null && nodes.get(i).leaderEpoch() > 0) { return i; } } TimeUnit.MILLISECONDS.sleep(30); } throw new AssertionError("no authenticated leader"); }
        /** 管理切主后等待指定节点真正就绪。 */
        void awaitLeader(RatisWalNode node) throws Exception { long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(30); while (node.leaderEpoch() < 1 && System.nanoTime() < until) { TimeUnit.MILLISECONDS.sleep(30); } assertTrue(node.leaderEpoch() > 0); }
        /** 全部在线节点实际应用确认事务后才执行故障。 */
        void awaitAll(long sequence) throws Exception { long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(30); while (nodes.stream().filter(Objects::nonNull).anyMatch(node -> node.appliedSequence() < sequence) && System.nanoTime() < until) { TimeUnit.MILLISECONDS.sleep(30); } for (var node : nodes) { if (node != null) { assertTrue(node.appliedSequence() >= sequence); } } }
        /** 仅分配开发机空闲临时端口，不使用现有服务。 */
        String freeAddress() throws Exception { try (var socket = new java.net.ServerSocket(0, 1, java.net.InetAddress.getLoopbackAddress())) { return "localhost:" + socket.getLocalPort(); } }
        /** 异常也完整关闭全部自有节点，不中断无关 JVM。 */
        @Override public void close() throws Exception { Exception failure = null; for (int i = 0; i < nodes.size(); i++) { try { stop(i); } catch (Exception close) { if (failure == null) { failure = close; } else { failure.addSuppressed(close); } } } if (failure != null) { throw failure; } }
    }
}
