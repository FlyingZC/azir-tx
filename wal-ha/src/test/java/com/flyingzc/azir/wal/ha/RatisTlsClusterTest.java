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

package com.flyingzc.azir.wal.ha;

import com.flyingzc.azir.wal.WalBackupOptions;
import com.flyingzc.azir.wal.WalCommitLookup;
import com.flyingzc.azir.wal.WalOptions;
import com.flyingzc.azir.wal.WalReceipt;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.thirdparty.io.grpc.Status;
import org.apache.ratis.thirdparty.io.grpc.StatusRuntimeException;
import org.apache.ratis.thirdparty.io.grpc.netty.GrpcSslContexts;
import org.apache.ratis.thirdparty.io.grpc.netty.NettyChannelBuilder;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Assertions;

import com.flyingzc.azir.common.IsolationLevel;

import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.proto.grpc.RaftServerProtocolServiceGrpc;

import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.concurrent.TimeUnit;

/**
 * Real two-way TLS cluster, disguise refusal, snapshot chase, member substitution and authentication mode reset.
 */
class RatisTlsClusterTest {

    @TempDir
    Path directory;

    /**
     * Normal copying and identity rejection verify cannot obtain a security test green light by rejecting all traffic.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void authenticatedReplicationRejectsImpersonationAndPlaintextDowngrade() throws Exception {
        try (var cluster = new Cluster(directory)) {
            int leader = cluster.leader();
            var serving = cluster.nodes.get(leader);
            var receipt = write(serving, new byte[]{1});
            cluster.awaitAll(1);
            int source = (leader + 1) % 3;
            var target = cluster.peers.get(leader);
            var identity = cluster.entries.get(source);
            try (var client = client(cluster.group, cluster.security.get(source))) {
                var reply = client.io().sendStaleRead(Message.valueOf(ByteString.copyFrom(RatisReplicaStatus.request())), 0, target.getId());
                assertTrue(reply.isSuccess());
                Assertions.assertEquals(target.getId().toString(), RatisReplicaStatus.decode(reply.getMessage().getContent().toByteArray()).node());
                Assertions.assertEquals(Status.Code.PERMISSION_DENIED, assertThrows(StatusRuntimeException.class, () -> client.getGroupManagementApi(target.getId()).list()).getStatus().getCode());
                assertThrows(java.io.IOException.class, () -> client.io().sendReadOnly(Message.valueOf("business-data")));
            }
            var rogue = cluster.pki.issue(cluster.id, "rogue", cluster.freeAddress());
            var unrelated = new RatisTlsTestCertificates(directory.resolve("foreign-ca"));
            var foreign = unrelated.issue(cluster.id, "foreign", cluster.freeAddress());
            rejectedVote(cluster, target, null, null, source, cluster.id, target.getId().toString(), false);
            rejectedVote(cluster, target, cluster.pki, null, source, cluster.id, target.getId().toString(), false);
            rejectedVote(cluster, target, cluster.pki, rogue, source, cluster.id, target.getId().toString(), false);
            rejectedVote(cluster, target, cluster.pki, foreign, source, cluster.id, target.getId().toString(), false);
            rejectedVote(cluster, target, unrelated, identity, source, cluster.id, target.getId().toString(), false);
            // Registered but not actually configured nodes, as well as the sender, group or recipient of a legitimate certificate, are denied separately.
            rejectedVote(cluster, target, cluster.pki, cluster.outsider, source, cluster.id, target.getId().toString(), true);
            rejectedVote(cluster, target, cluster.pki, identity, (source + 1) % 3, cluster.id, target.getId().toString(), true);
            rejectedVote(cluster, target, cluster.pki, identity, source, UUID.randomUUID(), target.getId().toString(), true);
            rejectedVote(cluster, target, cluster.pki, identity, source, cluster.id, "wrong-receiver", true);
            Assertions.assertEquals(leader, cluster.leader());
            Assertions.assertEquals(2, write(serving, new byte[]{2}).commitSequence());
            cluster.awaitAll(2);
            // The CA, SAN hosts and fingerprints are correct, but the exit must be refused when the endpoint is checked.
            var swapped = new ArrayList<RatisTlsSecurity.PeerTrust>();
            for (int i = 0; i < 3; i++) {
                var entry = cluster.entries.get(i);
                swapped.add(new RatisTlsSecurity.PeerTrust(entry.id(), i == source ? entry.address() : cluster.entries.get(3 - source - i).address(), entry.trust().certificateSha256()));
            }
            var badTargets = RatisTlsSecurity.load(cluster.id, identity.id(), identity.identity(), RatisTlsTestCertificates.PASSWORD, cluster.pki.trust, RatisTlsTestCertificates.PASSWORD, swapped);
            try (var client = client(cluster.group, badTargets)) {
                assertThrows(java.io.IOException.class, () -> client.io().sendStaleRead(Message.valueOf(ByteString.copyFrom(RatisReplicaStatus.request())), 0, target.getId()));
            }
            cluster.stop(leader);
            int next = cluster.leader();
            assertPair(cluster.nodes.get(next), new byte[]{2});
            Assertions.assertEquals(WalCommitLookup.Status.COMMITTED, cluster.nodes.get(next).lookupCommit(receipt.transactionId()).status());
            var plain = RaftGroup.valueOf(cluster.group.getGroupId(),
                    cluster.peers.stream().map(peer -> RaftPeer.newBuilder(peer).setAddress("127.0.0.1:" + RatisEndpoint.parse(peer.getAddress()).port()).build()).toList());
            assertThrows(java.io.IOException.class, () -> RatisWalNode.openGenerationalLoopback(cluster.root(leader), cluster.peers.get(leader).getId(), plain, Set.of("a", "b"),
                    WalReplicaBackendFactory.localOnly(), options(), limits()));
            cluster.start(leader);
            cluster.awaitAll(2);
            Assertions.assertEquals(3, write(cluster.nodes.get(next), new byte[]{3}).commitSequence());
        }
    }

    /**
     * TLS covers real large mirror installation, internal health probe and management cutter. Original remote management does not need to be open.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void snapshotReplacementAndRestartRemainAuthenticated() throws Exception {
        try (var cluster = new Cluster(directory)) {
            int leader = cluster.leader(), removed = (leader + 1) % 3;
            var serving = cluster.nodes.get(leader);
            WalReceipt receipt = null;
            byte[] expected = null;
            for (int i = 1; i <= 10; i++) {
                expected = new byte[480 * 1024];
                Arrays.fill(expected, (byte) i);
                var result = write(serving, expected);
                if (receipt == null) {
                    receipt = result;
                }
            }
            cluster.awaitAll(10);
            cluster.stop(removed);
            long snapshot = serving.snapshot();
            serving.purgeForTest(snapshot);
            assertTrue(serving.firstLogIndexForTest() > 2);
            var plan = serving.planReplacement(cluster.peers.get(removed).getId(), cluster.freeAddress(), 10_000);
            var fresh = cluster.pki.issue(cluster.id, plan.replacement().getId().toString(), plan.replacement().getAddress());
            assertThrows(java.io.IOException.class, () -> serving.beginReplacement(plan, 10_000));
            for (var policy : cluster.security) {
                policy.authorizePeer(fresh.trust());
            }
            var freshSecurity = RatisTlsSecurity.load(cluster.id, fresh.id(), fresh.identity(), RatisTlsTestCertificates.PASSWORD, cluster.pki.trust, RatisTlsTestCertificates.PASSWORD,
                    cluster.security.get(leader).trustedPeers());
            try (
                    var joined = RatisWalNode.openAuthenticated(directory.resolve("replacement"), plan.replacement().getId(), plan.joiningGroup(), Set.of("a", "b"),
                            WalReplicaBackendFactory.localOnly(), options(), limits(), freshSecurity)) {
                Assertions.assertEquals(RatisWalNode.ReplacementPhase.LEARNER_ADDED, serving.beginReplacement(plan, 30_000).phase());
                long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
                while ((joined.appliedSequence() < 10 || joined.snapshotIndex() < snapshot || !joined.ready()) && System.nanoTime() < until) {
                    TimeUnit.MILLISECONDS.sleep(30);
                }
                assertTrue(joined.ready());
                assertTrue(joined.snapshotIndex() >= snapshot);
                Assertions.assertEquals(10, joined.appliedSequence());
                assertTrue(joined.durableSnapshotAckIndex() >= snapshot);
                Assertions.assertEquals(RatisWalNode.ReplacementPhase.COMPLETE, serving.completeReplacement(plan, 30_000).phase());
                serving.transferLeadership(plan.replacement().getId(), 30_000);
                cluster.awaitLeader(joined);
                assertPair(joined, expected);
                Assertions.assertEquals(WalCommitLookup.Status.COMMITTED, joined.lookupCommit(receipt.transactionId()).status());
                Assertions.assertEquals(11, write(joined, new byte[]{9}).commitSequence());
                joined.snapshot();
                rejectedVote(cluster, plan.replacement(), cluster.pki, cluster.entries.get(removed), removed, cluster.id, fresh.id(), true);
            }
            try (
                    var restarted = RatisWalNode.openAuthenticated(directory.resolve("replacement"), plan.replacement().getId(), plan.joiningGroup(), Set.of("a", "b"),
                            WalReplicaBackendFactory.localOnly(), options(), limits(), freshSecurity)) {
                long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
                while (restarted.appliedSequence() < 11 && System.nanoTime() < until) {
                    TimeUnit.MILLISECONDS.sleep(30);
                }
                int current = cluster.leader();
                cluster.nodes.get(current).transferLeadership(plan.replacement().getId(), 30_000);
                cluster.awaitLeader(restarted);
                assertPair(restarted, new byte[]{9});
                Assertions.assertEquals(WalCommitLookup.Status.COMMITTED, restarted.lookupCommit(receipt.transactionId()).status());
                Assertions.assertEquals(12, write(restarted, new byte[]{10}).commitSequence());
            }
        }
    }

    /**
     * The two certificates overlap and reset, and the old private key cannot be reconnected to the current member after the old fingerprints have been removed.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void certificateRotationOverlapsPinsThenRejectsOldKey() throws Exception {
        try (var cluster = new Cluster(directory)) {
            int leader = cluster.leader(), rotating = (leader + 1) % 3;
            var receipt = write(cluster.nodes.get(leader), new byte[]{1});
            cluster.awaitAll(1);
            var previous = cluster.entries.get(rotating);
            Path old = directory.resolve("old-identity");
            Files.createDirectories(old);
            Path cert = Files.copy(previous.certificateFile(), old.resolve("node.pem")), key = Files.copy(previous.keyFile(), old.resolve("node.key"));
            var oldCredential = new RatisTlsTestCertificates.Entry(previous.id(), previous.address(), previous.identity(), cert, key, previous.certificate());
            var fresh = cluster.pki.issue(cluster.id, previous.id(), previous.address());
            Assertions.assertNotEquals(previous.trust().certificateSha256(), fresh.trust().certificateSha256());
            var pins = new HashSet<>(previous.trust().certificateSha256());
            pins.addAll(fresh.trust().certificateSha256());
            var overlap = new RatisTlsSecurity.PeerTrust(fresh.id(), fresh.address(), pins);
            for (var policy : cluster.security) {
                policy.authorizePeer(overlap);
            }
            cluster.stop(rotating);
            Assertions.assertEquals(2, write(cluster.nodes.get(leader), new byte[]{2}).commitSequence());
            cluster.security.set(rotating, RatisTlsSecurity.load(cluster.id, fresh.id(), fresh.identity(), RatisTlsTestCertificates.PASSWORD, cluster.pki.trust, RatisTlsTestCertificates.PASSWORD,
                    cluster.security.get(leader).trustedPeers()));
            cluster.start(rotating);
            cluster.awaitAll(2);
            for (var policy : cluster.security) {
                policy.authorizePeer(fresh.trust());
            }
            rejectedVote(cluster, cluster.peers.get(leader), cluster.pki, oldCredential, rotating, cluster.id, cluster.peers.get(leader).getId().toString(), false);
            Assertions.assertEquals(3, write(cluster.nodes.get(leader), new byte[]{3}).commitSequence());
            cluster.awaitAll(3);
            cluster.nodes.get(leader).transferLeadership(cluster.peers.get(rotating).getId(), 30_000);
            cluster.awaitLeader(cluster.nodes.get(rotating));
            assertPair(cluster.nodes.get(rotating), new byte[]{3});
            Assertions.assertEquals(WalCommitLookup.Status.COMMITTED, cluster.nodes.get(rotating).lookupCommit(receipt.transactionId()).status());
        }
    }

    /**
     * When a high-term request is rejected, it cannot affect an active cluster and the RPC error cannot be judged only by client timeout for identity.
     *
     * @param cluster cluster
     * @param target target
     * @param pki pki
     * @param certificate certificate
     * @param declaredSource declared source
     * @param group group
     * @param receiver receiver
     * @param permission permission
     * @throws Exception if the operation cannot complete
     */
    private static void rejectedVote(final Cluster cluster, final RaftPeer target, final RatisTlsTestCertificates pki, final RatisTlsTestCertificates.Entry certificate, final int declaredSource,
                                     final UUID group, final String receiver,
                                     final boolean permission) throws Exception {
        var endpoint = RatisEndpoint.parse(target.getAddress());
        var builder = NettyChannelBuilder.forAddress(endpoint.host(), endpoint.port());
        if (pki == null) {
            builder.usePlaintext();
        } else {
            var ssl = GrpcSslContexts.configure(org.apache.ratis.thirdparty.io.netty.handler.ssl.SslContextBuilder.forClient(), org.apache.ratis.thirdparty.io.netty.handler.ssl.SslProvider.JDK)
                    .trustManager(pki.ca.toFile());
            if (certificate != null) {
                ssl.keyManager(certificate.certificateFile().toFile(), certificate.keyFile().toFile());
            }
            builder.sslContext(ssl.build());
        }
        var channel = builder.build();
        try {
            var rpc = RaftProtos.RaftRpcRequestProto.newBuilder().setRequestorId(ByteString.copyFromUtf8("n" + declaredSource)).setReplyId(ByteString.copyFromUtf8(receiver))
                    .setRaftGroupId(org.apache.ratis.util.ProtoUtils.toRaftGroupIdProtoBuilder(RaftGroupId.valueOf(group)));
            var request = RaftProtos.RequestVoteRequestProto.newBuilder().setServerRequest(rpc).setCandidateTerm(1000)
                    .setCandidateLastEntry(RaftProtos.TermIndexProto.newBuilder().setTerm(0).setIndex(0)).build();
            var failure = assertThrows(StatusRuntimeException.class, () -> RaftServerProtocolServiceGrpc.newBlockingStub(channel).withDeadlineAfter(3, TimeUnit.SECONDS).requestVote(request));
            if (permission) {
                Assertions.assertEquals(Status.Code.PERMISSION_DENIED, failure.getStatus().getCode());
            } else {
                assertTrue(Set.of(Status.Code.UNAVAILABLE, Status.Code.UNAUTHENTICATED).contains(failure.getStatus().getCode()), failure.getStatus().toString());
            }
        } finally {
            channel.shutdownNow();
            assertTrue(channel.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    /**
     * Authenticate that the client is used only for internal status and rejecting tests, and closes automatically retry to preserve the true rejection result.
     *
     * @param group group
     * @param security security
     * @return the operation result
     */
    private static RaftClient client(final RaftGroup group, final RatisTlsSecurity security) {
        var properties = new RaftProperties();
        org.apache.ratis.client.RaftClientConfigKeys.Rpc.setRequestTimeout(properties, org.apache.ratis.util.TimeDuration.valueOf(3, TimeUnit.SECONDS));
        return RaftClient.newBuilder().setRaftGroup(group).setProperties(properties).setParameters(security.parameters()).setRetryPolicy(org.apache.ratis.retry.RetryPolicies.noRetry()).build();
    }

    /**
     * Each transaction writing of two pieces of the same payload to check complete snapshot and permanent receipt.
     *
     * @param node node
     * @param value value to write
     * @return the operation result
     */
    private static WalReceipt write(final RatisWalNode node, final byte[] value) {
        var tx = node.begin(IsolationLevel.REPEATABLE_READ);
        node.put(tx, "a", "k", value);
        node.put(tx, "b", "k", value);
        return node.commit(tx);
    }

    /**
     * A single RR statement checks two complete bytes.
     *
     * @param node node
     * @param expected expected
     */
    private static void assertPair(final RatisWalNode node, final byte[] expected) {
        var tx = node.begin(IsolationLevel.REPEATABLE_READ);
        var view = node.beginReadStatement(tx);
        for (String shard : List.of("a", "b")) {
            assertArrayEquals(expected, node.get(tx, shard, "k", view).orElseThrow());
        }
        node.rollback(tx);
    }

    /**
     * Operational memory and waiting is limited and throughput measurements are not performed.
     *
     * @return the operation result
     */
    private static WalOptions options() {
        return WalOptions.defaults().withTimeout(10_000).withHotCacheBytes(0);
    }

    /**
     * Snapshot Totals are clearly limited.
     *
     * @return the operation result
     */
    private static WalBackupOptions limits() {
        return new WalBackupOptions(32L * 1024 * 1024, 8, 1024 * 1024, 60_000);
    }

    /**
     * Three real Ratis services use their respective TLS keys and storage directories.
     */
    private static final class Cluster implements AutoCloseable {

        final Path directory;

        final UUID id = UUID.randomUUID();

        final List<RaftPeer> peers = new ArrayList<>();

        final List<RatisWalNode> nodes = new ArrayList<>();

        final List<RatisTlsSecurity> security = new ArrayList<>();

        final List<RatisTlsTestCertificates.Entry> entries = new ArrayList<>();

        final RatisTlsTestCertificates pki;

        final RatisTlsTestCertificates.Entry outsider;

        final RaftGroup group;

        /**
         * DNS endpoint verify is formally assembled instead of relying on an express return ring dedicated entrance.
         *
         * @param directory directory
         * @throws Exception if the operation cannot complete
         */
        Cluster(final Path directory) throws Exception {
            this.directory = directory;
            pki = new RatisTlsTestCertificates(directory.resolve("pki"));
            for (int i = 0; i < 3; i++) {
                String address = freeAddress();
                peers.add(RaftPeer.newBuilder().setId("n" + i).setAddress(address).build());
                entries.add(pki.issue(id, "n" + i, address));
                nodes.add(null);
            }
            outsider = pki.issue(id, "outsider", freeAddress());
            var trust = new ArrayList<RatisTlsSecurity.PeerTrust>();
            for (var entry : entries) {
                trust.add(entry.trust());
            }
            trust.add(outsider.trust());
            group = RaftGroup.valueOf(RaftGroupId.valueOf(id), peers);
            try {
                for (int i = 0; i < 3; i++) {
                    var entry = entries.get(i);
                    security.add(RatisTlsSecurity.load(id, entry.id(), entry.identity(), RatisTlsTestCertificates.PASSWORD, pki.trust, RatisTlsTestCertificates.PASSWORD, trust));
                    start(i);
                }
            } catch (final Exception failure) {
                try {
                    close();
                } catch (final Exception close) {
                    failure.addSuppressed(close);
                }
                throw failure;
            }
        }

        /**
         * Each node catalogue is independent, and AUTHENTICATED tags are also independently saved.
         *
         * @param node node
         * @return the operation result
         */
        Path root(final int node) {
            return directory.resolve("n" + node);
        }

        /**
         * Reopen to continue the stable identity and its own TLS receipt.
         *
         * @param node node
         * @throws Exception if the operation cannot complete
         */
        void start(final int node) throws Exception {
            nodes.set(node,
                    RatisWalNode.openAuthenticated(root(node), peers.get(node).getId(), group, Set.of("a", "b"), WalReplicaBackendFactory.localOnly(), options(), limits(), security.get(node)));
        }

        /**
         * Only closes the node held in this test.
         *
         * @param node node
         * @throws Exception if the operation cannot complete
         */
        void stop(final int node) throws Exception {
            var old = nodes.set(node, null);
            if (old != null) {
                old.close();
            }
        }

        /**
         * A list order cannot be used as a leader to observe the real state of elections.
         *
         * @return the operation result
         * @throws AssertionError if the operation cannot complete
         * @throws Exception if the operation cannot complete
         */
        int leader() throws Exception {
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (System.nanoTime() < until) {
                for (int i = 0; i < nodes.size(); i++) {
                    if (nodes.get(i) != null && nodes.get(i).leaderEpoch() > 0) {
                        return i;
                    }
                }
                TimeUnit.MILLISECONDS.sleep(30);
            }
            throw new AssertionError("no authenticated leader");
        }

        /**
         * Manage the master and wait for the assigned node to be really ready.
         *
         * @param node node
         * @throws Exception if the operation cannot complete
         */
        void awaitLeader(final RatisWalNode node) throws Exception {
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (node.leaderEpoch() < 1 && System.nanoTime() < until) {
                TimeUnit.MILLISECONDS.sleep(30);
            }
            assertTrue(node.leaderEpoch() > 0);
        }

        /**
         * Actual transaction confirmation for all online nodes before the failure is executed.
         *
         * @param sequence sequence
         * @throws Exception if the operation cannot complete
         */
        void awaitAll(final long sequence) throws Exception {
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (nodes.stream().filter(Objects::nonNull).anyMatch(node -> node.appliedSequence() < sequence) && System.nanoTime() < until) {
                TimeUnit.MILLISECONDS.sleep(30);
            }
            for (var node : nodes) {
                if (node != null) {
                    assertTrue(node.appliedSequence() >= sequence);
                }
            }
        }

        /**
         * Only open temporary ports of developers are assigned, and no available services are used.
         *
         * @return the operation result
         * @throws Exception if the operation cannot complete
         */
        String freeAddress() throws Exception {
            try (var socket = new java.net.ServerSocket(0, 1, java.net.InetAddress.getLoopbackAddress())) {
                return "localhost:" + socket.getLocalPort();
            }
        }

        /**
         * The anomaly also completely shuts down all its own nodes, without any interruption of JVM.
         *
         * @throws Exception if the operation cannot complete
         */
        @Override
        public void close() throws Exception {
            Exception failure = null;
            for (int i = 0; i < nodes.size(); i++) {
                try {
                    stop(i);
                } catch (final Exception close) {
                    if (failure == null) {
                        failure = close;
                    } else {
                        failure.addSuppressed(close);
                    }
                }
            }
            if (failure != null) {
                throw failure;
            }
        }
    }
}
