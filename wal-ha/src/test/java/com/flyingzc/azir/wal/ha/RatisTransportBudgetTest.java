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

import org.apache.ratis.thirdparty.io.grpc.ManagedChannel;
import org.apache.ratis.thirdparty.io.grpc.Server;
import org.apache.ratis.thirdparty.io.grpc.Status;
import org.apache.ratis.thirdparty.io.grpc.StatusRuntimeException;
import org.apache.ratis.thirdparty.io.grpc.netty.GrpcSslContexts;
import org.apache.ratis.thirdparty.io.grpc.netty.NettyChannelBuilder;
import org.apache.ratis.thirdparty.io.grpc.netty.NettyServerBuilder;
import org.apache.ratis.thirdparty.io.netty.handler.ssl.SslContextBuilder;
import org.apache.ratis.thirdparty.io.netty.handler.ssl.SslProvider;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Assertions;

import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.proto.grpc.RaftServerProtocolServiceGrpc;

import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;

/**
 * Real hype and HTTP2 call verify resource budget, this dedicated service does not pretend to be a real Raft consensus.
 */
class RatisTransportBudgetTest {

    @TempDir
    Path directory;

    private final List<org.apache.ratis.thirdparty.io.netty.channel.nio.NioEventLoopGroup> groups = new ArrayList<>();

    /**
     * The cycle of events is owned by the receiving-and-observed, and the services are empty after closing.
     */
    @org.junit.jupiter.api.AfterEach
    void closeGroups() {
        for (var group : groups) {
            group.shutdownGracefully(0, 5, TimeUnit.SECONDS).syncUninterruptibly();
        }
    }

    /**
     * Connection, which does not send any TLS data, is also subject to a total limit and can be used again after handshake timeout.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void unauthenticatedTcpConnectionsAreCappedBeforeTls() throws Exception {
        var budget = new RatisTransportBudget(new RatisTransportLimits(2, 4, 8, 8192, 16, 1000, 5000, 60_000));
        var pki = new RatisTlsTestCertificates(directory);
        var certificate = pki.issue(UUID.randomUUID(), "n0", "localhost:19001");
        var server = server(budget, certificate, new Service()).start();
        try (var first = new java.net.Socket("127.0.0.1", server.getPort()); var second = new java.net.Socket("127.0.0.1", server.getPort())) {
            await(() -> budget.stats().connections() == 2);
            try (var rejected = new java.net.Socket("127.0.0.1", server.getPort())) {
                await(() -> budget.stats().rejectedConnections() >= 1);
                rejected.setSoTimeout(2000);
                try {
                    Assertions.assertEquals(-1, rejected.getInputStream().read());
                } catch (final java.net.SocketException reset) {
                    /*
                     * The kernel rejection can be shown as EOF or reset.
                     */ }
            }
            await(() -> budget.stats().connections() == 0);
            Assertions.assertEquals(2, budget.stats().peakConnections());
            var channel = channel(pki, server.getPort());
            try {
                assertTrue(RaftServerProtocolServiceGrpc.newBlockingStub(channel).withDeadlineAfter(5, TimeUnit.SECONDS).requestVote(RaftProtos.RequestVoteRequestProto.getDefaultInstance())
                        .getServerReply().getSuccess());
            } finally {
                channel.shutdownNow();
                assertTrue(channel.awaitTermination(5, TimeUnit.SECONDS));
            }
        } finally {
            server.shutdownNow();
            assertTrue(server.awaitTermination(5, TimeUnit.SECONDS));
        }
        await(() -> budget.stats().connections() == 0);
    }

    /**
     * Different TLS connection s also share calls budget, which must be returned for normal termination and cancellation.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void rpcCapIsGlobalAndCancellationReleasesExactlyOnce() throws Exception {
        var budget = new RatisTransportBudget(new RatisTransportLimits(8, 2, 8, 8192, 16, 3000, 5000, 60_000));
        var service = new Service();
        var pki = new RatisTlsTestCertificates(directory);
        var certificate = pki.issue(UUID.randomUUID(), "n0", "localhost:19001");
        var server = server(budget, certificate, service).start();
        var a = channel(pki, server.getPort());
        var b = channel(pki, server.getPort());
        try {
            var first = RaftServerProtocolServiceGrpc.newStub(a).appendEntries(new Sink());
            first.onNext(RaftProtos.AppendEntriesRequestProto.getDefaultInstance());
            var second = RaftServerProtocolServiceGrpc.newStub(b).appendEntries(new Sink());
            second.onNext(RaftProtos.AppendEntriesRequestProto.getDefaultInstance());
            await(() -> budget.stats().calls() == 2);
            var blocked = assertThrows(StatusRuntimeException.class,
                    () -> RaftServerProtocolServiceGrpc.newBlockingStub(b).withDeadlineAfter(5, TimeUnit.SECONDS).requestVote(RaftProtos.RequestVoteRequestProto.getDefaultInstance()));
            Assertions.assertEquals(Status.Code.RESOURCE_EXHAUSTED, blocked.getStatus().getCode());
            first.onCompleted();
            await(() -> budget.stats().calls() == 1);
            assertTrue(RaftServerProtocolServiceGrpc.newBlockingStub(a).withDeadlineAfter(5, TimeUnit.SECONDS).requestVote(RaftProtos.RequestVoteRequestProto.getDefaultInstance()).getServerReply()
                    .getSuccess());
            second.onError(Status.CANCELLED.asRuntimeException());
            await(() -> budget.stats().calls() == 0);
            Assertions.assertEquals(2, budget.stats().peakCalls());
            Assertions.assertEquals(1, budget.stats().rejectedCalls());
        } finally {
            a.shutdownNow();
            b.shutdownNow();
            assertTrue(a.awaitTermination(5, TimeUnit.SECONDS));
            assertTrue(b.awaitTermination(5, TimeUnit.SECONDS));
            server.shutdownNow();
            assertTrue(server.awaitTermination(5, TimeUnit.SECONDS));
        }
        await(() -> budget.stats().connections() == 0);
        Assertions.assertEquals(0, budget.stats().calls());
    }

    /**
     * Error budget cannot become an unlimited connection configuration after startup.
     */
    @Test
    void invalidLimitsAreRejected() {
        assertThrows(IllegalArgumentException.class, () -> new RatisTransportLimits(0, 1, 1, 8192, 16, 1000, 1000, 1000));
        assertThrows(IllegalArgumentException.class, () -> new RatisTransportLimits(1, 1, 1, 8192, 16, 0, 1000, 1000));
    }

    /**
     * The actual TLS service uses JDK ALPN, the same budget component as the official node.
     *
     * @param budget budget
     * @param certificate certificate
     * @param service service
     * @return the operation result
     * @throws Exception if the operation cannot complete
     */
    private Server server(final RatisTransportBudget budget, final RatisTlsTestCertificates.Entry certificate, final Service service) throws Exception {
        var boss = new org.apache.ratis.thirdparty.io.netty.channel.nio.NioEventLoopGroup(1);
        var worker = new org.apache.ratis.thirdparty.io.netty.channel.nio.NioEventLoopGroup(1);
        groups.add(boss);
        groups.add(worker);
        var ssl = GrpcSslContexts.configure(SslContextBuilder.forServer(certificate.certificateFile().toFile(), certificate.keyFile().toFile()), SslProvider.JDK).build();
        return budget.configure(NettyServerBuilder.forAddress(new java.net.InetSocketAddress("127.0.0.1", 0)).bossEventLoopGroup(boss).workerEventLoopGroup(worker)).sslContext(ssl).addService(service)
                .build();
    }

    /**
     * The client only trusts this test CA without skipping the server hostname verify.
     *
     * @param pki pki
     * @param port port
     * @return the operation result
     * @throws Exception if the operation cannot complete
     */
    private static ManagedChannel channel(final RatisTlsTestCertificates pki, final int port) throws Exception {
        var ssl = GrpcSslContexts.configure(SslContextBuilder.forClient(), SslProvider.JDK).trustManager(pki.ca.toFile()).build();
        return NettyChannelBuilder.forAddress("localhost", port).sslContext(ssl).build();
    }

    /**
     * Waiting for release with actual observation conditions and cannot be checked in place of fixed sleep.
     *
     * @param condition condition
     * @throws Exception if the operation cannot complete
     */
    private static void await(final java.util.function.BooleanSupplier condition) throws Exception {
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!condition.getAsBoolean() && System.nanoTime() < until) {
            TimeUnit.MILLISECONDS.sleep(10);
        }
        assertTrue(condition.getAsBoolean());
    }

    /**
     * Only bounded responses and hold streams are provided, verify budget not transaction.
     */
    private static final class Service extends RaftServerProtocolServiceGrpc.RaftServerProtocolServiceImplBase {

        /**
         * Regular responses prove that budget was not permanently denied all traffic after release.
         *
         * @param request request
         * @param observer observer
         */
        @Override
        public void requestVote(final RaftProtos.RequestVoteRequestProto request, final StreamObserver<RaftProtos.RequestVoteReplyProto> observer) {
            observer.onNext(RaftProtos.RequestVoteReplyProto.newBuilder().setServerReply(RaftProtos.RaftRpcReplyProto.newBuilder().setSuccess(true)).build());
            observer.onCompleted();
        }

        /**
         * Keep the actual call occupancy until the client has taken the initiative to close.
         *
         * @param observer observer
         * @return the operation result
         */
        @Override
        public StreamObserver<RaftProtos.AppendEntriesRequestProto> appendEntries(final StreamObserver<RaftProtos.AppendEntriesReplyProto> observer) {
            return new StreamObserver<>() {

                /**
                 * To receive any limited test information and not implement consensus.
                 *
                 * @param value value to write
                 */
                @Override
                public void onNext(final RaftProtos.AppendEntriesRequestProto value) {
                }

                /**
                 * Cancels a gRPC call budget release.
                 *
                 * @param failure failure
                 */
                @Override
                public void onError(final Throwable failure) {
                }

                /**
                 * Normal termination trigger service closure.
                 */
                @Override
                public void onCompleted() {
                    observer.onCompleted();
                }
            };
        }
    }

    /**
     * Client-flow reply does not need to hold unbounded result queue.
     */
    private static final class Sink implements StreamObserver<RaftProtos.AppendEntriesReplyProto> {

        /**
         * No response is retained.
         *
         * @param value value to write
         */
        @Override
        public void onNext(final RaftProtos.AppendEntriesReplyProto value) {
        }

        /**
         * The cancellation is not expected to terminate the test main route.
         *
         * @param failure failure
         */
        @Override
        public void onError(final Throwable failure) {
        }

        /**
         * End by budget statistically independent verify.
         */
        @Override
        public void onCompleted() {
        }
    }
}
