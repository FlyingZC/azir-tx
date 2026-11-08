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

import org.apache.ratis.thirdparty.io.grpc.ForwardingServerCall;
import org.apache.ratis.thirdparty.io.grpc.ForwardingServerCallListener;
import org.apache.ratis.thirdparty.io.grpc.Metadata;
import org.apache.ratis.thirdparty.io.grpc.ServerCall;
import org.apache.ratis.thirdparty.io.grpc.ServerCallHandler;
import org.apache.ratis.thirdparty.io.grpc.ServerInterceptor;
import org.apache.ratis.thirdparty.io.grpc.Status;
import org.apache.ratis.thirdparty.io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.ratis.thirdparty.io.netty.channel.socket.nio.NioSocketChannel;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.ratis.thirdparty.io.grpc.netty.NettyServerBuilder;
import org.apache.ratis.thirdparty.io.netty.channel.Channel;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelOption;

import java.util.List;

/**
 * Limit the total number of RPCs that have accepted TCP connection before TLS shakes hands, and limit all services at node.
 */
final class RatisTransportBudget {

    private final RatisTransportLimits limits;

    private final Semaphore connections, calls;

    private final AtomicInteger connectionCount = new AtomicInteger(), callCount = new AtomicInteger(), peakConnections = new AtomicInteger(), peakCalls = new AtomicInteger();

    private final AtomicLong rejectedConnections = new AtomicLong(), rejectedCalls = new AtomicLong();

    /**
     * The entire service of a node is the same budget and cannot be scaled up by connection.
     *
     * @param limits limits
     */
    RatisTransportBudget(final RatisTransportLimits limits) {
        this.limits = java.util.Objects.requireNonNull(limits);
        connections = new Semaphore(limits.connections());
        calls = new Semaphore(limits.calls());
    }

    /**
     * The NIO receiver is limited to connection, and the original official TLS, HTTP2 and message decoding codes continue to be maintained.
     *
     * @param builder builder
     * @return the operation result
     */
    NettyServerBuilder configure(final NettyServerBuilder builder) {
        return builder.channelFactory(() -> new Listener(this)).withOption(ChannelOption.SO_BACKLOG, limits.listenBacklog())
                .maxConcurrentCallsPerConnection(limits.streamsPerConnection()).maxInboundMetadataSize(limits.metadataBytes())
                .handshakeTimeout(limits.handshakeMillis(), TimeUnit.MILLISECONDS).maxConnectionIdle(limits.idleMillis(), TimeUnit.MILLISECONDS)
                .maxConnectionAge(limits.connectionAgeMillis(), TimeUnit.MILLISECONDS).maxConnectionAgeGrace(30, TimeUnit.SECONDS).intercept(new Calls());
    }

    /**
     * Read-only takes the memory count without waiting for a network or business status lock.
     *
     * @return the operation result
     */
    RatisWalNode.TransportStats stats() {
        return new RatisWalNode.TransportStats(connectionCount.get(), peakConnections.get(), rejectedConnections.get(), callCount.get(), peakCalls.get(), rejectedCalls.get());
    }

    /**
     * Excess connection rejected before creating TLS conduit.
     *
     * @return the operation result
     */
    private boolean acquireConnection() {
        if (!connections.tryAcquire()) {
            rejectedConnections.incrementAndGet();
            return false;
        }
        peakConnections.accumulateAndGet(connectionCount.incrementAndGet(), Math::max);
        return true;
    }

    /**
     * The number of places will not be released until the real patch is closed.
     */
    private void releaseConnection() {
        connectionCount.decrementAndGet();
        connections.release();
    }

    /**
     * The RPC share of the entire node does not allow multiple connection s to bypass the upper limit.
     *
     * @return the operation result
     */
    private boolean acquireCall() {
        if (!calls.tryAcquire()) {
            rejectedCalls.incrementAndGet();
            return false;
        }
        peakCalls.accumulateAndGet(callCount.incrementAndGet(), Math::max);
        return true;
    }

    /**
     * The access monitor is limited at the accept border without relying on a successful TLS handshake.
     */
    private static final class Listener extends NioServerSocketChannel {

        private final RatisTransportBudget owner;

        /**
         * The same channel is used for the official NIO event.
         *
         * @param owner owner
         */
        Listener(final RatisTransportBudget owner) {
            this.owner = owner;
        }

        /**
         * Do not register to sub-incident cycle or allocate SSL engines without immediate closure of the original package through budget.
         *
         * @param messages messages
         * @return the operation result
         * @throws Exception if the operation cannot complete
         */
        @Override
        protected int doReadMessages(final List<Object> messages) throws Exception {
            var socket = javaChannel().accept();
            if (socket == null) {
                return 0;
            }
            if (!owner.acquireConnection()) {
                socket.close();
                return 0;
            }
            try {
                messages.add(new Connection(this, socket, owner));
                return 1;
            } catch (final Throwable failure) {
                owner.releaseConnection();
                socket.close();
                throw failure;
            }
        }
    }

    /**
     * Each connection release has been accepted once, and the failure of registration is followed by doClose.
     */
    private static final class Connection extends NioSocketChannel {

        private final RatisTransportBudget owner;

        private final AtomicBoolean released = new AtomicBoolean();

        /**
         * Only the kernels that have acquired the space will be sealed.
         *
         * @param parent parent
         * @param socket socket
         * @param owner owner
         */
        Connection(final Channel parent, final java.nio.channels.SocketChannel socket, final RatisTransportBudget owner) {
            super(parent, socket);
            this.owner = owner;
        }

        /**
         * Normal closures, handshake failures and pre-closure cancellations cannot be repeated.
         *
         * @throws Exception if the operation cannot complete
         */
        @Override
        protected void doClose() throws Exception {
            try {
                super.doClose();
            } finally {
                if (released.compareAndSet(false, true)) {
                    owner.releaseConnection();
                }
            }
        }
    }

    /**
     * The call covers the authentication interceptors and actual services, and both refusals and cancellations can be safely released.
     */
    private final class Calls implements ServerInterceptor {

        /**
         * First, limit the total number of calls and then allow the decomposition or execution of this node service.
         *
         * @param <Q> value type
         * @param <S> value type
         * @param call call
         * @param headers headers
         * @param next next
         * @return the operation result
         */
        @Override
        public <Q, S> ServerCall.Listener<Q> interceptCall(final ServerCall<Q, S> call, final Metadata headers, final ServerCallHandler<Q, S> next) {
            if (!acquireCall()) {
                call.close(Status.RESOURCE_EXHAUSTED.withDescription("WAL RPC budget exhausted"), new Metadata());
                return new ServerCall.Listener<>() {
                };
            }
            var released = new AtomicBoolean();
            Runnable release = () -> {
                if (released.compareAndSet(false, true)) {
                    callCount.decrementAndGet();
                    calls.release();
                }
            };
            var guarded = new ForwardingServerCall.SimpleForwardingServerCall<Q, S>(call) {

                /**
                 * The active refusal of services must also be returned to the place, without relying on client cooperation for closure.
                 *
                 * @param status status
                 * @param trailers trailers
                 */
                @Override
                public void close(final Status status, final Metadata trailers) {
                    try {
                        super.close(status, trailers);
                    } finally {
                        release.run();
                    }
                }
            };
            try {
                return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(next.startCall(guarded, headers)) {

                    /**
                     * Client cancels or terminates the connection return.
                     */
                    @Override
                    public void onCancel() {
                        try {
                            super.onCancel();
                        } finally {
                            release.run();
                        }
                    }

                    /**
                     * Regular call terminates the same idempotent return.
                     */
                    @Override
                    public void onComplete() {
                        try {
                            super.onComplete();
                        } finally {
                            release.run();
                        }
                    }
                };
            } catch (final Throwable failure) {
                release.run();
                throw failure;
            }
        }
    }
}
