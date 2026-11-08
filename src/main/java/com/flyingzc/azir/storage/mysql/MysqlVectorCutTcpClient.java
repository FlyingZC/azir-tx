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

package com.flyingzc.azir.storage.mysql;

import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.MvccException;

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

/**
 * Return ring TCP cut-off client, found by centre lease algebra and verify response per binary.
 */
final class MysqlVectorCutTcpClient implements AutoCloseable {

    private final MysqlVectorCutLeaderLease leases;

    private final List<String> routes;

    private final int stripes;

    private final ConcurrentLinkedQueue<Channel> idle = new ConcurrentLinkedQueue<>();

    private final AtomicBoolean closed = new AtomicBoolean();

    private volatile Endpoint cached;

    /**
     * Fixed-read side routing, which is fully consistent with the centre durable lease and the responder.
     *
     * @param center center
     * @param shards shards
     * @param stripes stripes
     */
    MysqlVectorCutTcpClient(final DataSource center, final Map<String, DataSource> shards, final int stripes) {
        this(center, shards, stripes, false);
    }

    /**
     * The official lease table found on the reading side is not acceptable to the leadership of the capacity experiment.
     *
     * @param center center
     * @param shards shards
     * @param stripes stripes
     * @return the operation result
     */
    static MysqlVectorCutTcpClient forProduct(final DataSource center,
                                              final Map<String, DataSource> shards, final int stripes) {
        return new MysqlVectorCutTcpClient(center, shards, stripes, true);
    }

    /**
     * Two sets of centres lease share algebra validate and bounded connection pool.
     *
     * @param center center
     * @param shards shards
     * @param stripes stripes
     * @param product product
     */
    private MysqlVectorCutTcpClient(final DataSource center, final Map<String, DataSource> shards,
                                    final int stripes, final boolean product) {
        Objects.requireNonNull(shards, "shards");
        leases = product ? MysqlVectorCutLeaderLease.forProduct(center, shards, stripes)
                : new MysqlVectorCutLeaderLease(center, shards, stripes);
        routes = shards.keySet().stream().sorted().toList();
        this.stripes = stripes;
    }

    /**
     * One request to read the full cut-off; old endpoint rejected or connection failed to find again.
     *
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     */
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
            } catch (final IOException | IllegalArgumentException ex) {
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

    /**
     * Prefer the current algebra free connection to avoid creating TCP short connection for each RC.
     *
     * @param endpoint endpoint
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    private Channel borrow(final Endpoint endpoint) throws IOException {
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
        } catch (final IOException | RuntimeException ex) {
            socket.close();
            throw ex;
        }
    }

    /**
     * Only the complete frame connection that still belongs to the current lease will be released back to the bounded empty pool.
     *
     * @param channel channel
     * @param endpoint endpoint
     */
    private void recycle(final Channel channel, final Endpoint endpoint) {
        Endpoint latest = cached;
        if (closed.get() || !endpoint.valid() || latest == null
                || latest.generation() != endpoint.generation()
                || !latest.ownerId().equals(endpoint.ownerId()) || idle.size() >= 64) {
            channel.close();
        } else {
            idle.offer(channel);
        }
    }

    /**
     * Reads lease only when the cache has expired and calculates the conservative local deadline for the start time of the request.
     *
     * @return the operation result
     * @throws IllegalStateException if the operation cannot complete
     * @throws MvccException if the operation cannot complete
     */
    private synchronized Endpoint resolve() {
        if (closed.get()) {
            throw new IllegalStateException("TCP vector cut client is closed");
        }
        Endpoint current = cached;
        if (current != null && current.valid()) {
            return current;
        }
        long started = System.nanoTime();
        var leader = leases.discover().orElseThrow(() -> new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
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

    /**
     * The experimental endpoint allows only a valid digital returnring IPv4 address and port.
     *
     * @param ownerId owner id
     * @throws IllegalArgumentException if the operation cannot complete
     */
    private static void requireLoopbackEndpoint(final String ownerId) {
        if (!ownerId.startsWith("127.0.0.1:")) {
            throw new IllegalArgumentException("TCP vector cut owner is not a loopback endpoint");
        }
        try {
            int port = Integer.parseInt(ownerId.substring("127.0.0.1:".length()));
            if (port < 1 || port > 65535) {
                throw new IllegalArgumentException("TCP vector cut owner port is invalid");
            }
        } catch (final NumberFormatException ex) {
            throw new IllegalArgumentException("TCP vector cut owner port is invalid", ex);
        }
    }

    /**
     * Clears the old end of this failure and does not overwrite new addresses that have been refreshed by other read threads.
     *
     * @param endpoint endpoint
     */
    private synchronized void invalidate(final Endpoint endpoint) {
        if (cached == endpoint) {
            cached = null;
            Channel channel;
            while ((channel = idle.poll()) != null) {
                channel.close();
            }
        }
    }

    /**
     * Closes all free TCP connection and will destroy it when loaning connection back.
     */
    @Override
    public void close() {
        closed.set(true);
        Channel channel;
        while ((channel = idle.poll()) != null) {
            channel.close();
        }
    }

    /**
     * Endpoints are conservatively disabled before the centre deadline and old leaders cannot be reused indefinitely.
     *
     * @param ownerId owner id
     * @param generation generation
     * @param validUntilNanos valid until nanos
     */
    private record Endpoint(String ownerId, long generation, long validUntilNanos) {

        /**
         * Only local monotonic clock addresses that remain under a conservative lease are accepted.
         *
         * @return the operation result
         */
        boolean valid() {
            return validUntilNanos - System.nanoTime() > 0;
        }
    }

    /**
     * A duplicate connection is loaned to only one reading request at the same time and the old algebra connection is destroyed directly.
     *
     * @param endpoint endpoint
     * @param socket socket
     * @param input input
     * @param output output
     */
    private record Channel(Endpoint endpoint, Socket socket,
            DataInputStream input, DataOutputStream output) implements AutoCloseable {

        /**
         * Closes the bottom connection, without leaving the damaged frame for next reading.
         */
        @Override
        public void close() {
            try {
                socket.close();
            } catch (final IOException ignored) {
                // The connection destruction failure affects only the current network resource collection and does not allow old algebras to be reused.
            }
        }
    }
}
