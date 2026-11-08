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

/**
 * Read-only remote cut-off client, cache current endpoint and validate centre algebra and response route.
 */
final class MysqlVectorCutRemoteClient {

    private final MysqlVectorCutLeaderLease leases;

    private final List<String> routes;

    private final int stripes;

    private final HttpClient client = HttpClient.newBuilder()
            .connectTimeout(Duration.ofMillis(500)).build();

    private volatile Endpoint cached;

    /**
     * Using the same layout to configure a detector, the order of shard in the reading response must be fully consistent.
     *
     * @param center center
     * @param shards shards
     * @param stripes stripes
     */
    MysqlVectorCutRemoteClient(final DataSource center, final Map<String, DataSource> shards, final int stripes) {
        Objects.requireNonNull(shards, "shards");
        this.leases = new MysqlVectorCutLeaderLease(center, shards, stripes);
        this.routes = shards.keySet().stream().sorted().toList();
        this.stripes = stripes;
    }

    /**
     * A RC statement takes the current edge of a remote cut; it is rediscovered once the old end point has failed.
     *
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     */
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
            } catch (final InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new MvccException(ErrorCode.STORAGE_UNAVAILABLE,
                        "remote vector cut read interrupted", true, null, ex);
            } catch (final IOException | IllegalArgumentException ex) {
                invalidate(endpoint);
            }
        }
        throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                "no current vector cut leader answered with the expected generation", true, null);
    }

    /**
     * Endpoint caches cover only the conservative front-end of lease, avoiding reading control libraries for each RC.
     *
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     */
    private synchronized Endpoint resolve() {
        Endpoint current = cached;
        if (current != null && current.valid()) {
            return current;
        }
        long started = System.nanoTime();
        var leader = leases.discover().orElseThrow(() -> new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
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

    /**
     * The return cycle experiment allows only a digital IPv4 return ring address and valid port to avoid error lease pointing at an external service.
     *
     * @param ownerId owner id
     * @throws IllegalArgumentException if the operation cannot complete
     */
    private static void requireLoopbackEndpoint(final String ownerId) {
        if (!ownerId.startsWith("127.0.0.1:")) {
            throw new IllegalArgumentException("vector cut owner is not a loopback endpoint");
        }
        String portText = ownerId.substring("127.0.0.1:".length());
        try {
            int port = Integer.parseInt(portText);
            if (port < 1 || port > 65535) {
                throw new IllegalArgumentException("vector cut owner port is invalid");
            }
        } catch (final NumberFormatException ex) {
            throw new IllegalArgumentException("vector cut owner port is invalid", ex);
        }
    }

    /**
     * Just clean up the old endpoint of this failure and keep other threads that have just refreshed new leaders.
     *
     * @param endpoint endpoint
     */
    private synchronized void invalidate(final Endpoint endpoint) {
        if (cached == endpoint) {
            cached = null;
        }
    }

    /**
     * Cache period is earlier than centre lease, expired or failed to be rediscovered.
     *
     * @param ownerId owner id
     * @param generation generation
     * @param validUntilNanos valid until nanos
     */
    private record Endpoint(String ownerId, long generation, long validUntilNanos) {

        /**
         * Expired endpoints should not be reused for subsequent transaction.
         *
         * @return the operation result
         */
        boolean valid() {
            return validUntilNanos - System.nanoTime() > 0;
        }
    }
}
