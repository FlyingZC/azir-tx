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

/**
 * Authenticate connection, call and life cycle budget, in milliseconds.
 *
 * @param connections connections
 * @param calls calls
 * @param streamsPerConnection streams per connection
 * @param metadataBytes metadata bytes
 * @param listenBacklog listen backlog
 * @param handshakeMillis handshake millis
 * @param idleMillis idle millis
 * @param connectionAgeMillis connection age millis
 */
public record RatisTransportLimits(int connections, int calls, int streamsPerConnection, int metadataBytes,
        int listenBacklog, long handshakeMillis, long idleMillis, long connectionAgeMillis) {

    /**
     * All external connection budget must be limited and cannot mean infinity with zero.
     *
     * @param connections connections
     * @param calls calls
     * @param streamsPerConnection streams per connection
     * @param metadataBytes metadata bytes
     * @param listenBacklog listen backlog
     * @param handshakeMillis handshake millis
     * @param idleMillis idle millis
     * @param connectionAgeMillis connection age millis
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public RatisTransportLimits {
        if (connections < 1 || connections > 4096 || calls < 1 || calls > 4096 || streamsPerConnection < 1 || streamsPerConnection > 1024
                || metadataBytes < 1024 || metadataBytes > 65536 || listenBacklog < 16 || listenBacklog > 4096
                || handshakeMillis < 100 || handshakeMillis > 30_000 || idleMillis < 1000 || idleMillis > 3_600_000
                || connectionAgeMillis < 1000 || connectionAgeMillis > 86_400_000) {
            throw new IllegalArgumentException("invalid authenticated transport limits");
        }
    }

    /**
     * Defaults to call up to sixty-four connection, thirty-two, with a handshake of up to three seconds per connection eight stream.
     *
     * @return the operation result
     */
    public static RatisTransportLimits defaults() {
        return new RatisTransportLimits(64, 32, 8, 8192, 128, 3000, 60_000, 900_000);
    }
}
