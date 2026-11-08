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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Read-only tangent bounded binary network format, algebra, routing and each serial number together validate.
 */
final class MysqlVectorCutWire {

    private static final int MAGIC = 0x415A5643;

    private static final int VERSION = 1;

    private static final int MAX_BYTES = 1024 * 1024;

    /**
     * The network codec does not need to save process status.
     */
    private MysqlVectorCutWire() {
    }

    /**
     * Encoding the leader ' s algebra and fixed cut-off as a non-separable response.
     *
     * @param view view
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     * @throws IllegalStateException if the operation cannot complete
     */
    static byte[] encode(final MysqlVectorCutLeaderRuntime.PublishedView view) {
        try (var bytes = new ByteArrayOutputStream(); var output = new DataOutputStream(bytes)) {
            output.writeInt(MAGIC);
            output.writeInt(VERSION);
            output.writeLong(view.generation());
            output.writeLong(view.snapshot().decisionEpoch());
            List<String> routes = view.snapshot().localSequences().keySet().stream().sorted().toList();
            output.writeInt(routes.size());
            for (String route : routes) {
                byte[] encodedName = route.getBytes(StandardCharsets.UTF_8);
                output.writeInt(encodedName.length);
                output.write(encodedName);
                List<Long> sequences = view.snapshot().localSequences().get(route);
                output.writeInt(sequences.size());
                for (long sequence : sequences) {
                    output.writeLong(sequence);
                }
            }
            output.flush();
            byte[] encoded = bytes.toByteArray();
            if (encoded.length > MAX_BYTES) {
                throw new IllegalArgumentException("vector cut response exceeds wire limit");
            }
            return encoded;
        } catch (final IOException ex) {
            throw new IllegalStateException("encode in-memory vector cut failed", ex);
        }
    }

    /**
     * Client rejects error version, algebra, route layout, number of strips and cut-off response.
     *
     * @param encoded encoded
     * @param expectedRoutes expected routes
     * @param stripes stripes
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    static MysqlVectorCutLeaderRuntime.PublishedView decode(final byte[] encoded,
                                                            final List<String> expectedRoutes, final int stripes) {
        if (encoded == null || encoded.length < 28 || encoded.length > MAX_BYTES
                || expectedRoutes == null || expectedRoutes.isEmpty()
                || expectedRoutes.size() > 128 || stripes < 1 || stripes > 256) {
            throw new IllegalArgumentException("invalid vector cut wire length or layout");
        }
        try (var input = new DataInputStream(new ByteArrayInputStream(encoded))) {
            if (input.readInt() != MAGIC || input.readInt() != VERSION) {
                throw new IllegalArgumentException("unknown vector cut wire protocol");
            }
            long generation = input.readLong();
            long epoch = input.readLong();
            int routeCount = input.readInt();
            if (generation < 1 || epoch < 0 || routeCount != expectedRoutes.size()) {
                throw new IllegalArgumentException("vector cut wire generation or route count changed");
            }
            Map<String, List<Long>> values = new LinkedHashMap<>();
            for (int index = 0; index < routeCount; index++) {
                int nameLength = input.readInt();
                if (nameLength < 1 || nameLength > 128 || nameLength > input.available()) {
                    throw new IllegalArgumentException("vector cut wire route name is invalid");
                }
                byte[] routeBytes = input.readNBytes(nameLength);
                String route = new String(routeBytes, StandardCharsets.UTF_8);
                if (!expectedRoutes.get(index).equals(route) || input.readInt() != stripes) {
                    throw new IllegalArgumentException("vector cut wire route layout changed");
                }
                Long[] sequences = new Long[stripes];
                for (int stripe = 0; stripe < stripes; stripe++) {
                    long sequence = input.readLong();
                    if (sequence < 0) {
                        throw new IllegalArgumentException("vector cut wire has negative sequence");
                    }
                    sequences[stripe] = sequence;
                }
                values.put(route, List.of(sequences));
            }
            if (input.available() != 0) {
                throw new IllegalArgumentException("vector cut wire contains trailing bytes");
            }
            return new MysqlVectorCutLeaderRuntime.PublishedView(generation,
                    new MysqlStripedSnapshotOracle.Snapshot(epoch, values));
        } catch (final IOException ex) {
            throw new IllegalArgumentException("vector cut wire response was truncated", ex);
        }
    }
}
