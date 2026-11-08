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

package com.flyingzc.azir.wal;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Assertions;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.zip.GZIPOutputStream;

/**
 * Independent bounded version model, reference data only from successful caller receipt and planned write set, not read engine version chain.
 */
final class WalSteadyModel implements AutoCloseable {

    private final Map<String, TreeMap<Long, String>> chains = new HashMap<>();

    private final Map<String, Long> pins = new HashMap<>();

    private final TreeMap<Long, Commit> pending = new TreeMap<>();

    private final BufferedWriter trace;

    private long known;

    private long observations;

    private long samples;

    private WalReceipt firstReceipt;

    private WalReceipt lastReceipt;

    /**
     * Fluid evidence is kept, and long-term stability does not allow for the full accumulation of history to be loaded into JVM.
     *
     * @param path path
     * @throws IOException if the operation cannot complete
     */
    WalSteadyModel(final Path path) throws IOException {
        trace = new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(Files.newOutputStream(path,
                java.nio.file.StandardOpenOption.CREATE_NEW)), StandardCharsets.UTF_8));
        trace.write("H\t2\n");
    }

    /**
     * Protects earlier known borders before engines become and avoids competition between model collection and reader registration.
     *
     * @param token token
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    synchronized long pin(final String token) throws IOException {
        assertNull(pins.put(token, known));
        trace.write("B\t" + token + "\t" + known + "\n");
        return known;
    }

    /**
     * Release model protection after the end of transaction and clean up historical anchors that are no longer required.
     *
     * @param token token
     * @throws IOException if the operation cannot complete
     */
    synchronized void unpin(final String token) throws IOException {
        assertNotNull(pins.remove(token));
        trace.write("E\t" + token + "\n");
        prune();
    }

    /**
     * The successful result can be returned in a disorderly manner and must be completed with a continuous commit prefix before the open reference reading.
     *
     * @param receipt receipt
     * @param start start
     * @param key business key
     * @param value value to write
     * @throws IOException if the operation cannot complete
     */
    synchronized void committed(final WalReceipt receipt, final long start, final String key, final String value) throws IOException {
        assertTrue(receipt.commitSequence() > known);
        assertNull(pending.put(receipt.commitSequence(), new Commit(receipt, start, key, value)));
        while (pending.containsKey(known + 1)) {
            var commit = pending.remove(++known);
            var chain = chains.computeIfAbsent(commit.key, ignored -> new TreeMap<>());
            assertTrue(commit.start < known);
            if (!chain.isEmpty()) {
                assertTrue(chain.lastKey() <= commit.start, "stale writer accepted");
            }
            chain.put(known, commit.value);
            if (firstReceipt == null) {
                firstReceipt = commit.receipt;
            }
            lastReceipt = commit.receipt;
            trace.write("C\t" + known + "\t" + commit.start + "\t" + commit.receipt.transactionId() + "\t" + commit.key + "\t" + scalar(commit.value) + "\n");
        }
        prune();
        notifyAll();
    }

    /**
     * Each observation was accurately checked for version and only sampled reading evidence was written to limit long-term disk occupancy.
     *
     * @param token token
     * @param snapshot snapshot
     * @param key business key
     * @param left left
     * @param right right
     * @param isolation global transaction isolation level
     * @param start start
     * @param minimum minimum
     * @param sample sample
     * @throws AssertionError if the operation cannot complete
     * @throws Exception if the operation cannot complete
     */
    synchronized void observed(final String token, final long snapshot, final String key, final String left, final String right,
                               final com.flyingzc.azir.common.IsolationLevel isolation, final long start, final long minimum, final boolean sample) throws Exception {
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(30);
        while (known < snapshot) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("reference model could not catch up");
            }
            wait(10);
        }
        assertTrue(pins.containsKey(token));
        assertTrue(snapshot >= pins.get(token));
        var chain = chains.get(key);
        var entry = chain == null ? null : chain.floorEntry(snapshot);
        String expected = entry == null ? null : entry.getValue();
        Assertions.assertEquals(expected, left, "invalid left snapshot version");
        Assertions.assertEquals(expected, right, "invalid right snapshot version");
        observations++;
        if (sample) {
            samples++;
            trace.write("R\t" + token + "\t" + snapshot + "\t" + key + "\t" + scalar(left)
                    + "\t" + scalar(right) + "\t" + isolation + "\t" + start + "\t" + minimum + "\n");
        }
    }

    /**
     * Returns a continuous watermark that is currently confirmed by the successful caller and is used to independently check the real-time lower boundary of RC.
     *
     * @return the operation result
     */
    synchronized long known() {
        return known;
    }

    /**
     * Returns the number of verified readings.
     *
     * @return the operation result
     */
    synchronized long observations() {
        return observations;
    }

    /**
     * Returns the number of reading samples for which original evidence has been saved.
     *
     * @return the operation result
     */
    synchronized long samples() {
        return samples;
    }

    /**
     * Returns the earliest confirmation of identity with a short verify test window that actually occurs receipt collection.
     *
     * @return the operation result
     */
    synchronized WalReceipt firstReceipt() {
        return firstReceipt;
    }

    /**
     * Returns the last continuous confirmation of identity to reset with verify to search for the latest results.
     *
     * @return the operation result
     */
    synchronized WalReceipt lastReceipt() {
        return lastReceipt;
    }

    /**
     * Returns the replica of the final reference state for checking the true MySQL by bypassing the engine cache.
     *
     * @return the operation result
     */
    synchronized Map<String, WalValue> finalValues() {
        var result = new HashMap<String, WalValue>();
        chains.forEach((key, chain) -> {
            var last = chain.lastEntry();
            result.put(key,
                    new WalValue(last.getKey(), last.getValue() == null ? null : last.getValue().getBytes(StandardCharsets.UTF_8)));
        });
        return result;
    }

    /**
     * Periodically update evidence buffer, business count not related to volume pressure measurement.
     *
     * @throws IOException if the operation cannot complete
     */
    synchronized void flush() throws IOException {
        trace.flush();
    }

    /**
     * Fixed active reader numbers only keep snapshot anchors and updates version, and the model cannot grow indefinitely.
     */
    private void prune() {
        long safe = pins.values().stream().mapToLong(Long::longValue).min().orElse(known);
        for (var chain : chains.values()) {
            Long anchor = chain.floorKey(safe);
            if (anchor != null) {
                chain.headMap(anchor, false).clear();
            }
        }
    }

    /**
     * Synthetic business value does not rename with this tombstone mark.
     *
     * @param value value to write
     * @return the operation result
     */
    private static String scalar(final String value) {
        return value == null ? "-" : value;
    }

    /**
     * The final count and gzip tail must be included in the completed evidence, and incomplete documents cannot be used as proof of passage.
     *
     * @throws IOException if the operation cannot complete
     */
    @Override
    public synchronized void close() throws IOException {
        assertTrue(pins.isEmpty());
        assertTrue(pending.isEmpty());
        trace.write("Z\t" + known + "\t" + observations + "\t" + samples + "\n");
        trace.close();
    }

    /**
     * Foreground has been confirmed but may not yet enter the model in global order.
     *
     * @param receipt receipt
     * @param start start
     * @param key business key
     * @param value value to write
     */
    private record Commit(WalReceipt receipt, long start, String key, String value) {
    }
}
