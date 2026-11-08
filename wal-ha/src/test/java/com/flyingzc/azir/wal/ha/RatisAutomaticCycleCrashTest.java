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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Assertions;

import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.lifecycle.Startables;

import java.nio.charset.StandardCharsets;

/**
 * Automatic snapshot, version GC check with three JVM/ MySQL for the actual collection cycle and external confirmation.
 */
class RatisAutomaticCycleCrashTest {

    static final List<org.testcontainers.containers.MySQLContainer<?>> DATABASES = List.of(MysqlGenerationSupport.database(), MysqlGenerationSupport.database(), MysqlGenerationSupport.database());

    @TempDir
    Path directory;

    /**
     * Full-class reuse of three self-limited cases, with new control libraries and groups identity used for each parameter scene.
     */
    @BeforeAll
    static void startDatabases() {
        Startables.deepStart(DATABASES.stream()).join();
    }

    /**
     * Only the database containers created by this test are removed and other user services are retained.
     */
    @AfterAll
    static void stopDatabases() {
        for (var database : DATABASES) {
            database.stop();
        }
    }

    /**
     * Automatic policy drives real mirrors with collection, and the parent process can not be manually called snapshot or purge when it reaches the defined boundary.
     *
     * @param boundary boundary
     * @throws Exception if the operation cannot complete
     */
    @ParameterizedTest
    @ValueSource(strings = {"AFTER_IMAGE_PREPARED", "AFTER_IMAGE_COMPLETE", "AFTER_IMAGE_PUBLISHED", "BEFORE_IMAGE_ACTIVE", "AFTER_IMAGE_ACTIVE", "BEFORE_LOG_PURGE", "AFTER_LOG_PURGE"})
    void automaticCycleSurvivesOwnedProcessKill(final String boundary) throws Exception {
        try (var cluster = new Cluster(directory, boundary)) {
            int source = cluster.leader(-1);
            var receipts = new ArrayList<Receipt>();
            receipts.add(commit(cluster.nodes.get(source), "INIT", 1));
            for (int value = 1; value <= 10; value++) {
                receipts.add(commit(cluster.nodes.get(source), "WRITE " + value, value + 1));
            }
            receipts.add(commit(cluster.nodes.get(source), "DELETE", 12));
            for (int i = 0; i < 3; i++) {
                cluster.cold(i, 12);
            }
            long beforeFirst = Long.parseLong(cluster.nodes.get(source).request("LOG").split(" ")[1]);
            Assertions.assertEquals(0, beforeFirst);
            Assertions.assertEquals("ARMED " + boundary, cluster.nodes.get(source).request("ARM " + boundary));
            for (var child : cluster.nodes) {
                Assertions.assertEquals("LIFECYCLE true true", child.request("LIFECYCLE"));
            }
            var paused = cluster.nodes.get(source);
            paused.boundary(boundary);
            boolean concurrent = Set.of("AFTER_IMAGE_PREPARED", "AFTER_IMAGE_COMPLETE", "AFTER_IMAGE_PUBLISHED", "BEFORE_LOG_PURGE").contains(boundary);
            int live = 0;
            if (concurrent) {
                live = 1;
                receipts.add(commit(paused, "LIVE 1", 13));
            }
            if (boundary.equals("BEFORE_LOG_PURGE")) {
                Assertions.assertEquals(beforeFirst, paused.proofFirst);
                assertTrue(paused.proofCovered >= beforeFirst);
                Assertions.assertEquals(beforeFirst, cluster.nativeFirstFile(source));
                cluster.history.record(source, paused.process.pid(), "CHECK", "NATIVE_FIRST_FILE " + beforeFirst);
            }
            if (boundary.equals("AFTER_LOG_PURGE")) {
                assertTrue(paused.proofFirst > beforeFirst);
                assertTrue(paused.proofFirst - 1 <= paused.proofCovered);
                Assertions.assertEquals(paused.proofFirst, cluster.nativeFirstFile(source));
                cluster.history.record(source, paused.process.pid(), "CHECK", "NATIVE_FIRST_FILE " + paused.proofFirst);
            }
            var firstActive = cluster.active(source);
            long image = cluster.imageIndex(source);
            if (Set.of("AFTER_IMAGE_ACTIVE", "BEFORE_LOG_PURGE", "AFTER_LOG_PURGE").contains(boundary)) {
                Assertions.assertEquals(12, firstActive.sequence());
                assertTrue(firstActive.index() >= 0);
            }
            if (Set.of("AFTER_IMAGE_PREPARED", "AFTER_IMAGE_COMPLETE", "AFTER_IMAGE_PUBLISHED", "BEFORE_IMAGE_ACTIVE").contains(boundary)) {
                Assertions.assertEquals(-1, firstActive.index());
            }
            cluster.capture(source, "first");
            paused.kill(boundary);
            int kills = 1;
            int next = cluster.leader(source);
            live++;
            receipts.add(commit(cluster.nodes.get(next), "LIVE " + live, 12L + live));
            UUID resumedGeneration = null;
            if (boundary.equals("AFTER_IMAGE_PUBLISHED")) {
                cluster.replace(source, "MID_RESTORE_BATCH");
                cluster.nodes.get(source).boundary("MID_RESTORE_BATCH");
                resumedGeneration = cluster.capture(source, "recovery");
                assertNotNull(resumedGeneration);
                cluster.partial(source);
                cluster.nodes.get(source).kill("MID_RESTORE_BATCH");
                kills++;
                next = cluster.leader(source);
                live++;
                receipts.add(commit(cluster.nodes.get(next), "LIVE " + live, 12L + live));
            }
            cluster.replace(source, resumedGeneration == null ? null : "AFTER_ACTIVE");
            if (resumedGeneration != null) {
                cluster.nodes.get(source).boundary("AFTER_ACTIVE");
                try {
                    Assertions.assertEquals(resumedGeneration, cluster.capture(source, "resumed"));
                    Assertions.assertEquals(resumedGeneration, cluster.active(source).generation());
                } finally {
                    cluster.release(source);
                }
            }
            cluster.nodes.get(source).started();
            long sequence = 12L + live;
            cluster.await(source, sequence, image);
            cluster.cold(source, sequence);
            Assertions.assertEquals("POLICY true true", cluster.nodes.get(source).request("POLICY"));
            next = cluster.leader(-1);
            if (next != source) {
                Assertions.assertEquals("TRANSFER true", cluster.nodes.get(next).request("TRANSFER p" + source));
                cluster.awaitLeader(source);
            }
            cluster.capture(source, "final");
            Assertions.assertEquals("DATA " + sequence + " " + expectedHash(10, live), cluster.nodes.get(source).request("READ"));
            for (var receipt : receipts) {
                Assertions.assertEquals("LOOKUP COMMITTED " + receipt.sequence() + " " + receipt.digest(), cluster.nodes.get(source).request("LOOKUP " + receipt.id()));
            }
            live++;
            receipts.add(commit(cluster.nodes.get(source), "LIVE " + live, 12L + live));
            sequence++;
            Assertions.assertEquals("DATA " + sequence + " " + expectedHash(10, live), cluster.nodes.get(source).request("READ"));
            cluster.history.record(source, cluster.nodes.get(source).process.pid(), "VERIFIED", "KILLS=" + kills + " SEQUENCE=" + sequence + " RECEIPTS=" + receipts.size());
            Files.writeString(directory.resolve("case.json"), "{\"passed\":true,\"boundary\":\"" + boundary + "\",\"group\":\"" + cluster.group + "\",\"source\":" + source + ",\"kills\":" + kills
                    + ",\"concurrentCommit\":" + concurrent + ",\"imageIndex\":" + image + ",\"firstBefore\":" + beforeFirst + ",\"firstAtBoundary\":" + paused.proofFirst + ",\"coveredAtBoundary\":"
                    + paused.proofCovered
                    + ",\"finalSequence\":" + sequence + ",\"confirmedReceipts\":" + receipts.size() + ",\"expectedDigest\":\"" + expectedHash(10, live) + "\"}\n");
        }
    }

    /**
     * The source 's successful response must be complete receipt, INTENT or error does not enter the confirmation prefix.
     *
     * @param child child
     * @param command command
     * @param sequence sequence
     * @return the operation result
     * @throws Exception if the operation cannot complete
     */
    private static Receipt commit(final Child child, final String command, final long sequence) throws Exception {
        String reply = child.request(command);
        assertTrue(reply.startsWith("COMMITTED "), reply);
        String[] parts = reply.split(" ");
        Assertions.assertEquals(sequence, Long.parseLong(parts[2]));
        UUID id = UUID.fromString(parts[1]);
        assertTrue(parts[3].matches("[a-f0-9]{64}"));
        return new Receipt(id, sequence, parts[3]);
    }

    /**
     * The parent process independently constructs the expected content and does not call for a sub-process summary code.
     *
     * @param blob blob
     * @param live live
     * @return the operation result
     * @throws Exception if the operation cannot complete
     */
    private static String expectedHash(final int blob, final int live) throws Exception {
        var hash = java.security.MessageDigest.getInstance("SHA-256");
        for (String shard : List.of("a", "b")) {
            for (int i = 0; i < 32; i++) {
                expectedEntry(hash, shard + "/cold" + i, i == 0 ? null : (shard + ":" + i).getBytes(StandardCharsets.UTF_8));
            }
            byte[] value = new byte[480 * 1024];
            Arrays.fill(value, (byte) (blob + (shard.equals("a") ? 0 : 100)));
            expectedEntry(hash, shard + "/blob", value);
            expectedEntry(hash, shard + "/live", Integer.toString(shard.equals("a") ? live : -live).getBytes(StandardCharsets.UTF_8));
        }
        return java.util.HexFormat.of().formatHex(hash.digest());
    }

    /**
     * Encoding the boundary with a fixed large length, with an empty value different from that of an empty array.
     *
     * @param hash hash
     * @param key business key
     * @param value value to write
     */
    private static void expectedEntry(final java.security.MessageDigest hash, final String key, final byte[] value) {
        byte[] name = key.getBytes(StandardCharsets.UTF_8);
        hash.update(java.nio.ByteBuffer.allocate(4).putInt(name.length).array());
        hash.update(name);
        hash.update(java.nio.ByteBuffer.allocate(4).putInt(value == null ? -1 : value.length).array());
        if (value != null) {
            hash.update(value);
        }
    }

    /**
     * The confirmation is based only on successful responses actually received by the parent process.
     *
     * @param id id
     * @param sequence sequence
     * @param digest digest
     */
    private record Receipt(UUID id, long sequence, String digest) {
    }

    /**
     * The parent process monotonic clock centrally records the order of external observations and injections for all processes.
     */
    private static final class History {

        final Path path;

        /**
         * Only test data, identity and summary are saved without recording environment receipt.
         *
         * @param path path
         */
        History(final Path path) {
            this.path = path;
        }

        /**
         * Each event is synchronized with the external history and the parent process is allowed to continue until you have received a response.
         *
         * @param node node
         * @param pid pid
         * @param kind kind
         * @param text text
         * @throws IOException if the operation cannot complete
         */
        synchronized void record(final int node, final long pid, final String kind, final String text) throws IOException {
            Files.writeString(path, System.nanoTime() + "\t" + node + "\t" + pid + "\t" + kind + "\t" + text + "\n", StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        }
    }

    /**
     * Bounded stdout queue separated start, install event and command response. recovery blockage does not require the forgery of STARTED.
     */
    private static final class Child implements AutoCloseable {

        final Process process;

        final int index;

        final History history;

        long proofFirst = -1, proofCovered = -1;

        final BufferedWriter commands;

        final BlockingQueue<String> events = new ArrayBlockingQueue<>(16);

        final BlockingQueue<String> replies = new ArrayBlockingQueue<>(64);

        final Thread reader;

        /**
         * All process handles are directly created and retained by the parent test, ending reliance on vague names or port guesses.
         *
         * @param process process
         * @param index index
         * @param history history
         * @throws Exception if the operation cannot complete
         */
        Child(final Process process, final int index, final History history) throws Exception {
            this.process = process;
            this.index = index;
            this.history = history;
            commands = new BufferedWriter(new OutputStreamWriter(process.getOutputStream(), StandardCharsets.UTF_8));
            history.record(index, process.pid(), "SPAWN", "OWNED_JVM");
            reader = Thread.ofPlatform().daemon().name("mysql-probe-reader-" + process.pid()).start(() -> {
                try (var input = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                    for (String line; (line = input.readLine()) != null;) {
                        if (!line.startsWith("AZIR ")) {
                            continue;
                        }
                        String value = line.substring(5);
                        history.record(index, process.pid(), "RECV", value);
                        if (value.startsWith("BOOT ") || value.startsWith("STARTED ") || value.startsWith("BOUNDARY ") || value.startsWith("PROOF ")) {
                            events.put(value);
                        } else {
                            replies.put(value);
                        }
                    }
                } catch (final Exception failure) {
                    replies.offer("READER_ENDED");
                } finally {
                    events.offer("TERMINAL");
                }
            });
            String boot = events.poll(30, TimeUnit.SECONDS);
            Assertions.assertEquals("BOOT " + process.pid(), boot);
        }

        /**
         * Only a process that is clearly waiting for the normal service to start requires STARTED, installation recovery failure can take place before this happens.
         *
         * @throws Exception if the operation cannot complete
         */
        void started() throws Exception {
            String event = events.poll(90, TimeUnit.SECONDS);
            Assertions.assertEquals("STARTED " + process.pid(), event);
        }

        /**
         * Waiting for a physical return and no fixed sleep inference as to which boundary is installed.
         *
         * @param expected expected
         * @throws AssertionError if the operation cannot complete
         * @throws Exception if the operation cannot complete
         */
        void boundary(final String expected) throws Exception {
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
            while (System.nanoTime() < until) {
                String event = events.poll(Math.max(1, until - System.nanoTime()), TimeUnit.NANOSECONDS);
                if (event != null && event.startsWith("STARTED ")) {
                    continue;
                }
                if (event != null && event.startsWith("PROOF ")) {
                    String[] fields = event.split(" ");
                    Assertions.assertEquals(expected, fields[1]);
                    proofFirst = Long.parseLong(fields[2]);
                    proofCovered = Long.parseLong(fields[3]);
                    continue;
                }
                Assertions.assertEquals("BOUNDARY " + expected + " " + process.pid(), event);
                assertTrue(process.isAlive());
                return;
            }
            throw new AssertionError("boundary observation expired; alive=" + process.isAlive());
        }

        /**
         * The same process serial command, waiting for expiry to report only failure to observe and will not be restarted on its own.
         *
         * @param command command
         * @return the operation result
         * @throws AssertionError if the operation cannot complete
         * @throws Exception if the operation cannot complete
         */
        synchronized String request(final String command) throws Exception {
            if (!process.isAlive()) {
                throw new AssertionError("owned process already terminal");
            }
            history.record(index, process.pid(), "SEND", command);
            commands.write(command);
            commands.newLine();
            commands.flush();
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (System.nanoTime() < until) {
                String reply = replies.poll(Math.max(1, until - System.nanoTime()), TimeUnit.NANOSECONDS);
                if (reply != null && reply.startsWith("INTENT ")) {
                    continue;
                }
                if (reply != null) {
                    return reply;
                }
                break;
            }
            throw new AssertionError("command observation expired; alive=" + process.isAlive());
        }

        /**
         * Tests are allowed to start the substitution process only when non-zero terminal state is killed and confirmed after a real border.
         *
         * @param boundary boundary
         * @throws Exception if the operation cannot complete
         */
        void kill(final String boundary) throws Exception {
            history.record(index, process.pid(), "KILL", boundary);
            process.destroyForcibly();
            assertTrue(process.waitFor(10, TimeUnit.SECONDS));
            Assertions.assertNotEquals(0, process.exitValue());
            history.record(index, process.pid(), "EXIT", Integer.toString(process.exitValue()));
        }

        /**
         * The clean-up only serves the process held in this test, and normal cessation of failed forced clean-up is recorded separately from failure.
         *
         * @throws Exception if the operation cannot complete
         */
        @Override
        public void close() throws Exception {
            if (process.isAlive()) {
                try {
                    history.record(index, process.pid(), "SEND", "STOP");
                    commands.write("STOP");
                    commands.newLine();
                    commands.flush();
                } catch (final IOException ignored) {
                }
                if (!process.waitFor(20, TimeUnit.SECONDS)) {
                    history.record(index, process.pid(), "CLEANUP_KILL", "OWNED_PROCESS");
                    process.destroyForcibly();
                    assertTrue(process.waitFor(10, TimeUnit.SECONDS));
                }
            }
            try {
                commands.close();
            } catch (final IOException ignored) {
                if (process.isAlive()) {
                    throw ignored;
                }
            }
            reader.join(2000);
        }
    }

    /**
     * Each scenario has three actual JVMs, three independent control libraries and durable directories, and failure replaces only clear targets.
     */
    private static final class Cluster implements AutoCloseable {

        final Path root;

        final String boundary;

        final UUID group = UUID.randomUUID();

        final List<Integer> ports = new ArrayList<>();

        final List<String> schemas = new ArrayList<>();

        final List<Child> nodes = new ArrayList<>();

        final History history;

        /**
         * Three database s were allocated to private control of the scene only in containers owned by this round.
         *
         * @param root root
         * @param boundary boundary
         * @throws Exception if the operation cannot complete
         */
        Cluster(final Path root, final String boundary) throws Exception {
            this.root = root;
            this.boundary = boundary;
            history = new History(root.resolve("events.tsv"));
            var held = new ArrayList<java.net.ServerSocket>();
            try {
                for (int i = 0; i < 3; i++) {
                    var socket = MysqlGenerationSupport.reservePeerPort();
                    held.add(socket);
                    ports.add(socket.getLocalPort());
                }
            } finally {
                for (var socket : held) {
                    socket.close();
                }
            }
            try {
                for (int i = 0; i < 3; i++) {
                    String schema = "c_" + UUID.randomUUID().toString().replace("-", "");
                    MysqlGenerationSupport.createControl(DATABASES.get(i), schema);
                    schemas.add(schema);
                    nodes.add(start(i, null));
                    nodes.get(i).started();
                }
            } catch (final Throwable failure) {
                close();
                throw failure;
            }
        }

        /**
         * SubJVM ' s largest stack of 192 MiB, a processor, receipt is passed through the environment and will not be written in history.
         *
         * @param index index
         * @param fault fault
         * @return the operation result
         * @throws Exception if the operation cannot complete
         */
        Child start(final int index, final String fault) throws Exception {
            Path directory = root.resolve("p" + index);
            Files.createDirectories(directory);
            String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
            var args = new ArrayList<>(List.of(Path.of(System.getProperty("java.home"), "bin", "java").toString(), "-Xmx192m", "-XX:ActiveProcessorCount=1", "-cp", classpath,
                    RatisMysqlProcessProbe.class.getName(), directory.toString(), "p" + index, group.toString()));
            for (int port : ports) {
                args.add(Integer.toString(port));
            }
            var builder = new ProcessBuilder(args).redirectError(directory.resolve("stderr-" + System.nanoTime() + ".log").toFile());
            builder.environment().put("AZIR_WAL_PROBE_JDBC_URL", DATABASES.get(index).getJdbcUrl());
            builder.environment().put("AZIR_WAL_PROBE_CONTROL_SCHEMA", schemas.get(index));
            builder.environment().put("AZIR_WAL_PROBE_PASSWORD", DATABASES.get(index).getPassword());
            if (fault != null) {
                builder.environment().put("AZIR_WAL_PROBE_FAULT", fault);
            } else {
                builder.environment().remove("AZIR_WAL_PROBE_FAULT");
            }
            builder.environment().remove("AZIR_WAL_PROBE_RELEASE_FILE");
            if ("AFTER_ACTIVE".equals(fault)) {
                Files.deleteIfExists(directory.resolve(".auto-release"));
                builder.environment().put("AZIR_WAL_PROBE_RELEASE_FILE", directory.resolve(".auto-release").toAbsolutePath().toString());
            }
            Process process = builder.start();
            try {
                return new Child(process, index, history);
            } catch (final Throwable failure) {
                process.destroyForcibly();
                process.waitFor(10, TimeUnit.SECONDS);
                throw failure;
            }
        }

        /**
         * The verify recovery operation will not continue until it has been published as it is, and the older operation cannot be lost by miscalculation due to subsequent legal change of
         * higher snapshot.
         *
         * @param index index
         * @throws IOException if the operation cannot complete
         */
        void release(final int index) throws IOException {
            history.record(index, nodes.get(index).process.pid(), "RELEASE", "AFTER_ACTIVE");
            Files.writeString(root.resolve("p" + index + "/.auto-release"), "resume", StandardOpenOption.CREATE_NEW);
        }

        /**
         * The old process must be confirmed as terminal state or visible normal shut-down, and the replacement cannot be triggered by observation timeout.
         *
         * @param index index
         * @param fault fault
         * @throws Exception if the operation cannot complete
         */
        void replace(final int index, final String fault) throws Exception {
            var old = nodes.get(index);
            old.close();
            nodes.set(index, start(index, fault));
        }

        /**
         * The current majority can select new health leaders and avoid using old role caches as proof of continued service.
         *
         * @param excluded excluded
         * @return the operation result
         * @throws AssertionError if the operation cannot complete
         * @throws Exception if the operation cannot complete
         */
        int leader(final int excluded) throws Exception {
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(45);
            while (System.nanoTime() < until) {
                for (int i = 0; i < nodes.size(); i++) {
                    var node = nodes.get(i);
                    if (i == excluded || !node.process.isAlive()) {
                        continue;
                    }
                    String status = node.request("STATUS");
                    if (status.startsWith("STATUS ") && Long.parseLong(status.split(" ")[1]) > 0 && node.request("PING").startsWith("PING ")) {
                        return i;
                    }
                }
                TimeUnit.MILLISECONDS.sleep(30);
            }
            throw new AssertionError("no healthy process with majority barrier");
        }

        /**
         * Checks the actual cold data watermark and cut-off results, instead of using normal memory queries as cold recovery.
         *
         * @param index index
         * @param sequence sequence
         * @throws AssertionError if the operation cannot complete
         * @throws Exception if the operation cannot complete
         */
        void cold(final int index, final long sequence) throws Exception {
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
            while (System.nanoTime() < until) {
                String reply = nodes.get(index).request("COLD " + sequence);
                if (reply.equals("COLD " + sequence + " 0 100")) {
                    return;
                }
                TimeUnit.MILLISECONDS.sleep(30);
            }
            throw new AssertionError("owned MySQL process did not reach cold checkpoint");
        }

        /**
         * Recovery to complete snapshot and follow-up logs before it becomes dominant, during which failure does not trigger a restart.
         *
         * @param index index
         * @param sequence sequence
         * @param snapshot snapshot
         * @throws AssertionError if the operation cannot complete
         * @throws Exception if the operation cannot complete
         */
        void await(final int index, final long sequence, final long snapshot) throws Exception {
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
            while (System.nanoTime() < until) {
                String reply = nodes.get(index).request("STATUS");
                if (reply.startsWith("STATUS ")) {
                    String[] fields = reply.split(" ");
                    if (fields[2].equals(Long.toString(sequence)) && fields[3].equals("true") && Long.parseLong(fields[4]) >= snapshot) {
                        return;
                    }
                }
                TimeUnit.MILLISECONDS.sleep(30);
            }
            throw new AssertionError("recovered process did not finish installation and catch up");
        }

        /**
         * The master needs real majority read barrier, not just to check the management response.
         *
         * @param index index
         * @throws AssertionError if the operation cannot complete
         * @throws Exception if the operation cannot complete
         */
        void awaitLeader(final int index) throws Exception {
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (System.nanoTime() < until) {
                String status = nodes.get(index).request("STATUS");
                if (status.startsWith("STATUS ") && Long.parseLong(status.split(" ")[1]) > 0 && nodes.get(index).request("PING").startsWith("PING ")) {
                    return;
                }
                TimeUnit.MILLISECONDS.sleep(30);
            }
            throw new AssertionError("transfer target did not become serving leader");
        }

        /**
         * Checks the paragraph file name and independently checks the original birthday header index instead of just believing in memory counts.
         *
         * @param index index
         * @return the operation result
         * @throws IOException if the operation cannot complete
         */
        long nativeFirstFile(final int index) throws IOException {
            long first = Long.MAX_VALUE;
            Path current = root.resolve("p" + index + "/raft/" + group + "/current");
            var pattern = java.util.regex.Pattern.compile("log_(?:inprogress_)?([0-9]+)(?:-[0-9]+)?");
            try (var files = Files.list(current)) {
                for (var path : files.toList()) {
                    var match = pattern.matcher(path.getFileName().toString());
                    if (match.matches()) {
                        first = Math.min(first, Long.parseLong(match.group(1)));
                    }
                }
            }
            if (first == Long.MAX_VALUE) {
                throw new IOException("native segment evidence missing");
            }
            return first;
        }

        /**
         * Only the full Ratis mirror name after the atomic release is recognized and not a private build directory as snapshot.
         *
         * @param index index
         * @return the operation result
         * @throws IOException if the operation cannot complete
         */
        long imageIndex(final int index) throws IOException {
            Path sm = root.resolve("p" + index + "/raft/" + group + "/sm");
            long latest = -1;
            try (var files = Files.list(sm)) {
                for (var path : files.toList()) {
                    String name = path.getFileName().toString();
                    if (name.matches("snapshot\\.[0-9]+_[0-9]+")) {
                        latest = Math.max(latest, Long.parseLong(name.substring(name.indexOf('_') + 1)));
                    }
                }
            }
            return latest;
        }

        /**
         * Reads the real live pointer.
         *
         * @param index index
         * @return the operation result
         * @throws IOException if the operation cannot complete
         */
        RatisGenerationRecord active(final int index) throws IOException {
            return RatisGenerationRecord.read(root.resolve("p" + index + "/business/ACTIVE"));
        }

        /**
         * Keep independent small file evidence of the pointer and operation before killing, check recovery for use of original generation.
         *
         * @param index index
         * @param label label
         * @return the operation result
         * @throws Exception if the operation cannot complete
         */
        UUID capture(final int index, final String label) throws Exception {
            Path business = root.resolve("p" + index + "/business");
            var active = active(index);
            Files.copy(business.resolve("ACTIVE"), root.resolve(label + "-active.record"));
            UUID generation = active.index() >= 0 ? active.generation() : null;
            if (Files.exists(business.resolve("INSTALLING"))) {
                var pending = RatisGenerationRecord.read(business.resolve("INSTALLING"));
                generation = pending.generation();
                Files.copy(business.resolve("INSTALLING"), root.resolve(label + "-installing.record"));
            }
            history.record(index, nodes.get(index).process.pid(), "CHECK", "GENERATION " + (generation == null ? "UNPUBLISHED" : generation));
            return generation;
        }

        /**
         * MID_RESTORE_BATCH must correspond to the actual two rows of commit with prefix not yet released database status.
         *
         * @param index index
         * @throws Exception if the operation cannot complete
         */
        void partial(final int index) throws Exception {
            var pending = RatisGenerationRecord.read(root.resolve("p" + index + "/business/INSTALLING"));
            try (var control = MysqlGenerationSupport.control(DATABASES.get(index), schemas.get(index))) {
                var factory = new MysqlWalReplicaBackendFactory(Map.of("a", control), MysqlWalReplicaBackendFactory.Options.defaults());
                String schema = factory.databaseName(pending.identity(), pending.generation(), "a");
                try (
                        var connection = control.getConnection(); var query = connection.createStatement();
                        var row = query.executeQuery("select restoring,applied_seq,(select count(*) from `" + schema + "`.azir_wal_version) from `" + schema + "`.azir_wal_binding where id=1")) {
                    assertTrue(row.next());
                    assertTrue(row.getBoolean(1));
                    Assertions.assertEquals(0, row.getLong(2));
                    Assertions.assertEquals(2, row.getInt(3));
                    history.record(index, nodes.get(index).process.pid(), "CHECK", "PARTIAL_RESTORE 2 1 0");
                }
            }
        }

        /**
         * Closes all self-owned processes to archive dissensitive history, logs and small metadata without saving any connection environment.
         *
         * @throws Exception if the operation cannot complete
         */
        @Override
        public void close() throws Exception {
            Throwable failure = null;
            try {
                for (var node : nodes) {
                    try {
                        node.close();
                    } catch (final Throwable close) {
                        if (failure == null) {
                            failure = close;
                        } else {
                            failure.addSuppressed(close);
                        }
                    }
                }
            } finally {
                String configured = System.getProperty("azir.wal.ha.autoCycleEvidenceDir");
                if (configured != null) {
                    Path target = Path.of(configured).toAbsolutePath().resolve(boundary + "-" + group);
                    Files.createDirectories(target);
                    try (var paths = Files.walk(root)) {
                        for (Path path : paths.filter(Files::isRegularFile).toList()) {
                            String name = path.getFileName().toString();
                            if (!name.endsWith(".log") && !name.endsWith(".record") && !name.equals("events.tsv") && !name.equals("case.json")) {
                                continue;
                            }
                            Path output = target.resolve(root.relativize(path));
                            Files.createDirectories(output.getParent());
                            Files.copy(path, output);
                        }
                    }
                }
            }
            if (failure instanceof Exception exception) {
                throw exception;
            }
            if (failure instanceof Error error) {
                throw error;
            }
        }
    }
}
