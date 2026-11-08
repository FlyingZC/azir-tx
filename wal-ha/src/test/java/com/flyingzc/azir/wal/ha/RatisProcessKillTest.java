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

import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Assertions;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Three independent JVMs were identified and killed and could not be used to close off with the object of the process as an impostor to collapse.
 */
class RatisProcessKillTest {

    @TempDir
    Path directory;

    /**
     * When a true confirmation is made and the leader is killed, the new master retains confirmation that the original node will be reopened and evened with full durable status.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void acknowledgedCrossShardDataSurvivesJvmKillAndRestart() throws Exception {
        var ports = ports();
        UUID group = UUID.randomUUID();
        var children = new ArrayList<Child>();
        try {
            for (int i = 0; i < 3; i++) {
                children.add(start(i, group, ports));
            }
            int first = leader(children);
            String value = Base64.getUrlEncoder().withoutPadding().encodeToString("confirmed".getBytes(StandardCharsets.UTF_8));
            String ack = children.get(first).request("WRITE " + value);
            assertTrue(ack.startsWith("COMMITTED "), ack);
            String id = ack.split(" ")[1];
            Assertions.assertEquals("1", ack.split(" ")[2]);
            Files.writeString(directory.resolve("acknowledged.txt"), id + "\t1\t" + value + "\n");
            Child killed = children.get(first);
            killed.kill();
            int next = leader(children);
            Assertions.assertNotEquals(first, next);
            Assertions.assertEquals("VALUES " + value + " " + value, children.get(next).request("READ"));
            Assertions.assertEquals("LOOKUP COMMITTED", children.get(next).request("LOOKUP " + id));
            String newer = Base64.getUrlEncoder().withoutPadding().encodeToString("after-failover".getBytes(StandardCharsets.UTF_8));
            String second = children.get(next).request("WRITE " + newer);
            assertTrue(second.endsWith(" 2"), second);
            killed.close();
            children.set(first, start(first, group, ports));
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            long applied = 0;
            while (System.nanoTime() < until) {
                String status = children.get(first).request("STATUS");
                applied = Long.parseLong(status.split(" ")[2]);
                if (applied >= 2) {
                    break;
                }
            }
            Assertions.assertEquals(2, applied);
            Assertions.assertEquals("VALUES " + newer + " " + newer, children.get(next).request("READ"));
        } finally {
            finish(children, "confirmed-kill-restart");
        }
    }

    /**
     * Two independent replica processes are offline, and the original leader cannot confirm new writings or create new readings with old caches.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void minorityProcessCannotAcknowledgeOrOpenSnapshot() throws Exception {
        var ports = ports();
        UUID group = UUID.randomUUID();
        var children = new ArrayList<Child>();
        try {
            for (int i = 0; i < 3; i++) {
                children.add(start(i, group, ports));
            }
            int first = leader(children);
            var node = children.get(first);
            String value = value("confirmed");
            assertTrue(node.request("WRITE " + value).endsWith(" 1"));
            assertTrue(node.request("BUFFER " + value("unconfirmed")).startsWith("BUFFERED "));
            for (int i = 0; i < 3; i++) {
                if (i != first) {
                    children.get(i).kill();
                }
            }
            String decision = node.request("FINISH");
            assertTrue(decision.startsWith("ERROR "), decision);
            Assertions.assertEquals("1", node.request("STATUS").split(" ")[2]);
            String read = node.request("READ");
            assertTrue(read.startsWith("ERROR "), read);
            Assertions.assertEquals("1", node.request("STATUS").split(" ")[2]);
        } finally {
            finish(children, "minority-rejects");
        }
    }

    /**
     * Suspend the old master and then cut it by a majority. The old master recovery cannot be local pending write set before the commit switch.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    @EnabledOnOs(OS.LINUX)
    void pausedOldLeaderCannotCommitOldTransactionAfterNewLeaderWrites() throws Exception {
        var ports = ports();
        UUID group = UUID.randomUUID();
        var children = new ArrayList<Child>();
        Child paused = null;
        try {
            for (int i = 0; i < 3; i++) {
                children.add(start(i, group, ports));
            }
            int first = leader(children);
            var original = children.get(first);
            assertTrue(original.request("WRITE " + value("initial")).endsWith(" 1"));
            String oldId = original.request("BUFFER " + value("stale")).split(" ")[1];
            original.signal("STOP");
            paused = original;
            // Keeps the original process and all local files, only allowing it to be suspended; observation timeout cannot be considered dead when the logic is found to exclude the pauser.
            int second = leader(children, first);
            String winner = value("new-leader");
            assertTrue(children.get(second).request("WRITE " + winner).endsWith(" 2"));
            original.signal("CONT");
            paused = null;
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            long applied = 0;
            while (System.nanoTime() < until) {
                String status = original.request("STATUS");
                applied = Long.parseLong(status.split(" ")[2]);
                if (applied >= 2 && status.split(" ")[1].equals("0")) {
                    break;
                }
                TimeUnit.MILLISECONDS.sleep(20);
            }
            Assertions.assertEquals(2, applied);
            String decision = original.request("FINISH");
            assertTrue(decision.startsWith("ERROR "), decision);
            Assertions.assertEquals("VALUES " + winner + " " + winner, children.get(second).request("READ"));
            Assertions.assertEquals("LOOKUP NOT_CONFIRMED", children.get(second).request("LOOKUP " + oldId));
            Assertions.assertEquals("2", children.get(second).request("STATUS").split(" ")[2]);
        } finally {
            try {
                if (paused != null && paused.process.isAlive()) {
                    paused.signal("CONT");
                }
            } finally {
                finish(children, "paused-old-leader");
            }
        }
    }

    /**
     * At the same time, three ports are maintained until distribution ends, reducing the probability that internal ports will be duplicated in this test.
     *
     * @return the operation result
     * @throws Exception if the operation cannot complete
     */
    private static List<Integer> ports() throws Exception {
        var result = new ArrayList<Integer>();
        var held = new ArrayList<java.net.ServerSocket>();
        try {
            for (int i = 0; i < 3; i++) {
                var socket = new java.net.ServerSocket(0);
                held.add(socket);
                result.add(socket.getLocalPort());
            }
        } finally {
            for (var socket : held) {
                socket.close();
            }
        }
        return result;
    }

    /**
     * The result payload is a fixed encoding, which does not consider the sending of INTENT or commands completed as a successful confirmation.
     *
     * @param text text
     * @return the operation result
     */
    private static String value(final String text) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(text.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Every effort is made to clean up all self-owned processes and maintain logs by a visible catalogue of evidence, failing which the site can be reviewed.
     *
     * @param children children
     * @param name name
     * @throws Exception if the operation cannot complete
     */
    private void finish(final List<Child> children, final String name) throws Exception {
        Throwable failure = null;
        try {
            for (var child : children) {
                try {
                    child.close();
                } catch (final Throwable error) {
                    if (failure == null) {
                        failure = error;
                    } else {
                        failure.addSuppressed(error);
                    }
                }
            }
        } finally {
            String configured = System.getProperty("azir.wal.ha.processEvidenceDir");
            if (configured != null) {
                Path target = Path.of(configured).toAbsolutePath().resolve(name + "-" + UUID.randomUUID());
                Files.createDirectories(target);
                try (var paths = Files.walk(directory)) {
                    for (var path : paths.filter(Files::isRegularFile).toList()) {
                        if (!path.getFileName().toString().endsWith(".log") && !path.getFileName().toString().endsWith(".txt")) {
                            continue;
                        }
                        Path output = target.resolve(directory.relativize(path));
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

    /**
     * Sub JVM maximum pile 128 MiB, one processor, all directories and standard streams only belong to this test.
     *
     * @param index index
     * @param group group
     * @param ports ports
     * @return the operation result
     * @throws AssertionError if the operation cannot complete
     * @throws Exception if the operation cannot complete
     */
    private Child start(final int index, final UUID group, final List<Integer> ports) throws Exception {
        Path root = directory.resolve("p" + index);
        Files.createDirectories(root);
        String cp = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        var args = new ArrayList<>(List.of(Path.of(System.getProperty("java.home"), "bin", "java").toString(), "-Xmx128m", "-XX:ActiveProcessorCount=1", "-cp", cp,
                RatisWalProcessProbe.class.getName(), root.toString(), "p" + index, group.toString()));
        for (int port : ports) {
            args.add(Integer.toString(port));
        }
        var process = new ProcessBuilder(args).redirectError(root.resolve("process-" + System.nanoTime() + ".log").toFile()).start();
        var child = new Child(process, root.resolve("commands-" + process.pid() + ".txt"));
        String started = child.responses.poll(30, TimeUnit.SECONDS);
        if (started == null || !started.startsWith("STARTED ")) {
            child.close();
            throw new AssertionError("probe did not start: " + started);
        }
        Assertions.assertEquals(Long.toString(process.pid()), started.split(" ")[1]);
        child.record(started);
        return child;
    }

    /**
     * Check Processhandle's survival and prove that a candidate can serve with an actual majority barrier.
     *
     * @param children children
     * @return the operation result
     * @throws Exception if the operation cannot complete
     */
    private static int leader(final List<Child> children) throws Exception {
        return leader(children, -1);
    }

    /**
     * The nodes remain alive, with clear exclusions pending the selection of actual serviceable leaders by the remaining majority.
     *
     * @param children children
     * @param excluded excluded
     * @return the operation result
     * @throws AssertionError if the operation cannot complete
     * @throws Exception if the operation cannot complete
     */
    private static int leader(final List<Child> children, final int excluded) throws Exception {
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (System.nanoTime() < until) {
            for (int i = 0; i < children.size(); i++) {
                if (i == excluded) {
                    continue;
                }
                var child = children.get(i);
                if (!child.process.isAlive()) {
                    continue;
                }
                String status = child.request("STATUS");
                if (status.startsWith("STATUS ") && Long.parseLong(status.split(" ")[1]) > 0 && child.request("PING").startsWith("PING ")) {
                    return i;
                }
            }
            TimeUnit.MILLISECONDS.sleep(50);
        }
        throw new AssertionError("no process with ready majority barrier");
    }

    /**
     * Each process has an bounded result queue, and observation timeout does not itself trigger a restart.
     */
    private static final class Child implements AutoCloseable {

        final Process process;

        final java.io.BufferedWriter commands;

        final java.util.concurrent.BlockingQueue<String> responses = new java.util.concurrent.ArrayBlockingQueue<>(64);

        final Thread reader;

        final Path history;

        /**
         * Continuously read from stdout, the library log is stored in a single stderr file.
         *
         * @param process process
         * @param history history
         */
        Child(final Process process, final Path history) {
            this.process = process;
            this.history = history;
            commands = new java.io.BufferedWriter(new java.io.OutputStreamWriter(process.getOutputStream(), StandardCharsets.UTF_8));
            reader = Thread.ofPlatform().daemon().name("wal-probe-output-" + process.pid()).start(() -> {
                try (var input = new java.io.BufferedReader(new java.io.InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                    for (String line; (line = input.readLine()) != null;) {
                        if (line.startsWith("AZIR ")) {
                            responses.put(line.substring(5));
                        }
                    }
                } catch (final Exception failure) {
                    responses.offer("READER_ENDED");
                }
            });
        }

        /**
         * Same process to send scheduling commands, INTENT is not a confirmation of success.
         *
         * @param command command
         * @return the operation result
         * @throws AssertionError if the operation cannot complete
         * @throws Exception if the operation cannot complete
         */
        synchronized String request(final String command) throws Exception {
            if (!process.isAlive()) {
                throw new AssertionError("probe process already terminal");
            }
            record("SEND " + command);
            commands.write(command);
            commands.newLine();
            commands.flush();
            for (String response; (response = responses.poll(10, TimeUnit.SECONDS)) != null;) {
                record("RECV " + response);
                if (!response.startsWith("INTENT ")) {
                    return response;
                }
            }
            throw new AssertionError("probe observation timed out while live=" + process.isAlive());
        }

        /**
         * Only the results actually received by the parent process are recorded and used to independently check the successful prefix and failure order.
         *
         * @param line line
         * @throws Exception if the operation cannot complete
         */
        private void record(final String line) throws Exception {
            Files.writeString(history, System.nanoTime() + "\t" + line + "\n", StandardCharsets.UTF_8, java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.APPEND);
        }

        /**
         * Kill explicitly held test subprocesses and confirm terminal state, which cannot be terminated by fuzzy process name batch.
         *
         * @throws Exception if the operation cannot complete
         */
        void kill() throws Exception {
            record("KILL " + process.pid());
            process.destroyForcibly();
            assertTrue(process.waitFor(10, TimeUnit.SECONDS));
            Assertions.assertNotEquals(0, process.exitValue());
        }

        /**
         * Linux's suspension and recovery are self-owned Processehandle, which only serves to survive; the moratorium is not considered as a process exit.
         *
         * @param signal signal
         * @throws Exception if the operation cannot complete
         * @throws IllegalStateException if the operation cannot complete
         */
        void signal(final String signal) throws Exception {
            if ((!signal.equals("STOP") && !signal.equals("CONT")) || !process.isAlive()) {
                throw new IllegalStateException("invalid owned process signal");
            }
            record("SIGNAL " + signal + " " + process.pid());
            var command = new ProcessBuilder("/bin/kill", "-" + signal, Long.toString(process.pid())).start();
            assertTrue(command.waitFor(5, TimeUnit.SECONDS));
            Assertions.assertEquals(0, command.exitValue());
        }

        /**
         * The cleanup only stops the process that this test explicitly creates and does not search for or terminate other Java jobs of the user.
         *
         * @throws Exception if the operation cannot complete
         */
        @Override
        public void close() throws Exception {
            if (process.isAlive()) {
                try {
                    commands.write("STOP");
                    commands.newLine();
                    commands.flush();
                } catch (final Exception ignored) {
                }
                if (!process.waitFor(15, TimeUnit.SECONDS)) {
                    process.destroyForcibly();
                    assertTrue(process.waitFor(10, TimeUnit.SECONDS));
                }
            }
            commands.close();
            reader.join(2000);
        }
    }
}
