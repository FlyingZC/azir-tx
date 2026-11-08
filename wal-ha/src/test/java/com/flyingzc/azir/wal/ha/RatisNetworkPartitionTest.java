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

import com.github.dockerjava.api.model.Bind;
import com.github.dockerjava.api.model.Capability;
import com.github.dockerjava.api.model.ContainerNetwork;
import com.github.dockerjava.api.model.Frame;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.Network;
import com.github.dockerjava.api.model.StreamType;
import com.github.dockerjava.api.model.Volume;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Assertions;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.async.ResultCallback;

import com.github.dockerjava.api.model.AccessMode;
import org.testcontainers.DockerClientFactory;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;

/**
 * Three stand-alone containers JVM have a real kernel break-up and control channels are independent of the cut off authentication data network.
 */
class RatisNetworkPartitionTest {

    @TempDir
    Path directory;

    /**
     * Covering the old dominant minority, after full division, and killing in failure and snapshot recovery, without posing as a grid-breaker.
     *
     * @param scenario scenario
     * @throws Exception if the operation cannot complete
     */
    @ParameterizedTest
    @ValueSource(strings = {"OLD_LEADER", "ALL_SPLIT", "SNAPSHOT_REKILL"})
    void authenticatedKernelPartitionPreservesConfirmedHistory(final String scenario) throws Exception {
        try (var cluster = new Cluster(directory, scenario)) {
            int old = cluster.leader(Set.of());
            cluster.commit(old, "INIT");
            cluster.awaitAll();
            if (scenario.equals("OLD_LEADER")) {
                String buffered = cluster.nodes.get(old).request("BUFFER");
                assertTrue(buffered.startsWith("BUFFERED "));
                String uncertain = buffered.split(" ")[1];
                cluster.cut(old);
                assertTrue(cluster.nodes.get(old).request("FINISH").startsWith("ERROR "));
                cluster.rejectFresh(old);
                int current = cluster.leader(Set.of(old));
                cluster.commit(current, "LIVE 1");
                cluster.verify(current);
                cluster.heal(old);
                cluster.awaitAll();
                Assertions.assertEquals("LOOKUP NOT_CONFIRMED", cluster.nodes.get(current).request("LOOKUP " + uncertain));
                cluster.commit(current, "LIVE 2");
                cluster.verify(current);
            } else if (scenario.equals("ALL_SPLIT")) {
                for (int i = 0; i < 3; i++) {
                    cluster.cut(i);
                }
                for (int i = 0; i < 3; i++) {
                    cluster.rejectFresh(i);
                }
                int isolated = (old + 2) % 3;
                for (int i = 0; i < 3; i++) {
                    if (i != isolated) {
                        cluster.heal(i);
                    }
                }
                int current = cluster.leader(Set.of(isolated));
                cluster.commit(current, "LIVE 1");
                cluster.verify(current);
                int victim = 3 - current - isolated;
                cluster.nodes.get(victim).kill("NO_QUORUM_REKILL");
                cluster.rejectFresh(current);
                cluster.replace(victim, "-");
                current = cluster.leader(Set.of(isolated));
                cluster.commit(current, "LIVE 2");
                cluster.verify(current);
                cluster.heal(isolated);
                cluster.awaitAll();
                cluster.commit(current, "LIVE 3");
                cluster.verify(current);
            } else {
                cluster.cut(old);
                int current = cluster.leader(Set.of(old));
                for (int i = 1; i <= 10; i++) {
                    cluster.commit(current, "WRITE " + i);
                }
                cluster.commit(current, "DELETE");
                long snapshot = Long.parseLong(cluster.nodes.get(current).request("SNAPSHOT").split(" ")[1]);
                assertTrue(Long.parseLong(cluster.nodes.get(current).request("PURGE " + snapshot).split(" ")[1]) > 2);
                cluster.nodes.get(old).kill("ISOLATED_OLD_LEADER");
                cluster.replace(old, "AFTER_INSTALL_RECORD");
                cluster.nodes.get(old).boundary();
                cluster.nodes.get(old).kill("AFTER_INSTALL_RECORD");
                cluster.commit(current, "LIVE 1");
                cluster.replace(old, "-");
                cluster.awaitAll();
                String[] status = cluster.nodes.get(old).request("STATUS").split(" ");
                assertTrue(Long.parseLong(status[4]) >= snapshot);
                // The ACK observation count after the process has been restarted starts empty, durable snapshot and the application watermark is based on recovery.
                assertTrue(RatisGenerationRecord.read(cluster.root.resolve("p" + old + "/business/ACTIVE")).index() >= snapshot);
                Assertions.assertEquals("TRANSFERRED", cluster.nodes.get(current).request("TRANSFER p" + old));
                cluster.awaitLeader(old);
                cluster.verify(old);
                cluster.commit(old, "LIVE 2");
                cluster.verify(old);
            }
            cluster.history(-1, "VERIFIED", scenario + " receipts=" + cluster.receipts.size());
            Files.writeString(cluster.root.resolve("case.json"),
                    "{\"scenario\":\"" + scenario + "\",\"group\":\"" + cluster.group + "\",\"sequence\":" + cluster.sequence + ",\"expectedDigest\":\"" + cluster.digest() + "\"}\n");
        }
    }

    /**
     * Only save complete commit receipt actually received by the parent process.
     *
     * @param id id
     * @param sequence sequence
     * @param digest digest
     */
    private record Receipt(String id, long sequence, String digest) {
    }

    /**
     * A set of scenes monopolize their own networks, containers, receipt and local replica directories.
     */
    private static final class Cluster implements AutoCloseable {

        final DockerClient docker = DockerClientFactory.instance().client();

        final Path root;

        final UUID group = UUID.randomUUID();

        final String network, image;

        final List<String> addresses = new ArrayList<>();

        final List<Child> nodes = new ArrayList<>();

        final List<RatisTlsTestCertificates.Entry> credentials = new ArrayList<>();

        final List<Receipt> receipts = new ArrayList<>();

        final List<Bind> classpathBinds = new ArrayList<>();

        final List<String> createdContainers = new ArrayList<>();

        final Path policy, roots;

        final String classpath;

        long sequence;

        int blob, live;

        boolean deleted;

        /**
         * Automatically allocates the exclusive Bridge segment, only connection for three non-privileged packagings created by this test.
         *
         * @param temporary temporary
         * @param scenario scenario
         * @throws Exception if the operation cannot complete
         */
        Cluster(final Path temporary, final String scenario) throws Exception {
            String configured = System.getProperty("azir.wal.ha.networkEvidenceDir");
            root = configured == null ? temporary : Path.of(configured).resolve(scenario + "-" + group);
            Files.createDirectories(root);
            image = docker.inspectImageCmd("eclipse-temurin:21-jdk").exec().getId();
            history(-1, "IMAGE", image);
            network = allocateNetwork();
            history(-1, "NETWORK_CREATED", network);
            try {
                var subnet = docker.inspectNetworkCmd().withNetworkId(network).exec().getIpam().getConfig().getFirst().getSubnet();
                byte[] prefix = java.net.InetAddress.getByName(subnet.split("/")[0]).getAddress();
                Assertions.assertEquals(4, prefix.length);
                // Synthetic keys exist only in the Junit private temporary directory, and the durable evidence catalogue retains only public strategies and results.
                var pki = new RatisTlsTestCertificates(temporary.resolve("pki"));
                roots = pki.trust;
                policy = root.resolve("policy.properties");
                var values = new Properties();
                for (int i = 0; i < 3; i++) {
                    byte[] ip = prefix.clone();
                    ip[3] = (byte) (10 + i);
                    String address = java.net.InetAddress.getByAddress(ip).getHostAddress() + ":19001";
                    addresses.add(address);
                    var identity = pki.issue(group, "p" + i, address);
                    credentials.add(identity);
                    nodes.add(null);
                    values.setProperty("p" + i + ".address", address);
                    values.setProperty("p" + i + ".pin", identity.trust().certificateSha256().iterator().next());
                }
                try (var output = Files.newOutputStream(policy)) {
                    values.store(output, "owned ephemeral TLS peer policy");
                }
                classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
                for (String entry : classpath.split(java.io.File.pathSeparator)) {
                    Path file = Path.of(entry).toAbsolutePath().normalize();
                    assertTrue(Files.exists(file));
                    classpathBinds.add(new Bind(file.toString(), new Volume(file.toString()), AccessMode.ro));
                }
                for (int i = 0; i < 3; i++) {
                    replace(i, "-");
                }
                for (var child : nodes) {
                    child.started();
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
         * The event uses a unified parent process monotonic clock, and the control result is maintained with Docker observation.
         *
         * @param node node
         * @param kind kind
         * @param text text
         * @throws IOException if the operation cannot complete
         */
        synchronized void history(final int node, final String kind, final String text) throws IOException {
            Files.writeString(root.resolve("events.tsv"), System.nanoTime() + "\t" + node + "\t" + kind + "\t" + text + "\n", StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        }

        /**
         * Only Docker can allocate non-conflicted segments to a temporary network of its own, creating an exclusive network that supports fixed addresses.
         *
         * @return the operation result
         * @throws Exception if the operation cannot complete
         */
        private String allocateNetwork() throws Exception {
            String reservation =
                    docker.createNetworkCmd().withName("azir-pool-" + group).withDriver("bridge").withInternal(true).withLabels(Map.of("azir.network.test", group.toString())).exec().getId();
            history(-1, "POOL_CREATED", reservation);
            String subnet;
            try {
                subnet = docker.inspectNetworkCmd().withNetworkId(reservation).exec().getIpam().getConfig().getFirst().getSubnet();
            } finally {
                docker.removeNetworkCmd(reservation).exec();
                history(-1, "POOL_REMOVED", reservation);
            }
            return docker.createNetworkCmd().withName("azir-net-" + group).withDriver("bridge").withInternal(true).withLabels(Map.of("azir.network.test", group.toString()))
                    .withIpam(new Network.Ipam().withConfig(new Network.Ipam.Config().withSubnet(subnet))).exec().getId();
        }

        /**
         * Only the old container has been terminated will it be allowed to rebuild and re-use the real voting and business catalogues.
         *
         * @param index index
         * @param boundary boundary
         * @throws Exception if the operation cannot complete
         */
        void replace(final int index, final String boundary) throws Exception {
            var previous = nodes.get(index);
            if (previous != null) {
                assertFalse(docker.inspectContainerCmd(previous.id).exec().getState().getRunning(), "cannot restart a live observation timeout");
                previous.close();
            }
            Path data = root.resolve("p" + index);
            Files.createDirectories(data);
            var entry = credentials.get(index);
            var binds = new ArrayList<>(classpathBinds);
            for (Path path : List.of(policy, roots, entry.identity())) {
                binds.add(new Bind(path.toAbsolutePath().toString(), new Volume(path.toAbsolutePath().toString()), AccessMode.ro));
            }
            binds.add(new Bind(data.toAbsolutePath().toString(), new Volume(data.toAbsolutePath().toString()), AccessMode.rw));
            var host = HostConfig.newHostConfig().withNetworkMode(network).withBinds(binds).withNanoCPUs(500_000_000L).withMemory(512L * 1024 * 1024).withPidsLimit(256L).withReadonlyRootfs(true)
                    .withCapDrop(Capability.ALL).withSecurityOpts(List.of("no-new-privileges")).withTmpFs(Map.of("/tmp", "rw,nosuid,noexec,size=32m"));
            String user = Files.getAttribute(data, "unix:uid") + ":" + Files.getAttribute(data, "unix:gid");
            var command = List.of("java", "-Xmx384m", "-XX:ActiveProcessorCount=2", "-cp", classpath, RatisNetworkProcessProbe.class.getName(), data.toAbsolutePath().toString(), "p" + index,
                    group.toString(), policy.toAbsolutePath().toString(), entry.identity().toAbsolutePath().toString(), roots.toAbsolutePath().toString(), boundary);
            String id = docker.createContainerCmd(image).withName("azir-jvm-" + group + "-" + index + "-" + UUID.randomUUID().toString().substring(0, 8)).withHostConfig(host).withUser(user)
                    .withIpv4Address(addresses.get(index).split(":")[0]).withLabels(Map.of("azir.network.test", group.toString())).withStdinOpen(true).withAttachStdin(true).withAttachStdout(true)
                    .withAttachStderr(true).withCmd(command).exec().getId();
            createdContainers.add(id);
            history(index, "CONTAINER_CREATED", id);
            docker.startContainerCmd(id).exec();
            var child = new Child(this, index, id);
            nodes.set(index, child);
            var state = docker.inspectContainerCmd(id).exec();
            Assertions.assertEquals(500_000_000L, state.getHostConfig().getNanoCPUs());
            Assertions.assertEquals(512L * 1024 * 1024, state.getHostConfig().getMemory());
            assertTrue(state.getState().getRunning());
            history(index, "JVM_RUNNING", "container=" + id + " hostPid=" + state.getState().getPidLong());
            history(index, "ATTACHED", "attached=true running=true container=" + id);
        }

        /**
         * The actual removal of an exclusive data network card confirms that JVM is still alive and the original network no longer exists.
         *
         * @param index index
         * @throws Exception if the operation cannot complete
         */
        void cut(final int index) throws Exception {
            docker.disconnectFromNetworkCmd().withContainerId(nodes.get(index).id).withNetworkId(network).withForce(true).exec();
            topology(index, false);
        }

        /**
         * Recovery The original static address, where the process is directly reconnected, does not replace network healing with a restart.
         *
         * @param index index
         * @throws Exception if the operation cannot complete
         */
        void heal(final int index) throws Exception {
            docker.connectToNetworkCmd().withContainerId(nodes.get(index).id).withNetworkId(network)
                    .withContainerNetwork(new ContainerNetwork().withIpamConfig(new ContainerNetwork.Ipam().withIpv4Address(addresses.get(index).split(":")[0]))).exec();
            topology(index, true);
        }

        /**
         * Records the real connection and process state from Docker, which cannot be successfully returned with an injection command alone.
         *
         * @param index index
         * @param expected expected
         * @throws Exception if the operation cannot complete
         */
        void topology(final int index, final boolean expected) throws Exception {
            var state = docker.inspectContainerCmd(nodes.get(index).id).exec();
            boolean attached = state.getNetworkSettings().getNetworks().values().stream().anyMatch(value -> network.equals(value.getNetworkID()));
            Assertions.assertEquals(expected, attached);
            assertTrue(state.getState().getRunning());
            history(index, expected ? "HEALED" : "PARTITIONED", "attached=" + attached + " running=true container=" + nodes.get(index).id);
        }

        /**
         * All new transaction and new snapshot queries must rely on the majority, and isolated nodes cannot retreat from cache success.
         *
         * @param index index
         * @throws Exception if the operation cannot complete
         */
        void rejectFresh(final int index) throws Exception {
            for (String command : List.of("LIVE 998", "READ", "PING")) {
                assertTrue(nodes.get(index).request(command).startsWith("ERROR "));
            }
        }

        /**
         * When the role is discovered, the real read barrier will be executed and the old master cache role will not be considered to still have a majority.
         *
         * @param excluded excluded
         * @return the operation result
         * @throws AssertionError if the operation cannot complete
         * @throws Exception if the operation cannot complete
         */
        int leader(final Set<Integer> excluded) throws Exception {
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
            while (System.nanoTime() < until) {
                for (int i = 0; i < 3; i++) {
                    if (excluded.contains(i) || nodes.get(i) == null || !nodes.get(i).running()) {
                        continue;
                    }
                    String status = nodes.get(i).request("STATUS");
                    if (status.startsWith("STATUS ") && Long.parseLong(status.split(" ")[1]) > 0 && nodes.get(i).request("PING").startsWith("PING ")) {
                        return i;
                    }
                }
                TimeUnit.MILLISECONDS.sleep(50);
            }
            throw new AssertionError("no authenticated majority leader");
        }

        /**
         * The target is fixed until its role and read barrier are really established.
         *
         * @param index index
         * @throws AssertionError if the operation cannot complete
         * @throws Exception if the operation cannot complete
         */
        void awaitLeader(final int index) throws Exception {
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
            while (System.nanoTime() < until) {
                String status = nodes.get(index).request("STATUS");
                if (status.startsWith("STATUS ") && Long.parseLong(status.split(" ")[1]) > 0 && nodes.get(index).request("PING").startsWith("PING ")) {
                    return;
                }
                TimeUnit.MILLISECONDS.sleep(50);
            }
            throw new AssertionError("target leader not serving");
        }

        /**
         * The end of recovery is declared after the practical application of the business prefix for three nodes.
         *
         * @throws AssertionError if the operation cannot complete
         * @throws Exception if the operation cannot complete
         */
        void awaitAll() throws Exception {
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
            while (System.nanoTime() < until) {
                boolean ready = true;
                for (var child : nodes) {
                    String status = child.request("STATUS");
                    ready &= status.startsWith("STATUS ") && Long.parseLong(status.split(" ")[2]) == sequence && status.endsWith(" true");
                }
                if (ready) {
                    return;
                }
                TimeUnit.MILLISECONDS.sleep(50);
            }
            throw new AssertionError("authenticated replicas did not catch up");
        }

        /**
         * Successful responses require a continuous commit serial number and complete UUID, summary to update the independent expectation model.
         *
         * @param index index
         * @param command command
         * @throws Exception if the operation cannot complete
         */
        void commit(final int index, final String command) throws Exception {
            String response = nodes.get(index).request(command);
            assertTrue(response.startsWith("COMMITTED "), response);
            String[] fields = response.split(" ");
            UUID.fromString(fields[1]);
            Assertions.assertEquals(++sequence, Long.parseLong(fields[2]));
            assertTrue(fields[3].matches("[a-f0-9]{64}"));
            receipts.add(new Receipt(fields[1], sequence, fields[3]));
            if (command.startsWith("WRITE ")) {
                blob = Integer.parseInt(command.split(" ")[1]);
            }
            if (command.startsWith("LIVE ")) {
                live = Integer.parseInt(command.split(" ")[1]);
            }
            if (command.equals("DELETE")) {
                deleted = true;
            }
        }

        /**
         * The external complete data summary and each confirmation receipt are reconciled and cannot be viewed only as a sum of the balances.
         *
         * @param index index
         * @throws Exception if the operation cannot complete
         */
        void verify(final int index) throws Exception {
            Assertions.assertEquals("DATA " + sequence + " " + digest(), nodes.get(index).request("READ"));
            for (var receipt : receipts) {
                Assertions.assertEquals("LOOKUP COMMITTED " + receipt.sequence() + " " + receipt.digest(), nodes.get(index).request("LOOKUP " + receipt.id()));
            }
        }

        /**
         * The parent process independently rebuilds the expected content of 68 keys, clearly distinguishing between tombstone and empty values.
         *
         * @return the operation result
         * @throws Exception if the operation cannot complete
         */
        String digest() throws Exception {
            var hash = java.security.MessageDigest.getInstance("SHA-256");
            for (String shard : List.of("a", "b")) {
                for (int i = 0; i < 32; i++) {
                    entry(hash, shard + "/cold" + i, deleted && i == 0 ? null : (shard + ":" + i).getBytes(StandardCharsets.UTF_8));
                }
                byte[] value = blob == 0 ? new byte[]{0} : new byte[480 * 1024];
                if (blob != 0) {
                    Arrays.fill(value, (byte) (blob + (shard.equals("a") ? 0 : 100)));
                }
                entry(hash, shard + "/blob", value);
                entry(hash, shard + "/live", Integer.toString(shard.equals("a") ? live : -live).getBytes(StandardCharsets.UTF_8));
            }
            return HexFormat.of().formatHex(hash.digest());
        }

        /**
         * Length and tombstone encoding are generated by the parent process to avoid ambiguity.
         *
         * @param hash hash
         * @param key business key
         * @param value value to write
         */
        private static void entry(final java.security.MessageDigest hash, final String key, final byte[] value) {
            byte[] name = key.getBytes(StandardCharsets.UTF_8);
            hash.update(java.nio.ByteBuffer.allocate(4).putInt(name.length).array());
            hash.update(name);
            hash.update(java.nio.ByteBuffer.allocate(4).putInt(value == null ? -1 : value.length).array());
            if (value != null) {
                hash.update(value);
            }
        }

        /**
         * Only containers and networks that were explicitly created by this test are cleared, and the rest of the resources cannot be bypassed.
         *
         * @throws Exception if the operation cannot complete
         */
        @Override
        public void close() throws Exception {
            Exception failure = null;
            for (var child : nodes) {
                if (child != null) {
                    try {
                        child.close();
                    } catch (final Exception close) {
                        if (failure == null) {
                            failure = close;
                        } else {
                            failure.addSuppressed(close);
                        }
                    }
                }
            }
            for (String id : createdContainers) {
                try {
                    var state = docker.inspectContainerCmd(id).exec();
                    if (Boolean.TRUE.equals(state.getState().getRunning())) {
                        history(-1, "CLEANUP_FORCED_KILL", id);
                        docker.stopContainerCmd(id).withTimeout(10).exec();
                    }
                    docker.removeContainerCmd(id).exec();
                    history(-1, "CONTAINER_REMOVED", id);
                } catch (final com.github.dockerjava.api.exception.NotFoundException removed) {
                    /*
                     * Process has been closed by normal sub-process.
                     */ } catch (final Exception close) {
                    if (failure == null) {
                        failure = close;
                    } else {
                        failure.addSuppressed(close);
                    }
                }
            }
            try {
                docker.removeNetworkCmd(network).exec();
                history(-1, "NETWORK_REMOVED", network);
            } catch (final Exception close) {
                if (failure == null) {
                    failure = close;
                } else {
                    failure.addSuppressed(close);
                }
            }
            if (failure != null) {
                throw failure;
            }
        }
    }

    /**
     * The container stdin with stdout is attached to a channel that does not pass the test data net card and can still ask for actual nodes when split.
     */
    private static final class Child implements AutoCloseable {

        final Cluster cluster;

        final int index;

        final String id;

        final BufferedWriter commands;

        final ResultCallback.Adapter<Frame> attached;

        final BlockingQueue<String> replies = new ArrayBlockingQueue<>(64), events = new ArrayBlockingQueue<>(16);

        final StringBuilder partial = new StringBuilder();

        boolean removed;

        /**
         * The historical log of the new container is only the current JVM and the old packaging log is not used to falsify a startup success.
         *
         * @param cluster cluster
         * @param index index
         * @param id id
         * @throws Exception if the operation cannot complete
         */
        Child(final Cluster cluster, final int index, final String id) throws Exception {
            this.cluster = cluster;
            this.index = index;
            this.id = id;
            var input = new PipedInputStream(8192);
            commands = new BufferedWriter(new OutputStreamWriter(new PipedOutputStream(input), StandardCharsets.UTF_8));
            attached = new ResultCallback.Adapter<>() {

                /**
                 * Callback splits by real frame, stdout queue and stderr file each bounded.
                 *
                 * @param frame frame
                 */
                @Override
                public void onNext(final Frame frame) {
                    try {
                        if (frame.getStreamType() == StreamType.STDOUT) {
                            consume(frame.getPayload());
                        } else if (frame.getStreamType() == StreamType.STDERR) {
                            Path log = cluster.root.resolve("stderr-" + index + "-" + id.substring(0, 12) + ".log");
                            if (!Files.exists(log) || Files.size(log) < 32L * 1024 * 1024) {
                                Files.write(log, frame.getPayload(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                            }
                        }
                    } catch (final Exception failure) {
                        replies.offer("READER_FAILED");
                    }
                }
            };
            cluster.docker.attachContainerCmd(id).withStdIn(input).withStdOut(true).withStdErr(true).withFollowStream(true).withLogs(true).exec(attached);
            try {
                String boot = events.poll(30, TimeUnit.SECONDS);
                assertNotNull(boot, "container BOOT missing");
                assertTrue(boot.startsWith("BOOT "), boot);
            } catch (final Throwable failure) {
                attached.close();
                commands.close();
                throw failure;
            }
        }

        /**
         * Accepts only the full AZIR event line and does not use the native log as business confirmation.
         *
         * @param payload payload
         * @throws Exception if the operation cannot complete
         * @throws IOException if the operation cannot complete
         */
        synchronized void consume(final byte[] payload) throws Exception {
            partial.append(new String(payload, StandardCharsets.UTF_8));
            if (partial.length() > 65536) {
                throw new IOException("probe line budget exceeded");
            }
            int newline;
            while ((newline = partial.indexOf("\n")) >= 0) {
                String line = partial.substring(0, newline).stripTrailing();
                partial.delete(0, newline + 1);
                if (!line.startsWith("AZIR ")) {
                    continue;
                }
                String value = line.substring(5);
                cluster.history(index, "RECV", value);
                var queue = value.startsWith("BOOT ") || value.startsWith("STARTED ") || value.startsWith("BOUNDARY ") ? events : replies;
                if (!queue.offer(value)) {
                    throw new IOException("probe output queue exceeded");
                }
            }
        }

        /**
         * Regular assembly waits for START; recovery border killings are allowed before service is ready.
         *
         * @throws Exception if the operation cannot complete
         */
        void started() throws Exception {
            String event = events.poll(90, TimeUnit.SECONDS);
            assertNotNull(event);
            assertTrue(event.startsWith("STARTED "), event);
        }

        /**
         * Freezing occurs at the real durable installation boundary, without fixing the location of the installation.
         *
         * @throws AssertionError if the operation cannot complete
         * @throws Exception if the operation cannot complete
         */
        void boundary() throws Exception {
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
            while (System.nanoTime() < until) {
                String event = events.poll(remaining(until), TimeUnit.MILLISECONDS);
                if (event != null && event.startsWith("STARTED ")) {
                    continue;
                }
                Assertions.assertEquals("BOUNDARY AFTER_INSTALL_RECORD", event);
                assertTrue(running());
                return;
            }
            throw new AssertionError("installation boundary observation expired");
        }

        /**
         * The command result timeout only reports failure to observe and does not automatically kill or rebuild nodes.
         *
         * @param command command
         * @return the operation result
         * @throws AssertionError if the operation cannot complete
         * @throws Exception if the operation cannot complete
         */
        String request(final String command) throws Exception {
            synchronized (commands) {
                assertTrue(running(), "owned JVM already terminal");
                cluster.history(index, "SEND", command);
                commands.write(command);
                commands.newLine();
                commands.flush();
                long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(45);
                while (System.nanoTime() < until) {
                    String reply = replies.poll(remaining(until), TimeUnit.MILLISECONDS);
                    if (reply != null && reply.startsWith("INTENT ")) {
                        continue;
                    }
                    if (reply != null) {
                        return reply;
                    }
                }
                throw new AssertionError("command observation expired; container still running=" + running());
            }
        }

        /**
         * The Docker state is the de facto source of survival for the process.
         *
         * @return the operation result
         */
        boolean running() {
            return !removed && Boolean.TRUE.equals(cluster.docker.inspectContainerCmd(id).exec().getState().getRunning());
        }

        /**
         * Only recorded self-contained containers that are confirmed as not zero terminal state will be allowed to call until they have been forcibly killed.
         *
         * @param boundary boundary
         * @throws Exception if the operation cannot complete
         */
        void kill(final String boundary) throws Exception {
            cluster.history(index, "KILL", boundary);
            cluster.docker.killContainerCmd(id).withSignal("KILL").exec();
            awaitExit();
            var state = cluster.docker.inspectContainerCmd(id).exec().getState();
            Assertions.assertNotEquals(0, state.getExitCodeLong());
            cluster.history(index, "EXIT", Long.toString(state.getExitCodeLong()));
        }

        /**
         * Wait for a definite terminal state; an observation timeout is not proof of termination.
         *
         * @throws Exception if the operation cannot complete
         */
        void awaitExit() throws Exception {
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (running() && System.nanoTime() < until) {
                TimeUnit.MILLISECONDS.sleep(30);
            }
            assertFalse(running(), "owned container did not exit");
        }

        /**
         * Normal cleanup first STOP, failed mandatory clean-up is a separate record and cannot be impersonated as an failure injection.
         *
         * @throws Exception if the operation cannot complete
         */
        @Override
        public void close() throws Exception {
            if (removed) {
                return;
            }
            if (running()) {
                try {
                    request("STOP");
                    awaitExit();
                } catch (final Exception failure) {
                    cluster.history(index, "CLEANUP_FORCED_KILL", id);
                    cluster.docker.killContainerCmd(id).withSignal("KILL").exec();
                    awaitExit();
                }
            }
            attached.close();
            commands.close();
            cluster.docker.removeContainerCmd(id).exec();
            removed = true;
            cluster.history(index, "CONTAINER_REMOVED", id);
        }

        /**
         * Use the same waiting period.
         *
         * @param until until
         * @return the operation result
         */
        private static long remaining(final long until) {
            return Math.max(1, TimeUnit.NANOSECONDS.toMillis(until - System.nanoTime()));
        }
    }
}
