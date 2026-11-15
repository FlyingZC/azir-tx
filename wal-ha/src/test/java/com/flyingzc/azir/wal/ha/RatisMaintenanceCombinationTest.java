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
 * Authentication Raft, independent MySQL, automatic maintenance with real network/backend in transit transaction failure.
 */
class RatisMaintenanceCombinationTest {

    @TempDir
    Path directory;

    static final List<org.testcontainers.containers.MySQLContainer<?>> DATABASES = List.of(MysqlGenerationSupport.database(), MysqlGenerationSupport.database(), MysqlGenerationSupport.database());

    /**
     * Three own database reuse processes, each scenario control library and data space stand alone.
     */
    @org.junit.jupiter.api.BeforeAll
    static void startDatabases() {
        org.testcontainers.lifecycle.Startables.deepStart(DATABASES.stream()).join();
    }

    /**
     * Only cases held in this category are discontinued.
     */
    @org.junit.jupiter.api.AfterAll
    static void stopDatabases() {
        for (var database : DATABASES) {
            database.stop();
        }
    }

    /**
     * Automaintenance, dual network, physical backend pause and co-location verify on transaction.
     *
     * @param scenario scenario
     * @throws Exception if the operation cannot complete
     */
    @ParameterizedTest
    @ValueSource(strings = {"BEFORE_APPEND", "DURABLE_REPLY_LOST", "ALL_SPLIT_REPLY_LOST"})
    void authenticatedMaintenanceBackendAndInflightFaults(final String scenario) throws Exception {
        try (var cluster = new Cluster(directory, scenario)) {
            int old = cluster.leader(Set.of());
            cluster.commit(old, "INIT");
            for (int value = 1; value <= 10; value++) {
                cluster.commit(old, "WRITE " + value);
            }
            cluster.commit(old, "DELETE");
            cluster.awaitAll();
            cluster.coldAll();
            for (var node : cluster.nodes) {
                Assertions.assertEquals("LIFECYCLE true true true", node.request("LIFECYCLE"));
            }
            cluster.maintainedAll();
            String buffered = cluster.nodes.get(old).request("BUFFER");
            assertTrue(buffered.startsWith("BUFFERED "));
            String uncertain = buffered.split(" ")[1];
            String boundary = scenario.equals("BEFORE_APPEND") ? "BEFORE_COMMIT_APPEND" : "AFTER_COMMIT_APPLIED";
            Assertions.assertEquals("ARMED " + boundary, cluster.nodes.get(old).request("ARM " + boundary));
            Assertions.assertEquals("SUBMITTED " + uncertain, cluster.nodes.get(old).request("FINISH_ASYNC"));
            cluster.nodes.get(old).boundary(boundary);
            if (!scenario.equals("BEFORE_APPEND")) {
                cluster.awaitPrefixExcept(old, 13);
            }
            int backend = scenario.equals("DURABLE_REPLY_LOST") ? old : (old + 1) % 3;
            if (scenario.equals("ALL_SPLIT_REPLY_LOST")) {
                for (int i = 0; i < 3; i++) {
                    cluster.cut(i);
                }
            } else {
                cluster.cut(old);
            }
            cluster.pauseBackend(backend);
            cluster.awaitQuarantined(backend);
            String rejected = null;
            int current;
            if (scenario.equals("BEFORE_APPEND")) {
                current = cluster.leader(Set.of(old, backend));
                cluster.commit(current, "LIVE 1");
                Assertions.assertEquals("RELEASED", cluster.nodes.get(old).request("RELEASE"));
                rejected = cluster.result(old);
                assertTrue(rejected.startsWith("ERROR "), rejected);
                cluster.rejectFresh(old);
                cluster.verify(current);
                cluster.resumeBackend(backend);
                cluster.heal(old);
                cluster.awaitAll();
                Assertions.assertEquals("LOOKUP NOT_CONFIRMED", cluster.nodes.get(current).request("LOOKUP " + uncertain));
            } else if (scenario.equals("DURABLE_REPLY_LOST")) {
                cluster.nodes.get(old).kill("DURABLE_CLIENT_REPLY_LOST");
                current = cluster.leader(Set.of(old));
                cluster.resolve(current, uncertain);
                cluster.verify(current);
                cluster.commit(current, "LIVE 1");
                cluster.resumeBackend(backend);
                cluster.replace(old, "-");
                cluster.nodes.get(old).started();
                cluster.awaitAll();
                cluster.verify(current);
            } else {
                Assertions.assertEquals("RELEASED", cluster.nodes.get(old).request("RELEASE"));
                for (int i = 0; i < 3; i++) {
                    cluster.rejectFresh(i);
                }
                for (int i = 0; i < 3; i++) {
                    if (i != old) {
                        cluster.heal(i);
                    }
                }
                current = cluster.leader(Set.of(old, backend));
                cluster.resolve(current, uncertain);
                cluster.verify(current);
                cluster.commit(current, "LIVE 1");
                cluster.verify(current);
                cluster.rejectFresh(old);
                cluster.resumeBackend(backend);
                cluster.heal(old);
                cluster.awaitAll();
            }
            current = cluster.leader(Set.of());
            cluster.verify(current);
            cluster.commit(current, "LIVE 2");
            cluster.verify(current);
            cluster.coldAll();
            cluster.maintainedAll();
            cluster.history(-1, "VERIFIED", scenario + " decisions=" + cluster.receipts.size());
            Files.writeString(cluster.root.resolve("case.json"),
                    "{\"scenario\":\"" + scenario + "\",\"group\":\"" + cluster.group + "\",\"old\":" + old + ",\"backend\":" + backend + ",\"uncertain\":\"" + uncertain + "\",\"resolved\":"
                            + !scenario.equals("BEFORE_APPEND") + ",\"sequence\":" + cluster.sequence + ",\"expectedDigest\":\"" + cluster.digest() + "\"}\n");
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

        final String image;

        String network, databaseNetwork;

        final List<String> addresses = new ArrayList<>(), backendAddresses = new ArrayList<>(), schemas = new ArrayList<>();

        final boolean[] paused = new boolean[3];

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
            String configured = System.getProperty("azir.wal.ha.combinationEvidenceDir");
            root = configured == null ? temporary : Path.of(configured).resolve(scenario + "-" + group);
            Files.createDirectories(root);
            image = docker.inspectImageCmd("eclipse-temurin:21-jdk").exec().getId();
            history(-1, "IMAGE", image);
            try {
                network = allocateNetwork("raft");
                history(-1, "NETWORK_CREATED", network);
                databaseNetwork = allocateNetwork("backend");
                history(-1, "NETWORK_CREATED", databaseNetwork);
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
                byte[] backendPrefix =
                        java.net.InetAddress.getByName(docker.inspectNetworkCmd().withNetworkId(databaseNetwork).exec().getIpam().getConfig().getFirst().getSubnet().split("/")[0]).getAddress();
                for (int i = 0; i < 3; i++) {
                    byte[] ip = backendPrefix.clone();
                    ip[3] = (byte) (20 + i);
                    String address = java.net.InetAddress.getByAddress(ip).getHostAddress();
                    backendAddresses.add(address);
                    docker.connectToNetworkCmd().withContainerId(DATABASES.get(i).getContainerId()).withNetworkId(databaseNetwork)
                            .withContainerNetwork(new ContainerNetwork().withIpamConfig(new ContainerNetwork.Ipam().withIpv4Address(address))).exec();
                    history(i, "BACKEND_ATTACHED", "container=" + DATABASES.get(i).getContainerId() + " network=" + databaseNetwork + " address=" + address);
                    String schema = "c_" + UUID.randomUUID().toString().replace("-", "");
                    MysqlGenerationSupport.createControl(DATABASES.get(i), schema);
                    schemas.add(schema);
                    values.setProperty("p" + i + ".backend", address + ":3306");
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
         * @param tag tag
         * @return the operation result
         * @throws Exception if the operation cannot complete
         */
        private String allocateNetwork(final String tag) throws Exception {
            var allocator = docker.createNetworkCmd().withName("azir-pool-" + group + "-" + tag).withDriver("bridge").withInternal(true).withLabels(Map.of("azir.network.test", group.toString()));
            String requestedSubnet = System.getProperty("azir.wal.ha.combinationSubnet." + tag);
            if (requestedSubnet != null && !requestedSubnet.isBlank()) {
                // An explicit unused subnet avoids exhausting the shared daemon's default pools.
                allocator.withIpam(new Network.Ipam().withConfig(new Network.Ipam.Config().withSubnet(requestedSubnet)));
            }
            String reservation = allocator.exec().getId();
            history(-1, "POOL_CREATED", reservation);
            String subnet;
            try {
                subnet = docker.inspectNetworkCmd().withNetworkId(reservation).exec().getIpam().getConfig().getFirst().getSubnet();
            } finally {
                docker.removeNetworkCmd(reservation).exec();
                history(-1, "POOL_REMOVED", reservation);
            }
            return docker.createNetworkCmd().withName("azir-net-" + group + "-" + tag).withDriver("bridge").withInternal(true).withLabels(Map.of("azir.network.test", group.toString()))
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
            Path ready = data.resolve(".network-ready");
            Files.deleteIfExists(ready);
            var actual = List.of("java", "-Xmx384m", "-XX:ActiveProcessorCount=2", "-cp", classpath, RatisCombinationProcessProbe.class.getName(), data.toAbsolutePath().toString(), "p" + index,
                    group.toString(), policy.toAbsolutePath().toString(), entry.identity().toAbsolutePath().toString(), roots.toAbsolutePath().toString(), boundary);
            var command = new ArrayList<>(List.of("sh", "-c", "while [ ! -f \"$1\" ]; do sleep 0.02; done; shift; exec \"$@\"", "owned-combination-start", ready.toAbsolutePath().toString()));
            command.addAll(actual);
            String id = docker.createContainerCmd(image).withName("azir-jvm-" + group + "-" + index + "-" + UUID.randomUUID().toString().substring(0, 8)).withHostConfig(host).withUser(user)
                    .withIpv4Address(addresses.get(index).split(":")[0]).withLabels(Map.of("azir.network.test", group.toString()))
                    .withEnv("AZIR_WAL_COMBINATION_SCHEMA=" + schemas.get(index), "AZIR_WAL_COMBINATION_PASSWORD=" + DATABASES.get(index).getPassword()).withStdinOpen(true).withAttachStdin(true)
                    .withAttachStdout(true).withAttachStderr(true).withCmd(command).exec().getId();
            createdContainers.add(id);
            history(index, "CONTAINER_CREATED", id);
            docker.startContainerCmd(id).exec();
            docker.connectToNetworkCmd().withContainerId(id).withNetworkId(databaseNetwork).exec();
            history(index, "JVM_BACKEND_ATTACHED", "container=" + id + " network=" + databaseNetwork);
            Files.writeString(ready, "ready", StandardOpenOption.CREATE_NEW);
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
            assertTrue(state.getNetworkSettings().getNetworks().values().stream().anyMatch(value -> databaseNetwork.equals(value.getNetworkID())), "backend NIC must survive Raft partition");
            history(index, expected ? "HEALED" : "PARTITIONED", "attached=" + attached + " running=true backendAttached=true container=" + nodes.get(index).id);
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
         * Maintenance must really generate complete snapshot, collection closed slots and implement version GC.
         *
         * @throws AssertionError if the operation cannot complete
         * @throws Exception if the operation cannot complete
         */
        void maintainedAll() throws Exception {
            long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
            while (System.nanoTime() < end) {
                boolean all = true;
                for (var child : nodes) {
                    String[] fields = child.request("MAINT").split(" ");
                    all &= fields[0].equals("MAINT") && Long.parseLong(fields[1]) > 0 && Long.parseLong(fields[2]) > 0 && Long.parseLong(fields[3]) > 0 && fields[4].equals("false")
                            && Long.parseLong(fields[6]) > 0;
                }
                if (all) {
                    return;
                }
                TimeUnit.MILLISECONDS.sleep(50);
            }
            throw new AssertionError("combined automatic maintenance did not complete");
        }

        /**
         * Make sure the two remaining practical applications are durable storage prefixed without relying on the old master ' s response.
         *
         * @param excluded excluded
         * @param expected expected
         * @throws AssertionError if the operation cannot complete
         * @throws Exception if the operation cannot complete
         */
        void awaitPrefixExcept(final int excluded, final long expected) throws Exception {
            long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
            while (System.nanoTime() < end) {
                boolean all = true;
                for (int i = 0; i < 3; i++) {
                    if (i == excluded) {
                        continue;
                    }
                    String[] fields = nodes.get(i).request("STATUS").split(" ");
                    all &= fields[0].equals("STATUS") && Long.parseLong(fields[2]) == expected;
                }
                if (all) {
                    return;
                }
                TimeUnit.MILLISECONDS.sleep(30);
            }
            throw new AssertionError("durable commit did not reach peer application boundary");
        }

        /**
         * The back end stop response only acts on the self-contained packagings of this class and does not affect Raft or control channels.
         *
         * @param index index
         * @throws Exception if the operation cannot complete
         */
        void pauseBackend(final int index) throws Exception {
            var database = DATABASES.get(index);
            docker.pauseContainerCmd(database.getContainerId()).exec();
            paused[index] = true;
            var state = docker.inspectContainerCmd(database.getContainerId()).exec().getState();
            assertTrue(state.getRunning());
            assertTrue(state.getPaused());
            history(index, "BACKEND_PAUSED", "container=" + database.getContainerId() + " running=true paused=true");
        }

        /**
         * The real suspension of the back end must be followed by a de facto disqualification from business and campaigning, not just an examination of Docker's success.
         *
         * @param index index
         * @throws AssertionError if the operation cannot complete
         * @throws Exception if the operation cannot complete
         */
        void awaitQuarantined(final int index) throws Exception {
            long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(45);
            while (System.nanoTime() < end) {
                String[] fields = nodes.get(index).request("MAINT").split(" ");
                if (fields.length >= 6 && fields[4].equals("true") && fields[5].equals("true")) {
                    history(index, "QUARANTINED", "business=false campaign=false");
                    return;
                }
                TimeUnit.MILLISECONDS.sleep(50);
            }
            throw new AssertionError("failed backend was not quarantined");
        }

        /**
         * Backend recovery is not built to replace failure recovery.
         *
         * @param index index
         * @throws Exception if the operation cannot complete
         */
        void resumeBackend(final int index) throws Exception {
            if (!paused[index]) {
                return;
            }
            docker.unpauseContainerCmd(DATABASES.get(index).getContainerId()).exec();
            paused[index] = false;
            assertFalse(docker.inspectContainerCmd(DATABASES.get(index).getContainerId()).exec().getState().getPaused());
            history(index, "BACKEND_RESUMED", DATABASES.get(index).getContainerId());
        }

        /**
         * Watches the actual completion of an unknown call and does not cancel a shared operation that is running.
         *
         * @param index index
         * @return the operation result
         * @throws AssertionError if the operation cannot complete
         * @throws Exception if the operation cannot complete
         */
        String result(final int index) throws Exception {
            long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(45);
            while (System.nanoTime() < end) {
                String reply = nodes.get(index).request("RESULT");
                if (!reply.startsWith("PENDING ")) {
                    return reply;
                }
                TimeUnit.MILLISECONDS.sleep(50);
            }
            throw new AssertionError("inflight outcome still pending while owned JVM alive");
        }

        /**
         * When no original commit response is received, the resolution is recorded separately through a new home query with the original UUID and does not pretend to be an initial
         * confirmation.
         *
         * @param index index
         * @param id id
         * @throws Exception if the operation cannot complete
         */
        void resolve(final int index, final String id) throws Exception {
            String reply = nodes.get(index).request("LOOKUP " + id);
            String[] fields = reply.split(" ");
            Assertions.assertEquals("LOOKUP", fields[0]);
            Assertions.assertEquals("COMMITTED", fields[1]);
            Assertions.assertEquals(sequence + 1, Long.parseLong(fields[2]));
            assertTrue(fields[3].matches("[a-f0-9]{64}"));
            sequence++;
            live = 999;
            receipts.add(new Receipt(id, sequence, fields[3]));
            history(index, "RESOLVED", id + " " + sequence + " " + fields[3]);
        }

        /**
         * Real SQL materialization confirms cold data only and no memory access is available to claim backend recovery.
         *
         * @throws Exception if the operation cannot complete
         */
        void coldAll() throws Exception {
            for (var child : nodes) {
                long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
                while (System.nanoTime() < end) {
                    if (child.request("COLD " + sequence).equals("COLD " + sequence)) {
                        break;
                    }
                    TimeUnit.MILLISECONDS.sleep(50);
                }
                Assertions.assertEquals("COLD " + sequence, child.request("COLD " + sequence));
            }
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
            for (int i = 0; i < 3; i++) {
                try {
                    resumeBackend(i);
                } catch (final Exception close) {
                    if (failure == null) {
                        failure = close;
                    } else {
                        failure.addSuppressed(close);
                    }
                }
            }
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
            if (databaseNetwork != null) {
                for (int i = 0; i < backendAddresses.size(); i++) {
                    try {
                        docker.disconnectFromNetworkCmd().withContainerId(DATABASES.get(i).getContainerId()).withNetworkId(databaseNetwork).withForce(true).exec();
                        history(i, "BACKEND_DETACHED", DATABASES.get(i).getContainerId());
                    } catch (final Exception close) {
                        if (failure == null) {
                            failure = close;
                        } else {
                            failure.addSuppressed(close);
                        }
                    }
                }
            }
            for (String owned : Arrays.asList(network, databaseNetwork)) {
                if (owned == null) {
                    continue;
                }
                try {
                    docker.removeNetworkCmd(owned).exec();
                    history(-1, "NETWORK_REMOVED", owned);
                } catch (final Exception close) {
                    if (failure == null) {
                        failure = close;
                    } else {
                        failure.addSuppressed(close);
                    }
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
         * @param expected expected
         * @throws AssertionError if the operation cannot complete
         * @throws Exception if the operation cannot complete
         */
        void boundary(final String expected) throws Exception {
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
            while (System.nanoTime() < until) {
                String event = events.poll(remaining(until), TimeUnit.MILLISECONDS);
                if (event != null && event.startsWith("STARTED ")) {
                    continue;
                }
                Assertions.assertEquals("BOUNDARY " + expected, event);
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
