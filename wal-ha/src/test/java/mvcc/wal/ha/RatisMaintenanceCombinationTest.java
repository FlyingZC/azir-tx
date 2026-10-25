package mvcc.wal.ha;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.async.ResultCallback;
import com.github.dockerjava.api.model.*;
import com.github.dockerjava.api.model.AccessMode;
import org.testcontainers.DockerClientFactory;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.io.TempDir;
import java.io.*;
import java.nio.file.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

/** 认证 Raft、独立 MySQL、自动维护与在途事务的真实网络/后端同场故障。 */
class RatisMaintenanceCombinationTest {
    @TempDir Path directory;
    static final List<org.testcontainers.containers.MySQLContainer<?>> DATABASES = List.of(MysqlGenerationSupport.database(), MysqlGenerationSupport.database(), MysqlGenerationSupport.database());
    /** 三份自有数据库复用进程，各场景控制库和数据空间独立。 */
    @org.junit.jupiter.api.BeforeAll static void startDatabases() { org.testcontainers.lifecycle.Startables.deepStart(DATABASES.stream()).join(); }
    /** 只停止本类持有的实例。 */
    @org.junit.jupiter.api.AfterAll static void stopDatabases() { for (var database : DATABASES) { database.stop(); } }

    /** 自动维护、双网络、实际后端暂停及在途事务的同场验证。 */
    @ParameterizedTest @ValueSource(strings = {"BEFORE_APPEND", "DURABLE_REPLY_LOST", "ALL_SPLIT_REPLY_LOST"})
    void authenticatedMaintenanceBackendAndInflightFaults(String scenario) throws Exception {
        try (var cluster = new Cluster(directory, scenario)) {
            int old = cluster.leader(Set.of()); cluster.commit(old, "INIT");
            for (int value = 1; value <= 10; value++) { cluster.commit(old, "WRITE " + value); } cluster.commit(old, "DELETE"); cluster.awaitAll(); cluster.coldAll();
            for (var node : cluster.nodes) { assertEquals("LIFECYCLE true true true", node.request("LIFECYCLE")); } cluster.maintainedAll();
            String buffered = cluster.nodes.get(old).request("BUFFER"); assertTrue(buffered.startsWith("BUFFERED ")); String uncertain = buffered.split(" ")[1];
            String boundary = scenario.equals("BEFORE_APPEND") ? "BEFORE_COMMIT_APPEND" : "AFTER_COMMIT_APPLIED";
            assertEquals("ARMED " + boundary, cluster.nodes.get(old).request("ARM " + boundary)); assertEquals("SUBMITTED " + uncertain, cluster.nodes.get(old).request("FINISH_ASYNC")); cluster.nodes.get(old).boundary(boundary);
            if (!scenario.equals("BEFORE_APPEND")) { cluster.awaitPrefixExcept(old, 13); }
            int backend = scenario.equals("DURABLE_REPLY_LOST") ? old : (old + 1) % 3;
            if (scenario.equals("ALL_SPLIT_REPLY_LOST")) { for (int i = 0; i < 3; i++) { cluster.cut(i); } }
            else { cluster.cut(old); }
            cluster.pauseBackend(backend); cluster.awaitQuarantined(backend);
            String rejected = null; int current;
            if (scenario.equals("BEFORE_APPEND")) {
                current = cluster.leader(Set.of(old, backend)); cluster.commit(current, "LIVE 1");
                assertEquals("RELEASED", cluster.nodes.get(old).request("RELEASE")); rejected = cluster.result(old); assertTrue(rejected.startsWith("ERROR "), rejected); cluster.rejectFresh(old); cluster.verify(current);
                cluster.resumeBackend(backend); cluster.heal(old); cluster.awaitAll(); assertEquals("LOOKUP NOT_CONFIRMED", cluster.nodes.get(current).request("LOOKUP " + uncertain));
            } else if (scenario.equals("DURABLE_REPLY_LOST")) {
                cluster.nodes.get(old).kill("DURABLE_CLIENT_REPLY_LOST"); current = cluster.leader(Set.of(old)); cluster.resolve(current, uncertain); cluster.verify(current); cluster.commit(current, "LIVE 1");
                cluster.resumeBackend(backend); cluster.replace(old, "-"); cluster.nodes.get(old).started(); cluster.awaitAll(); cluster.verify(current);
            } else {
                assertEquals("RELEASED", cluster.nodes.get(old).request("RELEASE")); for (int i = 0; i < 3; i++) { cluster.rejectFresh(i); }
                for (int i = 0; i < 3; i++) { if (i != old) { cluster.heal(i); } }
                current = cluster.leader(Set.of(old, backend)); cluster.resolve(current, uncertain); cluster.verify(current); cluster.commit(current, "LIVE 1"); cluster.verify(current); cluster.rejectFresh(old);
                cluster.resumeBackend(backend); cluster.heal(old); cluster.awaitAll();
            }
            current = cluster.leader(Set.of()); cluster.verify(current); cluster.commit(current, "LIVE 2"); cluster.verify(current); cluster.coldAll(); cluster.maintainedAll();
            cluster.history(-1, "VERIFIED", scenario + " decisions=" + cluster.receipts.size());
            Files.writeString(cluster.root.resolve("case.json"), "{\"scenario\":\"" + scenario + "\",\"group\":\"" + cluster.group + "\",\"old\":" + old + ",\"backend\":" + backend + ",\"uncertain\":\"" + uncertain + "\",\"resolved\":" + !scenario.equals("BEFORE_APPEND") + ",\"sequence\":" + cluster.sequence + ",\"expectedDigest\":\"" + cluster.digest() + "\"}\n");
        }
    }

    /** 只保存父进程实际收到的完整提交凭据。 */
    private record Receipt(String id, long sequence, String digest) { }

    /** 一组场景独占自己的网络、容器、凭据和本地副本目录。 */
    private static final class Cluster implements AutoCloseable {
        final DockerClient docker = DockerClientFactory.instance().client(); final Path root; final UUID group = UUID.randomUUID(); final String image; String network, databaseNetwork;
        final List<String> addresses = new ArrayList<>(), backendAddresses = new ArrayList<>(), schemas = new ArrayList<>(); final boolean[] paused = new boolean[3]; final List<Child> nodes = new ArrayList<>(); final List<RatisTlsTestCertificates.Entry> credentials = new ArrayList<>(); final List<Receipt> receipts = new ArrayList<>(); final List<Bind> classpathBinds = new ArrayList<>();
        final List<String> createdContainers = new ArrayList<>();
        final Path policy, roots; final String classpath; long sequence; int blob, live; boolean deleted;
        /** 自动分配专属 bridge 网段，只连接本测试创建的三个非特权容器。 */
        Cluster(Path temporary, String scenario) throws Exception {
            String configured = System.getProperty("azir.wal.ha.combinationEvidenceDir"); root = configured == null ? temporary : Path.of(configured).resolve(scenario + "-" + group); Files.createDirectories(root);
            image = docker.inspectImageCmd("eclipse-temurin:21-jdk").exec().getId(); history(-1, "IMAGE", image);
            try {
            network = allocateNetwork("raft"); history(-1, "NETWORK_CREATED", network); databaseNetwork = allocateNetwork("backend"); history(-1, "NETWORK_CREATED", databaseNetwork);
            var subnet = docker.inspectNetworkCmd().withNetworkId(network).exec().getIpam().getConfig().getFirst().getSubnet(); byte[] prefix = java.net.InetAddress.getByName(subnet.split("/")[0]).getAddress(); assertEquals(4, prefix.length);
            // 合成密钥只在 JUnit 私有临时目录中生存，持久证据目录只保留公开策略和结果。
            var pki = new RatisTlsTestCertificates(temporary.resolve("pki")); roots = pki.trust; policy = root.resolve("policy.properties"); var values = new Properties();
            for (int i = 0; i < 3; i++) { byte[] ip = prefix.clone(); ip[3] = (byte) (10 + i); String address = java.net.InetAddress.getByAddress(ip).getHostAddress() + ":19001"; addresses.add(address); var identity = pki.issue(group, "p" + i, address); credentials.add(identity); nodes.add(null); values.setProperty("p" + i + ".address", address); values.setProperty("p" + i + ".pin", identity.trust().certificateSha256().iterator().next()); }
            byte[] backendPrefix = java.net.InetAddress.getByName(docker.inspectNetworkCmd().withNetworkId(databaseNetwork).exec().getIpam().getConfig().getFirst().getSubnet().split("/")[0]).getAddress();
            for (int i = 0; i < 3; i++) {
                byte[] ip = backendPrefix.clone(); ip[3] = (byte) (20 + i); String address = java.net.InetAddress.getByAddress(ip).getHostAddress(); backendAddresses.add(address);
                docker.connectToNetworkCmd().withContainerId(DATABASES.get(i).getContainerId()).withNetworkId(databaseNetwork).withContainerNetwork(new ContainerNetwork().withIpamConfig(new ContainerNetwork.Ipam().withIpv4Address(address))).exec(); history(i, "BACKEND_ATTACHED", "container=" + DATABASES.get(i).getContainerId() + " network=" + databaseNetwork + " address=" + address);
                String schema = "c_" + UUID.randomUUID().toString().replace("-", ""); MysqlGenerationSupport.createControl(DATABASES.get(i), schema); schemas.add(schema); values.setProperty("p" + i + ".backend", address + ":3306");
            }
            try (var output = Files.newOutputStream(policy)) { values.store(output, "owned ephemeral TLS peer policy"); }
            classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
            for (String entry : classpath.split(java.io.File.pathSeparator)) { Path file = Path.of(entry).toAbsolutePath().normalize(); assertTrue(Files.exists(file)); classpathBinds.add(new Bind(file.toString(), new Volume(file.toString()), AccessMode.ro)); }
            for (int i = 0; i < 3; i++) { replace(i, "-"); } for (var child : nodes) { child.started(); }
            } catch (Exception failure) { try { close(); } catch (Exception close) { failure.addSuppressed(close); } throw failure; }
        }
        /** 事件使用统一父进程单调时钟，控制结果与 Docker 观察一起保留。 */
        synchronized void history(int node, String kind, String text) throws IOException { Files.writeString(root.resolve("events.tsv"), System.nanoTime() + "\t" + node + "\t" + kind + "\t" + text + "\n", StandardOpenOption.CREATE, StandardOpenOption.APPEND); }
        /** 只借助 Docker 为自有临时网络分配未冲突网段，再显式创建支持固定地址的专属网络。 */
        private String allocateNetwork(String tag) throws Exception {
            String reservation = docker.createNetworkCmd().withName("azir-pool-" + group + "-" + tag).withDriver("bridge").withInternal(true).withLabels(Map.of("azir.network.test", group.toString())).exec().getId();
            history(-1, "POOL_CREATED", reservation); String subnet;
            try { subnet = docker.inspectNetworkCmd().withNetworkId(reservation).exec().getIpam().getConfig().getFirst().getSubnet(); }
            finally { docker.removeNetworkCmd(reservation).exec(); history(-1, "POOL_REMOVED", reservation); }
            return docker.createNetworkCmd().withName("azir-net-" + group + "-" + tag).withDriver("bridge").withInternal(true).withLabels(Map.of("azir.network.test", group.toString()))
                    .withIpam(new Network.Ipam().withConfig(new Network.Ipam.Config().withSubnet(subnet))).exec().getId();
        }
        /** 只有旧容器已终止才允许重建，复用真实投票和业务目录。 */
        void replace(int index, String boundary) throws Exception {
            var previous = nodes.get(index); if (previous != null) { assertFalse(docker.inspectContainerCmd(previous.id).exec().getState().getRunning(), "cannot restart a live observation timeout"); previous.close(); }
            Path data = root.resolve("p" + index); Files.createDirectories(data); var entry = credentials.get(index); var binds = new ArrayList<>(classpathBinds);
            for (Path path : List.of(policy, roots, entry.identity())) { binds.add(new Bind(path.toAbsolutePath().toString(), new Volume(path.toAbsolutePath().toString()), AccessMode.ro)); } binds.add(new Bind(data.toAbsolutePath().toString(), new Volume(data.toAbsolutePath().toString()), AccessMode.rw));
            var host = HostConfig.newHostConfig().withNetworkMode(network).withBinds(binds).withNanoCPUs(500_000_000L).withMemory(512L * 1024 * 1024).withPidsLimit(256L).withReadonlyRootfs(true).withCapDrop(Capability.ALL).withSecurityOpts(List.of("no-new-privileges")).withTmpFs(Map.of("/tmp", "rw,nosuid,noexec,size=32m"));
            String user = Files.getAttribute(data, "unix:uid") + ":" + Files.getAttribute(data, "unix:gid");
            Path ready = data.resolve(".network-ready"); Files.deleteIfExists(ready);
            var actual = List.of("java", "-Xmx384m", "-XX:ActiveProcessorCount=2", "-cp", classpath, RatisCombinationProcessProbe.class.getName(), data.toAbsolutePath().toString(), "p" + index, group.toString(), policy.toAbsolutePath().toString(), entry.identity().toAbsolutePath().toString(), roots.toAbsolutePath().toString(), boundary);
            var command = new ArrayList<>(List.of("sh", "-c", "while [ ! -f \"$1\" ]; do sleep 0.02; done; shift; exec \"$@\"", "owned-combination-start", ready.toAbsolutePath().toString())); command.addAll(actual);
            String id = docker.createContainerCmd(image).withName("azir-jvm-" + group + "-" + index + "-" + UUID.randomUUID().toString().substring(0, 8)).withHostConfig(host).withUser(user).withIpv4Address(addresses.get(index).split(":")[0]).withLabels(Map.of("azir.network.test", group.toString())).withEnv("AZIR_WAL_COMBINATION_SCHEMA=" + schemas.get(index), "AZIR_WAL_COMBINATION_PASSWORD=" + DATABASES.get(index).getPassword()).withStdinOpen(true).withAttachStdin(true).withAttachStdout(true).withAttachStderr(true).withCmd(command).exec().getId();
            createdContainers.add(id); history(index, "CONTAINER_CREATED", id); docker.startContainerCmd(id).exec();
            docker.connectToNetworkCmd().withContainerId(id).withNetworkId(databaseNetwork).exec(); history(index, "JVM_BACKEND_ATTACHED", "container=" + id + " network=" + databaseNetwork); Files.writeString(ready, "ready", StandardOpenOption.CREATE_NEW);
            var child = new Child(this, index, id); nodes.set(index, child);
            var state = docker.inspectContainerCmd(id).exec(); assertEquals(500_000_000L, state.getHostConfig().getNanoCPUs()); assertEquals(512L * 1024 * 1024, state.getHostConfig().getMemory()); assertTrue(state.getState().getRunning()); history(index, "JVM_RUNNING", "container=" + id + " hostPid=" + state.getState().getPidLong()); history(index, "ATTACHED", "attached=true running=true container=" + id);
        }
        /** 实际摘除专属数据网卡，确认 JVM 仍活着且原网络已不存在。 */
        void cut(int index) throws Exception { docker.disconnectFromNetworkCmd().withContainerId(nodes.get(index).id).withNetworkId(network).withForce(true).exec(); topology(index, false); }
        /** 恢复原来的静态地址，已有进程直接重连，不以重启代替网络愈合。 */
        void heal(int index) throws Exception { docker.connectToNetworkCmd().withContainerId(nodes.get(index).id).withNetworkId(network).withContainerNetwork(new ContainerNetwork().withIpamConfig(new ContainerNetwork.Ipam().withIpv4Address(addresses.get(index).split(":")[0]))).exec(); topology(index, true); }
        /** 记录来自 Docker 的真实连接与进程状态，不能只依赖注入命令成功返回。 */
        void topology(int index, boolean expected) throws Exception { var state = docker.inspectContainerCmd(nodes.get(index).id).exec(); boolean attached = state.getNetworkSettings().getNetworks().values().stream().anyMatch(value -> network.equals(value.getNetworkID())); assertEquals(expected, attached); assertTrue(state.getState().getRunning()); assertTrue(state.getNetworkSettings().getNetworks().values().stream().anyMatch(value -> databaseNetwork.equals(value.getNetworkID())), "backend NIC must survive Raft partition"); history(index, expected ? "HEALED" : "PARTITIONED", "attached=" + attached + " running=true backendAttached=true container=" + nodes.get(index).id); }
        /** 所有新事务与新快照查询都必须依赖多数派，孤立节点不能回退缓存成功。 */
        void rejectFresh(int index) throws Exception { for (String command : List.of("LIVE 998", "READ", "PING")) { assertTrue(nodes.get(index).request(command).startsWith("ERROR ")); } }
        /** 发现角色后再执行真实读屏障，不把旧主缓存角色当成仍有多数派。 */
        int leader(Set<Integer> excluded) throws Exception { long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(60); while (System.nanoTime() < until) { for (int i = 0; i < 3; i++) { if (excluded.contains(i) || nodes.get(i) == null || !nodes.get(i).running()) { continue; } String status = nodes.get(i).request("STATUS"); if (status.startsWith("STATUS ") && Long.parseLong(status.split(" ")[1]) > 0 && nodes.get(i).request("PING").startsWith("PING ")) { return i; } } TimeUnit.MILLISECONDS.sleep(50); } throw new AssertionError("no authenticated majority leader"); }
        /** 固定目标直到其角色和读屏障均真正成立。 */
        void awaitLeader(int index) throws Exception { long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(60); while (System.nanoTime() < until) { String status = nodes.get(index).request("STATUS"); if (status.startsWith("STATUS ") && Long.parseLong(status.split(" ")[1]) > 0 && nodes.get(index).request("PING").startsWith("PING ")) { return; } TimeUnit.MILLISECONDS.sleep(50); } throw new AssertionError("target leader not serving"); }
        /** 维护必须真正生成完整快照、回收闭合段并执行版本 GC。 */
        void maintainedAll() throws Exception { long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(90); while (System.nanoTime() < end) { boolean all = true; for (var child : nodes) { String[] fields = child.request("MAINT").split(" "); all &= fields[0].equals("MAINT") && Long.parseLong(fields[1]) > 0 && Long.parseLong(fields[2]) > 0 && Long.parseLong(fields[3]) > 0 && fields[4].equals("false") && Long.parseLong(fields[6]) > 0; } if (all) { return; } TimeUnit.MILLISECONDS.sleep(50); } throw new AssertionError("combined automatic maintenance did not complete"); }
        /** 不依赖旧主回复，先确认其余两份实际应用了已持久化前缀。 */
        void awaitPrefixExcept(int excluded, long expected) throws Exception { long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(60); while (System.nanoTime() < end) { boolean all = true; for (int i = 0; i < 3; i++) { if (i == excluded) { continue; } String[] fields = nodes.get(i).request("STATUS").split(" "); all &= fields[0].equals("STATUS") && Long.parseLong(fields[2]) == expected; } if (all) { return; } TimeUnit.MILLISECONDS.sleep(30); } throw new AssertionError("durable commit did not reach peer application boundary"); }
        /** 后端停止响应只作用于本类自有容器，不影响 Raft 或控制通道。 */
        void pauseBackend(int index) throws Exception { var database = DATABASES.get(index); docker.pauseContainerCmd(database.getContainerId()).exec(); paused[index] = true; var state = docker.inspectContainerCmd(database.getContainerId()).exec().getState(); assertTrue(state.getRunning()); assertTrue(state.getPaused()); history(index, "BACKEND_PAUSED", "container=" + database.getContainerId() + " running=true paused=true"); }
        /** 后端真实暂停后必须实际撤销业务及竞选资格，不能只检查 Docker 注入成功。 */
        void awaitQuarantined(int index) throws Exception { long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(45); while (System.nanoTime() < end) { String[] fields = nodes.get(index).request("MAINT").split(" "); if (fields.length >= 6 && fields[4].equals("true") && fields[5].equals("true")) { history(index, "QUARANTINED", "business=false campaign=false"); return; } TimeUnit.MILLISECONDS.sleep(50); } throw new AssertionError("failed backend was not quarantined"); }
        /** 原后端原地恢复，不用重新建库代替故障恢复。 */
        void resumeBackend(int index) throws Exception { if (!paused[index]) { return; } docker.unpauseContainerCmd(DATABASES.get(index).getContainerId()).exec(); paused[index] = false; assertFalse(docker.inspectContainerCmd(DATABASES.get(index).getContainerId()).exec().getState().getPaused()); history(index, "BACKEND_RESUMED", DATABASES.get(index).getContainerId()); }
        /** 观察未知调用的实际完成结果，不取消正在运行的共享操作。 */
        String result(int index) throws Exception { long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(45); while (System.nanoTime() < end) { String reply = nodes.get(index).request("RESULT"); if (!reply.startsWith("PENDING ")) { return reply; } TimeUnit.MILLISECONDS.sleep(50); } throw new AssertionError("inflight outcome still pending while owned JVM alive"); }
        /** 未收到原提交回复时通过新主查询原 UUID，单独记录解析结果，不冒充最初确认。 */
        void resolve(int index, String id) throws Exception { String reply = nodes.get(index).request("LOOKUP " + id); String[] fields = reply.split(" "); assertEquals("LOOKUP", fields[0]); assertEquals("COMMITTED", fields[1]); assertEquals(sequence + 1, Long.parseLong(fields[2])); assertTrue(fields[3].matches("[a-f0-9]{64}")); sequence++; live = 999; receipts.add(new Receipt(id, sequence, fields[3])); history(index, "RESOLVED", id + " " + sequence + " " + fields[3]); }
        /** 真实 SQL 物化后才确认冷数据，不凭内存读取声称后端恢复。 */
        void coldAll() throws Exception { for (var child : nodes) { long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(60); while (System.nanoTime() < end) { if (child.request("COLD " + sequence).equals("COLD " + sequence)) { break; } TimeUnit.MILLISECONDS.sleep(50); } assertEquals("COLD " + sequence, child.request("COLD " + sequence)); } }

        /** 三节点实际应用业务前缀后才宣告恢复结束。 */
        void awaitAll() throws Exception { long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(90); while (System.nanoTime() < until) { boolean ready = true; for (var child : nodes) { String status = child.request("STATUS"); ready &= status.startsWith("STATUS ") && Long.parseLong(status.split(" ")[2]) == sequence && status.endsWith(" true"); } if (ready) { return; } TimeUnit.MILLISECONDS.sleep(50); } throw new AssertionError("authenticated replicas did not catch up"); }
        /** 成功响应必须有连续提交序号与完整 UUID、摘要，才更新独立预期模型。 */
        void commit(int index, String command) throws Exception { String response = nodes.get(index).request(command); assertTrue(response.startsWith("COMMITTED "), response); String[] fields = response.split(" "); UUID.fromString(fields[1]); assertEquals(++sequence, Long.parseLong(fields[2])); assertTrue(fields[3].matches("[a-f0-9]{64}")); receipts.add(new Receipt(fields[1], sequence, fields[3])); if (command.startsWith("WRITE ")) { blob = Integer.parseInt(command.split(" ")[1]); } if (command.startsWith("LIVE ")) { live = Integer.parseInt(command.split(" ")[1]); } if (command.equals("DELETE")) { deleted = true; } }
        /** 外部完整数据摘要和每份确认凭据均核对，不能只看余额总和。 */
        void verify(int index) throws Exception { assertEquals("DATA " + sequence + " " + digest(), nodes.get(index).request("READ")); for (var receipt : receipts) { assertEquals("LOOKUP COMMITTED " + receipt.sequence() + " " + receipt.digest(), nodes.get(index).request("LOOKUP " + receipt.id())); } }
        /** 父进程独立重建六十八个键位的预期内容，明确区分墓碑和空值。 */
        String digest() throws Exception { var hash = java.security.MessageDigest.getInstance("SHA-256"); for (String shard : List.of("a", "b")) { for (int i = 0; i < 32; i++) { entry(hash, shard + "/cold" + i, deleted && i == 0 ? null : (shard + ":" + i).getBytes(StandardCharsets.UTF_8)); } byte[] value = blob == 0 ? new byte[]{0} : new byte[480 * 1024]; if (blob != 0) { Arrays.fill(value, (byte) (blob + (shard.equals("a") ? 0 : 100))); } entry(hash, shard + "/blob", value); entry(hash, shard + "/live", Integer.toString(shard.equals("a") ? live : -live).getBytes(StandardCharsets.UTF_8)); } return HexFormat.of().formatHex(hash.digest()); }
        /** 长度与墓碑编码由父进程生成，避免拼接歧义。 */
        private static void entry(java.security.MessageDigest hash, String key, byte[] value) { byte[] name = key.getBytes(StandardCharsets.UTF_8); hash.update(java.nio.ByteBuffer.allocate(4).putInt(name.length).array()); hash.update(name); hash.update(java.nio.ByteBuffer.allocate(4).putInt(value == null ? -1 : value.length).array()); if (value != null) { hash.update(value); } }
        /** 只清理本测试明确创建的容器和网络，异常不能跳过其余资源。 */
        @Override public void close() throws Exception {
            Exception failure = null;
            for (int i = 0; i < 3; i++) { try { resumeBackend(i); } catch (Exception close) { if (failure == null) { failure = close; } else { failure.addSuppressed(close); } } }
            for (var child : nodes) { if (child != null) { try { child.close(); } catch (Exception close) { if (failure == null) { failure = close; } else { failure.addSuppressed(close); } } } }
            for (String id : createdContainers) { try {
                var state = docker.inspectContainerCmd(id).exec(); if (Boolean.TRUE.equals(state.getState().getRunning())) { history(-1, "CLEANUP_FORCED_KILL", id); docker.stopContainerCmd(id).withTimeout(10).exec(); }
                docker.removeContainerCmd(id).exec(); history(-1, "CONTAINER_REMOVED", id);
            } catch (com.github.dockerjava.api.exception.NotFoundException removed) { /* 已由正常子进程关闭流程清理。 */ }
            catch (Exception close) { if (failure == null) { failure = close; } else { failure.addSuppressed(close); } } }
            if (databaseNetwork != null) { for (int i = 0; i < backendAddresses.size(); i++) { try { docker.disconnectFromNetworkCmd().withContainerId(DATABASES.get(i).getContainerId()).withNetworkId(databaseNetwork).withForce(true).exec(); history(i, "BACKEND_DETACHED", DATABASES.get(i).getContainerId()); } catch (Exception close) { if (failure == null) { failure = close; } else { failure.addSuppressed(close); } } } }
            for (String owned : Arrays.asList(network, databaseNetwork)) { if (owned == null) { continue; } try { docker.removeNetworkCmd(owned).exec(); history(-1, "NETWORK_REMOVED", owned); } catch (Exception close) { if (failure == null) { failure = close; } else { failure.addSuppressed(close); } } }
            if (failure != null) { throw failure; }
        }
    }

    /** 容器 stdin 与 stdout 附着通道不经过测试数据网卡，分裂时仍能询问实际节点。 */
    private static final class Child implements AutoCloseable {
        final Cluster cluster; final int index; final String id; final BufferedWriter commands; final ResultCallback.Adapter<Frame> attached; final BlockingQueue<String> replies = new ArrayBlockingQueue<>(64), events = new ArrayBlockingQueue<>(16); final StringBuilder partial = new StringBuilder(); boolean removed;
        /** 新容器的历史日志只属于当前 JVM，不复用旧容器日志伪造启动成功。 */
        Child(Cluster cluster, int index, String id) throws Exception {
            this.cluster = cluster; this.index = index; this.id = id; var input = new PipedInputStream(8192); commands = new BufferedWriter(new OutputStreamWriter(new PipedOutputStream(input), StandardCharsets.UTF_8));
            attached = new ResultCallback.Adapter<>() {
                /** 回调按真实帧拆分，stdout 队列和 stderr 文件各自有界。 */
                @Override public void onNext(Frame frame) {
                    try {
                        if (frame.getStreamType() == StreamType.STDOUT) { consume(frame.getPayload()); }
                        else if (frame.getStreamType() == StreamType.STDERR) { Path log = cluster.root.resolve("stderr-" + index + "-" + id.substring(0, 12) + ".log"); if (!Files.exists(log) || Files.size(log) < 32L * 1024 * 1024) { Files.write(log, frame.getPayload(), StandardOpenOption.CREATE, StandardOpenOption.APPEND); } }
                    } catch (Exception failure) { replies.offer("READER_FAILED"); }
                }
            };
            cluster.docker.attachContainerCmd(id).withStdIn(input).withStdOut(true).withStdErr(true).withFollowStream(true).withLogs(true).exec(attached);
            try { String boot = events.poll(30, TimeUnit.SECONDS); assertNotNull(boot, "container BOOT missing"); assertTrue(boot.startsWith("BOOT "), boot); }
            catch (Throwable failure) { attached.close(); commands.close(); throw failure; }
        }
        /** 只接受完整 AZIR 事件行，不把 native 日志当作业务确认。 */
        synchronized void consume(byte[] payload) throws Exception { partial.append(new String(payload, StandardCharsets.UTF_8)); if (partial.length() > 65536) { throw new IOException("probe line budget exceeded"); } int newline; while ((newline = partial.indexOf("\n")) >= 0) { String line = partial.substring(0, newline).stripTrailing(); partial.delete(0, newline + 1); if (!line.startsWith("AZIR ")) { continue; } String value = line.substring(5); cluster.history(index, "RECV", value); var queue = value.startsWith("BOOT ") || value.startsWith("STARTED ") || value.startsWith("BOUNDARY ") ? events : replies; if (!queue.offer(value)) { throw new IOException("probe output queue exceeded"); } } }
        /** 正常装配才等待 STARTED；恢复边界强杀允许在服务就绪以前发生。 */
        void started() throws Exception { String event = events.poll(90, TimeUnit.SECONDS); assertNotNull(event); assertTrue(event.startsWith("STARTED "), event); }
        /** 冻结发生于真实持久安装边界，不通过固定等待猜测安装位置。 */
        void boundary(String expected) throws Exception { long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(90); while (System.nanoTime() < until) { String event = events.poll(remaining(until), TimeUnit.MILLISECONDS); if (event != null && event.startsWith("STARTED ")) { continue; } assertEquals("BOUNDARY " + expected, event); assertTrue(running()); return; } throw new AssertionError("installation boundary observation expired"); }
        /** 命令结果超时只报告观察失败，不自动杀掉或重建节点。 */
        String request(String command) throws Exception { synchronized (commands) { assertTrue(running(), "owned JVM already terminal"); cluster.history(index, "SEND", command); commands.write(command); commands.newLine(); commands.flush(); long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(45); while (System.nanoTime() < until) { String reply = replies.poll(remaining(until), TimeUnit.MILLISECONDS); if (reply != null && reply.startsWith("INTENT ")) { continue; } if (reply != null) { return reply; } } throw new AssertionError("command observation expired; container still running=" + running()); } }
        /** Docker 状态是进程存活的事实来源。 */
        boolean running() { return !removed && Boolean.TRUE.equals(cluster.docker.inspectContainerCmd(id).exec().getState().getRunning()); }
        /** 只强杀已记录的自有容器，确认非零终态后才允许调用 replace。 */
        void kill(String boundary) throws Exception { cluster.history(index, "KILL", boundary); cluster.docker.killContainerCmd(id).withSignal("KILL").exec(); awaitExit(); var state = cluster.docker.inspectContainerCmd(id).exec().getState(); assertNotEquals(0, state.getExitCodeLong()); cluster.history(index, "EXIT", Long.toString(state.getExitCodeLong())); }
        /** 等待明确终态，观察超时不能被当作终止证明。 */
        void awaitExit() throws Exception { long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(30); while (running() && System.nanoTime() < until) { TimeUnit.MILLISECONDS.sleep(30); } assertFalse(running(), "owned container did not exit"); }
        /** 正常清理先 STOP，失败的强制清理单独记录，不能冒充故障注入。 */
        @Override public void close() throws Exception { if (removed) { return; } if (running()) { try { request("STOP"); awaitExit(); } catch (Exception failure) { cluster.history(index, "CLEANUP_FORCED_KILL", id); cluster.docker.killContainerCmd(id).withSignal("KILL").exec(); awaitExit(); } } attached.close(); commands.close(); cluster.docker.removeContainerCmd(id).exec(); removed = true; cluster.history(index, "CONTAINER_REMOVED", id); }
        /** 使用同一等待期限。 */
        private static long remaining(long until) { return Math.max(1, TimeUnit.NANOSECONDS.toMillis(until - System.nanoTime())); }
    }
}
