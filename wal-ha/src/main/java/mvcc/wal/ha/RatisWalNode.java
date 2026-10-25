package mvcc.wal.ha;

import mvcc.common.IsolationLevel;
import mvcc.wal.*;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.protocol.*;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.TimeDuration;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/** 真实 Raft 与 WAL/MVCC 的节点集成，回环验收和显式认证装配保持独立入口。 */
public final class RatisWalNode implements AutoCloseable {
    private final WalReplicaIdentity identity;
    private final RaftGroup group;
    private final Authority authority;
    private final RatisWalState state;
    private final RatisWalStateMachine machine;
    private final RaftServer server;
    private final RatisTlsSecurity security;
    private final RatisTransportBudget transport;
    private final Path nodeDirectory;
    private volatile RatisHealthController health;
    private volatile boolean healthQuarantined, healthCampaignBlocked;
    private final AtomicLong campaignDenials = new AtomicLong();
    private volatile long healthRetiredTerm;
    private volatile RatisSnapshotController snapshotController;
    private volatile RatisGcController versionGc;
    private volatile boolean snapshotLogPressure;
    private volatile boolean serving;
    private volatile boolean closed;
    private final java.util.concurrent.locks.ReentrantLock closeGuard = new java.util.concurrent.locks.ReentrantLock();
    private final java.util.concurrent.locks.ReentrantLock membershipGuard = new java.util.concurrent.locks.ReentrantLock();
    private final ClientId membershipClient = ClientId.randomId();
    private final AtomicLong membershipCalls = new AtomicLong();
    private final java.util.concurrent.ExecutorService membershipExecutor = new java.util.concurrent.ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS,
            new java.util.concurrent.ArrayBlockingQueue<>(1), Thread.ofPlatform().name("wal-membership-command").factory(), new java.util.concurrent.ThreadPoolExecutor.AbortPolicy());
    private java.util.concurrent.ScheduledExecutorService retirementExecutor;
    private final AtomicLong retirementRuns = new AtomicLong(), retiredGenerations = new AtomicLong(), retiredSchemas = new AtomicLong(), retiredEntries = new AtomicLong();
    private volatile RetirementProgress lastRetirement;
    private volatile String retirementFailure;

    /** 回环测试入口只接受固定三个唯一节点，不允许把未配置 TLS 的装配暴露到外部地址。 */
    public static RatisWalNode openLoopback(Path directory, RaftPeerId id, RaftGroup group,
                                            Set<String> shards, Map<String, WalMaterializedStore> stores, WalOptions options) throws IOException {
        if (group.getPeers().size() != 3 || group.getPeers().stream().anyMatch(peer -> !peer.getAddress().startsWith("127.0.0.1:"))
                || group.getPeer(id) == null) { throw new IllegalArgumentException("initial Raft integration requires three loopback peers"); }
        for (var store : stores.values()) { store.validateProduction(); }
        return new RatisWalNode(directory, id, group, shards, stores, options, null, WalBackupOptions.defaults());
    }

    /** 可安装快照的回环装配使用独立代次工厂，固定后端入口不会被隐式升级。 */
    public static RatisWalNode openGenerationalLoopback(Path directory, RaftPeerId id, RaftGroup group, Set<String> shards,
                                                        WalReplicaBackendFactory factory, WalOptions options, WalBackupOptions limits) throws IOException {
        return openGenerationalForTest(directory, id, group, shards, factory, options, limits, stage -> { });
    }

    /** 对外端点必须显式提供双向 TLS 身份与授权策略，后端仍使用独立快照代次。 */
    public static RatisWalNode openAuthenticated(Path directory, RaftPeerId id, RaftGroup group, Set<String> shards,
                                                 WalReplicaBackendFactory factory, WalOptions options, WalBackupOptions limits, RatisTlsSecurity security) throws IOException {
        return openAuthenticatedForTest(directory, id, group, shards, factory, options, limits, security, stage -> { });
    }

    /** 生产装配可显式限制认证连接和调用数量，原入口使用有界默认值。 */
    public static RatisWalNode openAuthenticated(Path directory, RaftPeerId id, RaftGroup group, Set<String> shards,
                                                 WalReplicaBackendFactory factory, WalOptions options, WalBackupOptions limits, RatisTlsSecurity security, RatisTransportLimits transportLimits) throws IOException {
        java.util.Objects.requireNonNull(security).validate(group, id);
        return new RatisWalNode(directory, id, group, shards, Map.of(), options, java.util.Objects.requireNonNull(factory), java.util.Objects.requireNonNull(limits), stage -> { }, security, transportLimits);
    }

    /** TLS 与真实安装边界调度共用生产构造，包内注入不替代认证或持久化。 */
    static RatisWalNode openAuthenticatedForTest(Path directory, RaftPeerId id, RaftGroup group, Set<String> shards,
                                                WalReplicaBackendFactory factory, WalOptions options, WalBackupOptions limits, RatisTlsSecurity security, RatisWalState.Hook hook) throws IOException {
        java.util.Objects.requireNonNull(security).validate(group, id);
        return new RatisWalNode(directory, id, group, shards, Map.of(), options, java.util.Objects.requireNonNull(factory), java.util.Objects.requireNonNull(limits), hook, security);
    }

    /** 托管装配测试同时保持正式传输限制，钩子不暴露给业务调用方。 */
    static RatisWalNode openAuthenticatedForTest(Path directory, RaftPeerId id, RaftGroup group, Set<String> shards,
                                                WalReplicaBackendFactory factory, WalOptions options, WalBackupOptions limits, RatisTlsSecurity security, RatisTransportLimits transport, RatisWalState.Hook hook) throws IOException {
        java.util.Objects.requireNonNull(security).validate(group, id);
        return new RatisWalNode(directory, id, group, shards, Map.of(), options, java.util.Objects.requireNonNull(factory), java.util.Objects.requireNonNull(limits), hook, security, transport);
    }

    /** 包内安装边界调度只用于真实集群故障验证，普通入口始终传空钩子。 */
    static RatisWalNode openGenerationalForTest(Path directory, RaftPeerId id, RaftGroup group, Set<String> shards,
                                               WalReplicaBackendFactory factory, WalOptions options, WalBackupOptions limits, RatisWalState.Hook hook) throws IOException {
        if (group.getPeers().size() < 3 || group.getPeers().size() > 9 || group.getPeer(id) == null
                || group.getPeers().stream().anyMatch(peer -> !peer.getAddress().matches("127\\.0\\.0\\.1:[0-9]{1,5}"))) { throw new IllegalArgumentException("generational integration requires bounded loopback peers"); }
        for (var peer : group.getPeers()) { int port = Integer.parseInt(peer.getAddress().substring(10)); if (port < 1 || port > 65535) { throw new IllegalArgumentException("invalid loopback port"); } }
        return new RatisWalNode(directory, id, group, shards, Map.of(), options, java.util.Objects.requireNonNull(factory), java.util.Objects.requireNonNull(limits), hook);
    }

    /** 先绑定本地节点和独立存储，再启动共识，角色尚未就绪时不服务业务请求。 */
    private RatisWalNode(Path directory, RaftPeerId id, RaftGroup group, Set<String> shards,
                          Map<String, WalMaterializedStore> stores, WalOptions options, WalReplicaBackendFactory factory, WalBackupOptions limits) throws IOException {
        this(directory, id, group, shards, stores, options, factory, limits, stage -> { });
    }

    /** 正式装配和包内调度共用相同持久性协议，钩子不能替代任何实际文件操作。 */
    private RatisWalNode(Path directory, RaftPeerId id, RaftGroup group, Set<String> shards,
                          Map<String, WalMaterializedStore> stores, WalOptions options, WalReplicaBackendFactory factory, WalBackupOptions limits, RatisWalState.Hook hook) throws IOException {
        this(directory, id, group, shards, stores, options, factory, limits, hook, null);
    }

    /** 认证配置在任何数据打开之前核对，模式标记只在取得代次锁之后发布。 */
    private RatisWalNode(Path directory, RaftPeerId id, RaftGroup group, Set<String> shards,
                          Map<String, WalMaterializedStore> stores, WalOptions options, WalReplicaBackendFactory factory, WalBackupOptions limits, RatisWalState.Hook hook, RatisTlsSecurity security) throws IOException {
        this(directory, id, group, shards, stores, options, factory, limits, hook, security, RatisTransportLimits.defaults());
    }

    /** 认证装配将连接名额分配在 TLS 之前，旧回环装配不改变其传输工厂。 */
    private RatisWalNode(Path directory, RaftPeerId id, RaftGroup group, Set<String> shards,
                          Map<String, WalMaterializedStore> stores, WalOptions options, WalReplicaBackendFactory factory, WalBackupOptions limits, RatisWalState.Hook hook, RatisTlsSecurity security, RatisTransportLimits transportLimits) throws IOException {
        this.security = security; transport = new RatisTransportBudget(transportLimits);
        this.group = group; identity = new WalReplicaIdentity(group.getGroupId().getUuid(), group.getGroupId().getUuid(), id.toString()); nodeDirectory = directory;
        var healthPolicy = RatisHealthRecord.load(directory, identity); healthQuarantined = healthPolicy != null; healthCampaignBlocked = healthPolicy != null;
        var gcPolicy = RatisGcPolicy.load(directory, identity);
        if (gcPolicy != null && (healthPolicy == null || factory == null)) { throw new IOException("version GC policy requires generational health management"); }
        var snapshotPolicy = RatisSnapshotPolicy.load(directory, identity); snapshotLogPressure = snapshotPolicy != null;
        if (snapshotPolicy != null && (healthPolicy == null || factory == null || !factory.supportsRetirement())) { throw new IOException("snapshot policy requires health management and bounded generational retirement"); }
        if (healthPolicy != null && factory == null) { throw new IOException("health policy requires generational backend"); }
        RatisTransportIdentity.verify(directory, identity, security != null);
        Path wal = directory.resolve("wal"); Path raft = directory.resolve("raft");
        Path business = directory.resolve("business");
        boolean bootstrap = factory != null && validBootstrap(business, identity);
        boolean recovering = hasRaftMetadata(raft, group.getGroupId(), bootstrap);
        if (bootstrap && recovering) { verifyInitialVote(raft.resolve(group.getGroupId().getUuid().toString()).resolve("current/raft-meta")); }
        if (factory == null && Files.exists(business) || factory != null && Files.exists(wal)) { throw new IOException("cannot implicitly convert node storage layout"); }
        boolean evidence = factory == null ? Files.exists(wal.resolve("REPLICA")) : Files.exists(business.resolve("ACTIVE")) || Files.exists(business.resolve("INSTALLING")) || bootstrap;
        if (evidence && !recovering && !bootstrap) { throw new IOException("replica Raft voting state missing; rebuild with a new peer identity"); }
        if (!evidence && recovering) { throw new IOException("replica MVCC state missing; snapshot recovery required"); }
        Authority openedAuthority = null;
        RatisWalState openedState = null;
        RatisWalStateMachine openedMachine = null;
        RaftServer created = null;
        try {
            // 构造早期的资源也进入清理范围；只保存实际返回的对象，避免访问未赋值的 final 字段。
            authority = openedAuthority = new Authority();
            state = openedState = factory == null ? new RatisWalState(WalMvccRuntime.openReplica(wal, shards, stores, options, WalRetentionOptions.preserveAll(), identity, authority))
                    : new RatisWalState(business, identity, shards, options, authority, factory, limits, hook);
            machine = openedMachine = new RatisWalStateMachine(identity, state, authority, limits, () -> !healthQuarantined && !healthCampaignBlocked && !snapshotLogPressure);
            var endpoint = RatisEndpoint.parse(group.getPeer(id).getAddress()); var properties = properties(raft, endpoint.host(), endpoint.port());
            if (factory != null) {
                RaftServerConfigKeys.Log.setPurgeGap(properties, 1);
                RaftServerConfigKeys.Snapshot.setTriggerWhenStopEnabled(properties, false);
                RaftServerConfigKeys.Snapshot.setTriggerWhenRemoveEnabled(properties, false);
                org.apache.ratis.RaftConfigKeys.Rpc.setType(properties, new DurableGrpcRpcType());
            }
            // 读取到完整记录不能替代持久确认；取得独占代次锁后、服务启动前重新同步控制策略。
            if (healthPolicy != null) { healthPolicy.publish(directory); }
            if (gcPolicy != null) { gcPolicy.publish(directory); }
            if (snapshotPolicy != null) { snapshotPolicy.publish(directory); }
            if (gcPolicy != null) { state.requireBoundedGc(); }
            if (snapshotPolicy != null) { state.requirePinnedSnapshots(); }
            var parameters = clientParameters(); var transportServer = new AtomicReference<RaftServer>();
            DurableGrpcRpcType.setCampaignGuard(parameters, this::campaignAllowed);
            if (security != null) {
                RatisTransportIdentity.persist(directory, identity); GrpcConfigKeys.setUseEpoll(properties, false);
                // 官方分离心跳流重建时可能留下另一条旧流，共用单流避免耗尽有限 HTTP2 名额。
                GrpcConfigKeys.Server.setHeartbeatChannel(properties, false);
                GrpcConfigKeys.Server.setServicesCustomizer(parameters, (builder, types) -> { builder.intercept(new RatisTlsInterceptor(security, peer -> authenticatedMember(transportServer.get(), peer))); return transport.configure(builder); });
            }
            created = RaftServer.newBuilder().setServerId(id).setGroup(group).setProperties(properties).setParameters(parameters).setStateMachine(machine)
                    .setOption(recovering ? org.apache.ratis.server.storage.RaftStorage.StartupOption.RECOVER : org.apache.ratis.server.storage.RaftStorage.StartupOption.FORMAT).build();
            server = created; transportServer.set(server); authority.server.set(server); server.start(); serving = true;
            if (healthPolicy != null) { startHealth(healthPolicy.options()); }
            if (snapshotPolicy != null) { startSnapshotLifecycle(snapshotPolicy.options()); }
            if (gcPolicy != null) { startVersionGc(gcPolicy.options()); }
        } catch (Throwable failure) {
            serving = false;
            var retainedServer = created; var retainedMachine = openedMachine;
            var retainedAuthority = openedAuthority; var retainedState = openedState;
            // 前一阶段尚未排空时不关闭它依赖的存储，异常返回仅清理句柄供调用方沿原阶段重试。
            var cleanup = new RatisCloseSequence(this::stopMaintenance,
                    () -> { if (retainedServer != null) { retainedServer.close(); } },
                    () -> { if (retainedMachine != null) { retainedMachine.close(); } },
                    () -> { if (retainedAuthority != null) { retainedAuthority.closeControl(); } },
                    this::closeMembershipExecutor,
                    () -> { if (retainedState != null) { retainedState.close(); } });
            throw RatisStartupException.afterFailure(failure, cleanup);
        }
    }

    /** 先停止维护生产者，保留尚未排空任务所需的共识、内核及后端资源。 */
    private void stopMaintenance() throws IOException {
        if (versionGc != null) { versionGc.close(); }
        if (snapshotController != null) { snapshotController.close(); }
        if (health != null) { health.close(); }
        membershipExecutor.shutdown();
        if (retirementExecutor != null) {
            retirementExecutor.shutdown();
            try { if (!retirementExecutor.awaitTermination(30, TimeUnit.SECONDS)) { throw new IOException("retirement maintenance did not drain"); } }
            catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new IOException("retirement close interrupted", interrupted); }
        }
    }

    /** 共识停止后排空成员命令，再允许释放本地代次锁和数据库租约。 */
    private void closeMembershipExecutor() throws IOException {
        membershipExecutor.shutdown();
        try { if (!membershipExecutor.awaitTermination(30, TimeUnit.SECONDS)) { throw new IOException("membership executor did not drain"); } }
        catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new IOException("membership executor close interrupted", interrupted); }
    }

    /** 实际共识配置包含联合阶段旧成员和非投票成员，移除完成后旧证书也失去 RPC 资格。 */
    private boolean authenticatedMember(RaftServer current, String peer) {
        if (current == null) { return false; }
        try {
            var configuration = current.getDivision(group.getGroupId()).getRaftConf(); var id = RaftPeerId.valueOf(peer);
            return java.util.stream.Stream.of(org.apache.ratis.proto.RaftProtos.RaftPeerRole.FOLLOWER, org.apache.ratis.proto.RaftProtos.RaftPeerRole.LISTENER)
                    .anyMatch(role -> configuration.getCurrentPeers(role).stream().anyMatch(value -> value.getId().equals(id)) || configuration.getPreviousPeers(role).stream().anyMatch(value -> value.getId().equals(id)));
        } catch (IOException notInitialized) { return false; }
    }

    /** 本机管理端的追平探针也使用同一 TLS 身份，不另建明文客户端。 */
    private org.apache.ratis.conf.Parameters clientParameters() { return security == null ? new org.apache.ratis.conf.Parameters() : security.parameters(); }

    /** 精确核对本组投票元数据，拒绝借用别组文件或空格式化旧投票身份。 */
    private static boolean hasRaftMetadata(Path directory, RaftGroupId group, boolean bootstrap) throws IOException {
        Path groupDirectory = directory.resolve(group.getUuid().toString()); Path current = groupDirectory.resolve("current"); Path metadata = current.resolve("raft-meta");
        for (Path path : List.of(directory, groupDirectory, current, metadata)) {
            if (Files.isSymbolicLink(path)) { throw new IOException("Raft voting path contains symbolic link"); }
        }
        boolean found = Files.isRegularFile(metadata, java.nio.file.LinkOption.NOFOLLOW_LINKS);
        if (Files.exists(directory, java.nio.file.LinkOption.NOFOLLOW_LINKS)) {
            if (!Files.isDirectory(directory, java.nio.file.LinkOption.NOFOLLOW_LINKS)) { throw new IOException("Raft storage is not a directory"); }
            try (var entries = Files.list(directory)) {
                for (Path path : entries.toList()) {
                    if (!path.equals(groupDirectory) || !found && !bootstrap) { throw new IOException("Raft storage does not contain this group's voting metadata"); }
                }
            }
        }
        return found;
    }

    /** 只有尚未服务过的明确初始化操作可以重试；旧运行目录没有此标记仍严格拒绝空格式化。 */
    private static boolean validBootstrap(Path business, WalReplicaIdentity identity) throws IOException {
        Path marker = business.resolve("BOOTSTRAPPING"); if (!Files.exists(marker, java.nio.file.LinkOption.NOFOLLOW_LINKS)) { return false; }
        var record = RatisGenerationRecord.read(marker);
        if (!record.identity().equals(identity) || record.term() != -1 || Files.exists(business.resolve("INSTALLING"))) { throw new IOException("invalid bootstrap ownership or phase"); }
        if (Files.exists(business.resolve("ACTIVE")) && !record.equals(RatisGenerationRecord.read(business.resolve("ACTIVE")))) { throw new IOException("bootstrap active identity mismatch"); }
        return true;
    }

    /** 未完成初始化不能已有选举任期或投票，避免初始化重试被用来清除已参与共识的身份。 */
    private static void verifyInitialVote(Path metadata) throws IOException {
        if (!Files.isRegularFile(metadata, java.nio.file.LinkOption.NOFOLLOW_LINKS) || Files.size(metadata) > 65_536) { throw new IOException("bootstrap voting metadata invalid"); }
        var properties = new java.util.Properties(); try (var input = Files.newInputStream(metadata, java.nio.file.LinkOption.NOFOLLOW_LINKS)) { properties.load(input); }
        if (!"0".equals(properties.getProperty("term")) || !"".equals(properties.getProperty("votedFor"))) { throw new IOException("bootstrap node already participated in voting"); }
    }

    /** 配置可靠日志刷盘、读索引和有限缓冲，完整状态快照完成前关闭自动日志快照清理。 */
    private static RaftProperties properties(Path directory, String host, int port) {
        var props = new RaftProperties(); RaftServerConfigKeys.setStorageDir(props, List.of(directory.toFile()));
        RaftServerConfigKeys.Read.setOption(props, RaftServerConfigKeys.Read.Option.LINEARIZABLE);
        RaftServerConfigKeys.Log.setForceSyncNum(props, 1);
        RaftServerConfigKeys.Log.setUnsafeFlushEnabled(props, false);
        RaftServerConfigKeys.Log.setSegmentSizeMax(props, SizeInBytes.valueOf("8MB"));
        RaftServerConfigKeys.Log.setWriteBufferSize(props, SizeInBytes.valueOf("32MB"));
        RaftServerConfigKeys.Log.setQueueElementLimit(props, 64);
        RaftServerConfigKeys.Log.setQueueByteLimit(props, SizeInBytes.valueOf("32MB"));
        RaftServerConfigKeys.Log.Appender.setBufferByteLimit(props, SizeInBytes.valueOf("16MB"));
        RaftServerConfigKeys.Snapshot.setAutoTriggerEnabled(props, false);
        RaftServerConfigKeys.Rpc.setTimeoutMin(props, TimeDuration.valueOf(1000, TimeUnit.MILLISECONDS));
        RaftServerConfigKeys.Rpc.setTimeoutMax(props, TimeDuration.valueOf(2000, TimeUnit.MILLISECONDS));
        GrpcConfigKeys.Server.setHost(props, host); GrpcConfigKeys.Server.setPort(props, port);
        GrpcConfigKeys.Server.setBossGroupSize(props, 1); GrpcConfigKeys.Server.setWorkerGroupSize(props, 2);
        GrpcConfigKeys.Server.setAsyncRequestThreadPoolCached(props, false); GrpcConfigKeys.Server.setAsyncRequestThreadPoolSize(props, 4);
        GrpcConfigKeys.Server.setLeaderOutstandingAppendsMax(props, 8); GrpcConfigKeys.setMessageSizeMax(props, SizeInBytes.valueOf("32MB"));
        return props;
    }

    /** 认证传输的当前量、峰值和拒绝计数，不包含证书、网络地址或业务内容。 */
    public record TransportStats(int connections, int peakConnections, long rejectedConnections, int calls, int peakCalls, long rejectedCalls) { }

    /** 读取连接预算无需网络 I/O，明文验收入口的此组计数保持零。 */
    public TransportStats transportStats() { return transport.stats(); }

    /** 本地角色仅用于发现，读写仍必须经过共识端口的任期和屏障检查。 */
    public long leaderEpoch() { return authority.epoch(); }

    /** 开始事务的快照建立在多数派读屏障之后。 */
    public WalTransaction begin(IsolationLevel isolation) { return business().begin(isolation); }

    /** 托管会话固定最初拥有该事务的内核，关闭只清理它的未提交本地写集。 */
    RatisTransaction beginManagedTransaction(IsolationLevel isolation) { var original = business(); return new RatisTransaction(this, original, original.begin(isolation)); }

    /** 近期多数派证明仅由托管观察线程调用，不创建或迁移业务事务。 */
    long probeManagedLeader(long timeoutMillis) throws Exception { if (!ready()) { throw new IOException("managed replica not ready"); } return authority.readBarrier(timeoutMillis); }

    /** 返回固定代次的统计样本，不把监控查询转成业务数据查询。 */
    RatisWalState.LocalSample managedSample() { return state.localSample(); }

    /** 每条语句使用同一经过屏障的框架快照。 */
    public WalReadSnapshot beginReadStatement(WalTransaction tx) { return business().beginReadStatement(tx); }

    /** 查询本事务和一致快照可见版本，不能绕过角色校验直接暴露内核。 */
    public Optional<byte[]> get(WalTransaction tx, String shard, String key, WalReadSnapshot snapshot) { return business().get(tx, shard, key, snapshot); }

    /** 本地缓冲写集，副本之间尚无业务修改。 */
    public void put(WalTransaction tx, String shard, String key, byte[] value) { business().put(tx, shard, key, value); }

    /** 缓冲墓碑，只有共识提交后才可见。 */
    public void delete(WalTransaction tx, String shard, String key) { business().delete(tx, shard, key); }

    /** 等待多数派和本地完整安装，超时仍按 UNKNOWN 处理。 */
    public WalReceipt commit(WalTransaction tx) { return business().commit(tx); }

    /** 仅清理尚未进入决议的本地事务。 */
    public void rollback(WalTransaction tx) { business().rollback(tx); }

    /** 事务结果查询也经过多数派屏障，缺失不证明回滚。 */
    public WalCommitLookup lookupCommit(UUID transaction) { return business().lookupCommit(transaction); }

    /** 返回节点本地已应用的业务边界供追平观测，不能代替线性化读取。 */
    public long appliedSequence() { return state.core().stats().publishedSequence(); }

    /** 本地实际 Raft 应用位置供追赶观测，与业务提交序号不同，不代替多数派读屏障。 */
    public long appliedRaftIndex() { var position = machine.getLastAppliedTermIndex(); return position == null ? -1 : position.getIndex(); }

    /** 节点本地观测不构成线性化业务查询，供维护和追平检查使用。 */
    public WalMvccRuntime.Stats localStats() { return state.core().stats(); }

    /** 只截断已物化的本地 WAL，不能据此清理 Raft 日志或宣称完整集群快照。 */
    public WalMvccRuntime.CheckpointResult checkpointLocal() { return business().checkpoint(); }

    /** 就绪表示完整代次已打开，角色仍须通过多数派屏障确认。 */
    public boolean ready() { return serving && !healthQuarantined && !snapshotLogPressure && state.ready() && state.coreHealthy() && !machine.failed() && (security == null || security.localIdentityValid()); }

    /** 在实际应用队列中创建完整镜像，当前不自动清理复制日志。 */
    public long snapshot() throws IOException { return machine.takeSnapshot(); }

    /** 本地已验证快照的位置供追赶观测，不代替业务读屏障。 */
    public long snapshotIndex() { var snapshot = machine.getLatestSnapshot(); return snapshot == null ? -1 : snapshot.getIndex(); }

    /** 健康控制的缓存状态，不在监控读取中触发数据库 I/O。 */
    public record HealthStats(boolean enabled, boolean quarantined, boolean electionPaused, String state, int failures, int successes,
                              long rounds, long isolations, long transfers, long recoveries, String error) {
        /** 有效竞选资格门控；旧 electionPaused 字段保留兼容，但不再调用有候选者恢复缺口的官方 Pause。 */
        public boolean campaignBlocked() { return electionPaused; }
    }

    /** 持久启用健康策略，重启会自动恢复管理并先关闭竞选和业务资格。 */
    public void enableHealthManagement(RatisHealthOptions options) throws IOException {
        java.util.Objects.requireNonNull(options); closeGuard.lock();
        try {
            if (closed || !state.supportsSnapshots()) { throw new IOException("health management requires an open generational node"); }
            if (health != null) { if (!health.options().equals(options)) { throw new IOException("health management already configured differently"); } return; }
            new RatisHealthRecord(identity, options).publish(nodeDirectory); startHealth(options);
        } finally { closeGuard.unlock(); }
    }

    /** 已持久策略和首次启用使用相同任务，不能在重启时遗漏健康配置。 */
    private void startHealth(RatisHealthOptions options) { machine.configureProbeBudget(options.probeMillis()); health = new RatisHealthController(this, options); health.start(); }

    /** 返回脱敏统计，未启用时保持原装配语义。 */
    public HealthStats healthStats() { var current = health; return current == null ? new HealthStats(false, healthQuarantined, healthCampaignBlocked, "DISABLED", 0, 0, 0, 0, 0, 0, null) : current.stats(); }

    /** 安装和备份期间不把暂停误计为数据库故障。 */
    enum HealthVerdict { BUSY, GOOD, BAD }

    /** 固定内核代次执行实际后端检查，返回前再验证代次未切换。 */
    HealthVerdict healthProbe(RatisHealthOptions options) {
        if (!serving || !state.ready()) { return HealthVerdict.BUSY; }
        if (snapshotLogPressure) { return HealthVerdict.BAD; }
        var core = state.core(); var status = core.lifecycle(); if (status.backup()) { return HealthVerdict.BUSY; }
        if (machine.failed() || status.fatal() != null || status.closed() || !status.accepting()) { return HealthVerdict.BAD; }
        boolean backend = state.backendHealthy(core, options.probeMillis());
        if (!state.ready() || state.core() != core) { return HealthVerdict.BUSY; }
        var after = core.lifecycle(); return backend && after.fatal() == null && after.materializationFailure() == null && !after.proposalBlocked() ? HealthVerdict.GOOD : HealthVerdict.BAD;
    }

    /** 立即关闭业务资格，再异步退休旧句柄；不等待数据库或共识锁。 */
    void healthIsolate() {
        healthQuarantined = true; healthCampaignBlocked = true;
        try { long term = server.getDivision(group.getGroupId()).getInfo().getCurrentTerm(); healthRetiredTerm = Math.max(healthRetiredTerm, term); if (term > 0) { authority.obsoleteEpoch.accumulateAndGet(term, Math::max); authority.scheduleFence(); } }
        catch (IOException stopped) { /* 已停止的分区没有业务资格。 */ }
    }

    /** 隔离状态供控制任务和缓存探针读取。 */
    boolean healthQuarantined() { return healthQuarantined; }
    /** 应用层门控阻止发起投票及强制竞选，保留正常入站投票、复制和原生选举循环。 */
    boolean healthCampaignBlocked() { return healthCampaignBlocked; }

    /** 竞选发送及远端强制竞选都经过同一门控，拒绝计数可用于验证原生选举循环没有停死。 */
    private boolean campaignAllowed() {
        boolean allowed = !healthQuarantined && !healthCampaignBlocked && !snapshotLogPressure && state.ready() && state.coreHealthy() && !machine.failed();
        if (!allowed) { campaignDenials.incrementAndGet(); } return allowed;
    }

    /** 包内测试观察实际投票发送被拒绝，不靠固定睡眠推断候选阶段。 */
    long campaignDenialsForTest() { return campaignDenials.get(); }

    /** 目标健康时受控移交，无法证明健康或多数派时维持隔离并请求官方退位。 */
    boolean healthMaintainIsolation(RatisHealthOptions options) throws Exception {
        long until = deadline(options.transferMillis()); healthCampaignBlocked = true;
        if (!server.getDivision(group.getGroupId()).getInfo().isLeader() || !membershipGuard.tryLock()) { return false; }
        try {
            try {
                var current = membershipAt(until, true);
                var minimum = RatisReplicaStatus.decode(control(() -> machine.query(Message.valueOf(ByteString.copyFrom(RatisReplicaStatus.request())))).thenCompose(value -> value).get(remaining(until), TimeUnit.MILLISECONDS).getContent().toByteArray());
                for (var target : current.voters()) {
                    if (target.getId().toString().equals(identity.nodeId()) || !awaitHealthyPeer(currentGroup(current), target.getId(), minimum, until)) { continue; }
                    var request = new TransferLeadershipRequest(membershipClient, RaftPeerId.valueOf(identity.nodeId()), group.getGroupId(), membershipCalls.incrementAndGet(), target.getId(), remaining(until));
                    var reply = control(() -> server.transferLeadershipAsync(request)).thenCompose(value -> value).get(remaining(until), TimeUnit.MILLISECONDS);
                    if (reply.isSuccess()) { return true; }
                }
            } catch (Exception unavailable) { /* 已撤销业务资格，未证明的目标不能通过管理转移强行提升。 */ }
            // 退位由官方协议执行；剩余健康节点仍须依法选举，并不假造目标可用。
            if (server.getDivision(group.getGroupId()).getInfo().isLeader()) {
                var request = new TransferLeadershipRequest(membershipClient, RaftPeerId.valueOf(identity.nodeId()), group.getGroupId(), membershipCalls.incrementAndGet(), null, 1000);
                control(() -> server.transferLeadershipAsync(request)).thenCompose(value -> value).get(1000, TimeUnit.MILLISECONDS);
            }
            return false;
        } finally { membershipGuard.unlock(); }
    }

    /** 原主同一任期不能重新使用已退休提案；跟随者需实际应用选定已提交边界。 */
    boolean healthCanRecover() throws Exception {
        if (!state.ready() || !state.coreHealthy() || machine.failed()) { return false; }
        var division = server.getDivision(group.getGroupId()); var info = division.getInfo();
        if (info.isLeader() && info.getCurrentTerm() <= healthRetiredTerm) { return false; }
        long required = division.getRaftLog().getLastCommittedIndex();
        var status = RatisReplicaStatus.decode(machine.queryStale(Message.valueOf(ByteString.copyFrom(RatisReplicaStatus.request())), required).get(1000, TimeUnit.MILLISECONDS).getContent().toByteArray());
        return status.appliedIndex() >= required;
    }

    /** 连续健康和应用边界均满足以后，恢复竞选，再开放业务资格。 */
    void healthResume(RatisHealthOptions options) { healthCampaignBlocked = false; healthQuarantined = false; }

    /** 自动镜像与日志回收的缓存状态，水位是协作背压阈值。 */
    public record SnapshotStats(boolean enabled, String state, long raftLogBytes, long firstLogIndex, long snapshotIndex, long rounds,
                                long snapshots, long purges, boolean exporting, boolean backpressure, String error) { }

    /** 持久启用自动镜像和回收，健康隔离用于保护日志高水位后的事务任期。 */
    public void enableSnapshotMaintenance(RatisSnapshotOptions options) throws IOException {
        java.util.Objects.requireNonNull(options); closeGuard.lock();
        try {
            if (closed || !state.supportsSnapshots() || !state.supportsRetirement() || !state.supportsPinnedSnapshots()) { throw new IOException("snapshot lifecycle requires bounded backend retirement and pinned views"); }
            if (snapshotController != null) { if (!snapshotController.options().equals(options)) { throw new IOException("snapshot lifecycle already configured differently"); } return; }
            state.requirePinnedSnapshots();
            if (health == null) { enableHealthManagement(RatisHealthOptions.defaults()); }
            new RatisSnapshotPolicy(identity, options).publish(nodeDirectory); startSnapshotLifecycle(options);
        } finally { closeGuard.unlock(); }
    }

    /** 重启自动恢复相同策略，不启用官方同步应用线程的自动导出路径。 */
    private void startSnapshotLifecycle(RatisSnapshotOptions options) { snapshotController = new RatisSnapshotController(this, options); snapshotController.start(); }

    /** 仅读缓存计数，未配置时保持原入口语义。 */
    public SnapshotStats snapshotStats() { var current = snapshotController; return current == null ? new SnapshotStats(false, "DISABLED", 0, -1, -1, 0, 0, 0, false, false, null) : current.stats(); }

    /** 版本回收指标为固定代次完成后的缓存，探针不会访问数据库。 */
    public record GcStats(boolean enabled, String state, long rounds, long failures, long inspectedKeys, long removedRows,
                          long safePoint, long publishedSequence, long retiredMemoryVersions, long durationMillis, long overruns,
                          long sampleAgeMillis, boolean stale, String error, UUID generation) { }

    /** 显式持久启用有界版本回收，既有未配置节点不被自动转换。 */
    public void enableVersionGc(RatisGcOptions options) throws IOException {
        java.util.Objects.requireNonNull(options); closeGuard.lock();
        try {
            if (closed || !state.supportsSnapshots()) { throw new IOException("version GC requires an open generational node"); }
            if (versionGc != null) { if (!versionGc.options().equals(options)) { throw new IOException("version GC already configured differently"); } return; }
            state.requireBoundedGc();
            if (health == null) { enableHealthManagement(RatisHealthOptions.defaults()); }
            new RatisGcPolicy(identity, options).publish(nodeDirectory); startVersionGc(options);
        } finally { closeGuard.unlock(); }
    }

    /** 首次装配与重启策略恢复共用唯一回收工作者。 */
    private void startVersionGc(RatisGcOptions options) { versionGc = new RatisGcController(this, options); versionGc.start(); }

    /** 安装和应用失效时停止回收，隔离副本仍可清理自己的安全版本。 */
    RatisWalState.GcSample collectVersionGarbage(RatisGcOptions options) {
        if (!serving || !state.ready() || machine.failed()) { return null; }
        return state.collectVersionGarbage(options);
    }

    /** 返回缓存与采样年龄，维护错误或停滞不会被探针隐藏。 */
    public GcStats gcStats() { var current = versionGc; return current == null ? new GcStats(false, "DISABLED", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, false, null, null) : current.stats(); }

    /** 日志背压不会挡住完成镜像所需的复制与本机维护。 */
    boolean snapshotMaintenanceReady() { return serving && state.ready() && state.coreHealthy() && !machine.failed() && (security == null || security.localIdentityValid()); }
    /** 背压只关闭新业务入口，已接受决议继续遵守原提交合同。 */
    void snapshotLogPressure(boolean pressure) { snapshotLogPressure = pressure; }
    /** 当前水位状态供健康控制和缓存监控读取。 */
    boolean snapshotLogPressure() { return snapshotLogPressure; }
    /** 镜像任务属于状态机，调度器只保留一个观察句柄。 */
    CompletableFuture<Long> requestSnapshotImage() throws IOException { return machine.requestSnapshot(); }

    /** 有界统计本机 Raft 段文件，不扫描业务数据库或整个工作区。 */
    long measureRaftLogBytes() throws IOException {
        Path current = nodeDirectory.resolve("raft").resolve(group.getGroupId().getUuid().toString()).resolve("current"); long total = 0; int inspected = 0;
        try (var files = java.nio.file.Files.list(current)) { var iterator = files.iterator(); while (iterator.hasNext()) {
            if (++inspected > 4096) { throw new IOException("Raft log catalog exceeds maintenance budget"); } var file = iterator.next(); String name = file.getFileName().toString();
            if (!name.matches("log_(inprogress_[0-9]+|[0-9]+-[0-9]+)")) { continue; }
            if (!java.nio.file.Files.isRegularFile(file, java.nio.file.LinkOption.NOFOLLOW_LINKS)) { throw new IOException("Raft log segment is not regular"); }
            try { total = Math.addExact(total, java.nio.file.Files.size(file)); } catch (ArithmeticException invalid) { throw new IOException("Raft log byte counter overflow", invalid); }
        } }
        return total;
    }

    /** 每次删除只使用重新验证且已激活的完整快照边界，保留尾部和所有未覆盖记录。 */
    CompletableFuture<Long> purgeCoveredSnapshot(long snapshot, long preserve) throws IOException {
        if (!snapshotMaintenanceReady()) { throw new IOException("node not ready for snapshot purge"); }
        long complete = machine.durableSnapshotBoundary(snapshot); var division = server.getDivision(group.getGroupId());
        long limit = Math.min(Math.min(snapshot, complete) - preserve, Math.min(appliedRaftIndex(), division.getRaftLog().getLastCommittedIndex()));
        if (limit < 0) { return CompletableFuture.completedFuture(-1L); }
        state.lifecycleStage(RatisWalState.Stage.BEFORE_LOG_PURGE, division.getRaftLog().getStartIndex(), limit);
        return division.getRaftLog().purge(limit).thenApply(purged -> {
            try { state.lifecycleStage(RatisWalState.Stage.AFTER_LOG_PURGE, division.getRaftLog().getStartIndex(), limit); }
            catch (IOException failure) { throw new java.util.concurrent.CompletionException(failure); }
            return purged;
        });
    }

    /** 旧代次、旧快照及中断镜像暂存共用有限维护计数。 */
    void maintainSnapshotFiles() throws IOException {
        if (!snapshotMaintenanceReady()) { return; }
        machine.maintainFiles(new RatisRetirementWork(RatisRetirementOptions.defaults()));
    }

    /** 记录入站完整快照通过 ACK 前持久性检查的位置，供故障验收与监控核对。 */
    public long durableSnapshotAckIndex() { return machine.durableSnapshotAckIndex(); }

    /** 一轮真实回收计数；时间预算协作式，单次文件系统和驱动调用不能通过中断强行取消。 */
    public record RetirementProgress(int backendShards, int droppedSchemas, int removedEntries, int completedGenerations, boolean pending, boolean budgetExhausted) { }

    /** 有界回收旧后端、旧目录和旧快照，当前代次及未完成安装始终受保护。 */
    public RetirementProgress retireOldState(RatisRetirementOptions options) throws IOException {
        if (!ready() || !state.supportsSnapshots()) { throw new IOException("node not ready for generation maintenance"); }
        var work = new RatisRetirementWork(options); boolean maintained = machine.maintainFiles(work);
        return new RetirementProgress(work.inspected, work.dropped, work.removed, work.completed, !maintained || state.retirementPending(), work.exhausted);
    }

    /** 后台回收单独显式启用，不自动开启快照或共识日志清理。 */
    public void enableRetirementMaintenance(RatisRetirementOptions options, long intervalMillis) throws IOException {
        if (intervalMillis < 100 || intervalMillis > 60_000) { throw new IllegalArgumentException("retirement interval must be 100..60000ms"); }
        java.util.Objects.requireNonNull(options); closeGuard.lock();
        try {
            if (closed || !state.supportsSnapshots()) { throw new IOException("node cannot start retirement maintenance"); }
            if (retirementExecutor != null) { throw new IOException("retirement maintenance already configured"); }
            retirementExecutor = java.util.concurrent.Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().daemon().name("wal-retirement-" + identity.nodeId()).factory());
            retirementExecutor.scheduleWithFixedDelay(() -> {
                if (!ready()) { return; }
                try {
                    var progress = retireOldState(options); lastRetirement = progress; retirementRuns.incrementAndGet(); retiredGenerations.addAndGet(progress.completedGenerations()); retiredSchemas.addAndGet(progress.droppedSchemas()); retiredEntries.addAndGet(progress.removedEntries()); retirementFailure = null;
                } catch (Exception failure) { retirementFailure = failure.getClass().getSimpleName(); }
            }, intervalMillis, intervalMillis, TimeUnit.MILLISECONDS);
        } finally { closeGuard.unlock(); }
    }

    /** 观测只读取缓存计数，不在探针中执行数据库查询或等待维护锁。 */
    public record RetirementStats(long runs, long generations, long schemas, long entries, RetirementProgress last, String failure) { }

    /** 返回最近维护进度与脱敏异常类型。 */
    public RetirementStats retirementStats() { return new RetirementStats(retirementRuns.get(), retiredGenerations.get(), retiredSchemas.get(), retiredEntries.get(), lastRetirement, retirementFailure); }

    /** 操作结果区分仅已加入非投票副本、等待追平和已完成，不把超时解释成回滚。 */
    public enum ReplacementPhase { LEARNER_ADDED, WAITING_FOR_LEARNER, COMPLETE }

    /** 当前结果绑定已经过多数派屏障的稳定配置，操作者须保留原操作票据以续做。 */
    public record ReplacementProgress(ReplacementPhase phase, RatisMembershipView membership) { }

    /** 从实际提交并应用的配置读取成员，未稳定的联合配置不作为新操作起点。 */
    public RatisMembershipView membership(long timeoutMillis) throws IOException { return membershipAt(deadline(timeoutMillis)); }

    /** 生成新的 UUID 节点身份和固定原配置，复用操作票据不重新生成身份。 */
    public RatisMemberReplacement planReplacement(RaftPeerId removed, String address, long timeoutMillis) throws IOException {
        var original = membership(timeoutMillis); UUID operation = UUID.randomUUID();
        var peer = RaftPeer.newBuilder().setId("r-" + operation).setAddress(address).setStartupRole(org.apache.ratis.proto.RaftProtos.RaftPeerRole.LISTENER).build();
        return new RatisMemberReplacement(operation, identity.groupId(), original, removed, peer);
    }

    /** 第一阶段只加入非投票成员，旧的三个投票成员和多数派要求保持。 */
    public ReplacementProgress beginReplacement(RatisMemberReplacement plan, long timeoutMillis) throws IOException {
        long until = deadline(timeoutMillis); if (!membershipGuard.tryLock()) { throw new IOException("another local membership operation is running"); }
        try { return beginReplacementLocked(plan, until); } finally { membershipGuard.unlock(); }
    }

    /** 应用位置与内核健康经目标副本确认之后，才通过 CAS 及官方联合共识同时替换一票。 */
    public ReplacementProgress completeReplacement(RatisMemberReplacement plan, long timeoutMillis) throws IOException {
        long until = deadline(timeoutMillis); if (!membershipGuard.tryLock()) { throw new IOException("another local membership operation is running"); }
        try {
            var progress = beginReplacementLocked(plan, until); if (progress.phase() == ReplacementPhase.COMPLETE) { return progress; }
            control(() -> authority.readBarrier(remaining(until))).get(remaining(until), TimeUnit.MILLISECONDS);
            var status = RatisReplicaStatus.decode(control(() -> machine.query(Message.valueOf(ByteString.copyFrom(RatisReplicaStatus.request())))).thenCompose(value -> value).get(remaining(until), TimeUnit.MILLISECONDS).getContent().toByteArray());
            if (!status.healthy()) { throw new IOException("coordinator business kernel is not healthy"); }
            boolean caughtUp = awaitHealthyPeer(currentGroup(progress.membership()), plan.replacement().getId(), status, until);
            if (!caughtUp) { return new ReplacementProgress(ReplacementPhase.WAITING_FOR_LEARNER, membershipAt(until)); }
            var current = membershipAt(until); int phase = phase(plan, current);
            if (phase == 2) { return new ReplacementProgress(ReplacementPhase.COMPLETE, current); }
            if (phase != 1) { throw new IOException("replacement learner configuration no longer current"); }
            if (plan.removed().toString().equals(identity.nodeId())) { throw new IOException("transfer leadership to a retained voter before removing this node"); }
            changeMembership(current, plan.completed(current.index()), until);
            return new ReplacementProgress(ReplacementPhase.COMPLETE, membershipAt(until));
        } catch (InterruptedException failure) { Thread.currentThread().interrupt(); throw new IOException("replacement wait interrupted; retain ticket", failure); }
        catch (IOException failure) { throw failure; }
        catch (Exception failure) { throw new IOException("replacement progress unresolved; retain and retry the same ticket", failure); }
        finally { membershipGuard.unlock(); }
    }

    /** 管理切主只允许已追平的现有投票成员，认证节点不需要开放远程原始管理服务。 */
    public void transferLeadership(RaftPeerId target, long timeoutMillis) throws IOException {
        long until = deadline(timeoutMillis); if (!membershipGuard.tryLock()) { throw new IOException("another local management operation is running"); }
        try {
            var current = membershipAt(until);
            if (target.toString().equals(identity.nodeId()) || current.voters().stream().noneMatch(peer -> peer.getId().equals(target))) { throw new IOException("leadership target must be another current voter"); }
            var minimum = RatisReplicaStatus.decode(control(() -> machine.query(Message.valueOf(ByteString.copyFrom(RatisReplicaStatus.request())))).thenCompose(value -> value).get(remaining(until), TimeUnit.MILLISECONDS).getContent().toByteArray());
            if (!minimum.healthy() || !awaitHealthyPeer(currentGroup(current), target, minimum, until)) { throw new IOException("leadership target not caught up and healthy"); }
            var request = new TransferLeadershipRequest(membershipClient, RaftPeerId.valueOf(identity.nodeId()), group.getGroupId(), membershipCalls.incrementAndGet(), target, remaining(until));
            var reply = control(() -> server.transferLeadershipAsync(request)).thenCompose(value -> value).get(remaining(until), TimeUnit.MILLISECONDS);
            if (!reply.isSuccess()) { throw new IOException("leadership transfer rejected", reply.getException()); }
        } catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new IOException("leadership transfer interrupted; inspect actual leader", interrupted); }
        catch (IOException failure) { throw failure; }
        catch (Exception failure) { throw new IOException("leadership transfer unresolved; inspect actual leader", failure); }
        finally { membershipGuard.unlock(); }
    }

    /** 固定追平边界并复用认证客户端，轮询次数与整次管理操作共用期限。 */
    private boolean awaitHealthyPeer(RaftGroup current, RaftPeerId peer, RatisReplicaStatus minimum, long until) throws Exception {
        long queryMillis = Math.min(health == null ? 1000 : health.options().probeMillis() + 500, remaining(until)); var properties = new RaftProperties(); org.apache.ratis.client.RaftClientConfigKeys.Rpc.setRequestTimeout(properties, TimeDuration.valueOf(queryMillis, TimeUnit.MILLISECONDS));
        long grace = Math.min(until, System.nanoTime() + TimeUnit.SECONDS.toNanos(5));
        try (var client = org.apache.ratis.client.RaftClient.newBuilder().setRaftGroup(current).setLeaderId(RaftPeerId.valueOf(identity.nodeId())).setProperties(properties).setParameters(clientParameters()).setRetryPolicy(org.apache.ratis.retry.RetryPolicies.noRetry()).build()) {
            while (grace - System.nanoTime() >= TimeUnit.MILLISECONDS.toNanos(queryMillis)) {
                try {
                    var reply = client.io().sendStaleRead(Message.valueOf(ByteString.copyFrom(RatisReplicaStatus.request())), minimum.appliedIndex(), peer);
                    if (reply.isSuccess() && reply.getMessage() != null) {
                        var target = RatisReplicaStatus.decode(reply.getMessage().getContent().toByteArray());
                        if (target.group().equals(identity.groupId()) && target.node().equals(peer.toString()) && target.healthy()
                                && target.appliedIndex() >= minimum.appliedIndex() && target.publishedSequence() >= minimum.publishedSequence()) { return true; }
                    }
                } catch (IOException notReady) { /* 有界尝试尚未追平，保留当前角色及操作票据。 */ }
                TimeUnit.MILLISECONDS.sleep(30);
            }
        }
        return false;
    }

    /** 任意阶段续做只接受同一组以及原始、中间、最终三个精确配置。 */
    private ReplacementProgress beginReplacementLocked(RatisMemberReplacement plan, long until) throws IOException {
        if (!state.supportsSnapshots() || !plan.groupId().equals(identity.groupId())) { throw new IOException("replacement requires matching snapshot-capable group"); }
        if (security != null) { security.requirePeer(plan.replacement()); }
        else if (plan.joiningGroup().getPeers().stream().anyMatch(peer -> !peer.getAddress().matches("127\\.0\\.0\\.1:[0-9]{1,5}"))) { throw new IOException("plaintext member replacement restricted to loopback"); }
        var current = membershipAt(until); int phase = phase(plan, current);
        if (phase == 2) { return new ReplacementProgress(ReplacementPhase.COMPLETE, current); }
        if (phase == 0) { changeMembership(current, plan.staged(current.index()), until); current = membershipAt(until); }
        return new ReplacementProgress(ReplacementPhase.LEARNER_ADDED, current);
    }

    /** 本地角色改变或配置尚未提交应用时拒绝管理观察，不复用旧多数派确认。 */
    private RatisMembershipView membershipAt(long until) throws IOException { return membershipAt(until, false); }

    /** 健康路径观察配置也必须经过新鲜多数派栅栏。 */
    private RatisMembershipView membershipAt(long until, boolean management) throws IOException {
        try {
            long epoch = control(() -> authority.barrier(remaining(until), management)).get(remaining(until), TimeUnit.MILLISECONDS); var division = server.getDivision(group.getGroupId()); var configuration = division.getRaftConf();
            if (!configuration.getPreviousPeers().isEmpty() || !configuration.getPreviousPeers(org.apache.ratis.proto.RaftProtos.RaftPeerRole.LISTENER).isEmpty()
                    || configuration.getLogEntryIndex() > division.getRaftLog().getLastCommittedIndex() || configuration.getLogEntryIndex() > machine.getLastAppliedTermIndex().getIndex() || epoch != (management ? authority.consensusEpoch() : authority.epoch())) { throw new IOException("membership is not yet stable and applied"); }
            return new RatisMembershipView(configuration.getLogEntryIndex(), List.copyOf(configuration.getCurrentPeers()), List.copyOf(configuration.getCurrentPeers(org.apache.ratis.proto.RaftProtos.RaftPeerRole.LISTENER)));
        } catch (InterruptedException failure) { Thread.currentThread().interrupt(); throw new IOException("membership observation interrupted", failure); }
        catch (IOException failure) { throw failure; } catch (Exception failure) { throw new IOException("cannot confirm current membership", failure); }
    }

    /** CAS 防止过期管理员覆盖另一操作；超时不取消已经进入共识的配置请求。 */
    private void changeMembership(RatisMembershipView previous, RatisMembershipView next, long until) throws IOException {
        var arguments = new SetConfigurationRequest.Arguments.Builder().setMode(SetConfigurationRequest.Mode.COMPARE_AND_SET)
                .setServersInCurrentConf(previous.voters()).setListenersInCurrentConf(previous.learners()).setServersInNewConf(next.voters()).setListenersInNewConf(next.learners()).build();
        var request = new SetConfigurationRequest(membershipClient, RaftPeerId.valueOf(identity.nodeId()), group.getGroupId(), membershipCalls.incrementAndGet(), arguments);
        try {
            var future = control(() -> {
                    remaining(until);
                    var actual = server.getDivision(group.getGroupId()).getRaftConf();
                    if (authority.epoch() <= 0 || actual.getLogEntryIndex() != previous.index()
                            || !RatisMembershipView.same(List.copyOf(actual.getCurrentPeers()), previous.voters())
                            || !RatisMembershipView.same(List.copyOf(actual.getCurrentPeers(org.apache.ratis.proto.RaftProtos.RaftPeerRole.LISTENER)), previous.learners())) { throw new IOException("full membership changed before acceptance"); }
                    // 不持 Ratis 内部监视器调用异步入口；该入口内部会等待执行线程接受请求。
                    return server.setConfigurationAsync(request);
            }).thenCompose(value -> value).thenApplyAsync(reply -> {
                try { if (!reply.isSuccess()) { throw new IOException("membership request rejected", reply.getException()); } RaftDurability.current(server, group.getGroupId()); return reply; }
                catch (IOException failure) { throw new java.util.concurrent.CompletionException(failure); }
            }, membershipExecutor);
            future.get(remaining(until), TimeUnit.MILLISECONDS);
        } catch (InterruptedException failure) { Thread.currentThread().interrupt(); throw new IOException("membership wait interrupted; retain ticket", failure); }
        catch (Exception failure) { throw new IOException("membership outcome unresolved; inspect configuration and retry the same ticket", failure); }
    }

    /** 目标组取自实际配置而非初始静态节点列表。 */
    private RaftGroup currentGroup(RatisMembershipView view) { var peers = new java.util.ArrayList<>(view.voters()); peers.addAll(view.learners()); return RaftGroup.valueOf(group.getGroupId(), peers); }

    /** 配置不匹配是拒绝条件，不自动改写操作票据。 */
    private static int phase(RatisMemberReplacement plan, RatisMembershipView current) throws IOException { try { return plan.phase(current); } catch (IllegalStateException mismatch) { throw new IOException("membership no longer matches replacement ticket", mismatch); } }

    /** 管理操作使用单调总期限，不允许无限等待追平。 */
    private static long deadline(long timeoutMillis) { if (timeoutMillis < 1 || timeoutMillis > 120_000) { throw new IllegalArgumentException("membership timeout must be 1..120000ms"); } return System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis); }

    /** 每个阶段共用同一时间预算，过期不产生新的配置请求。 */
    private static long remaining(long deadline) throws IOException { long value = deadline - System.nanoTime(); if (value <= 0) { throw new IOException("membership operation timed out; retain ticket"); } return Math.max(1, TimeUnit.NANOSECONDS.toMillis(value)); }

    /** 连方法入口同步等待和文件同步也隔离到有限执行器，调用方总期限不依赖原生异步命名。 */
    private <T> CompletableFuture<T> control(Control<T> task) { return CompletableFuture.supplyAsync(() -> { try { return task.run(); } catch (Exception failure) { throw new java.util.concurrent.CompletionException(failure); } }, membershipExecutor); }

    /** 管理任务可传播 I/O 异常，超时只停止调用方等待，不中断已经进入共识的操作。 */
    @FunctionalInterface private interface Control<T> {
        /** 在本节点有限管理执行器内运行。 */
        T run() throws Exception;
    }

    /** 安装中或关闭时拒绝所有业务操作，不能回退到旧内核引用。 */
    private WalMvccRuntime business() {
        if (!ready()) { throw new WalMvccException(mvcc.common.ErrorCode.STORAGE_UNAVAILABLE, "replica business generation not ready", null, null); }
        return state.core();
    }

    /** 包内故障验收只允许在完整快照位置内清理，用于强制触发真实落后副本安装。 */
    long purgeForTest(long index) throws Exception {
        var snapshot = machine.getLatestSnapshot(); if (snapshot == null || index > snapshot.getIndex()) { throw new IOException("no durable snapshot covers purge"); }
        return server.getDivision(group.getGroupId()).getRaftLog().purge(index).get(30, TimeUnit.SECONDS);
    }

    /** 包内验证实际日志缺口，不能只用 purge 返回值推断副本必须安装快照。 */
    long firstLogIndexForTest() throws IOException { return server.getDivision(group.getGroupId()).getRaftLog().getStartIndex(); }

    /** 先停止入口和共识，再排空状态机后释放本地 WAL，不能提前关闭在用通道。 */
    @Override public void close() throws IOException {
        closeGuard.lock();
        try {
            if (closed) { return; } serving = false;
            stopMaintenance();
            try { long epoch = server.getDivision(group.getGroupId()).getInfo().getCurrentTerm(); if (epoch > 0) { state.fence(epoch); } }
            catch (IOException alreadyStopped) { /* 前次关闭已停止共识，但代次资源仍需继续安全关闭。 */ }
            server.close(); machine.close(); authority.closeControl();
            closeMembershipExecutor();
            state.close(); closed = true;
        } finally { closeGuard.unlock(); }
    }

    /** 当前领导者的唯一提案指纹，原始 Raft 客户端不能伪造未准备的业务批次。 */
    private record Pending(long epoch, String digest) { }

    /** 共识适配器的本地提交不自动重定向，防止旧任期已分配序号被重提。 */
    final class Authority implements WalCommitAuthority {
        final AtomicReference<RaftServer> server = new AtomicReference<>();
        final AtomicReference<Pending> pending = new AtomicReference<>();
        private final java.util.concurrent.ConcurrentHashMap<String, FencePermit> readFences = new java.util.concurrent.ConcurrentHashMap<>();
        private final java.util.concurrent.Semaphore readPermits = new java.util.concurrent.Semaphore(64);
        private final AtomicLong calls = new AtomicLong();
        private final ClientId client = ClientId.randomId();
        private final java.util.concurrent.ExecutorService control = java.util.concurrent.Executors.newSingleThreadExecutor(Thread.ofPlatform().name("wal-raft-control-" + identity.nodeId()).factory());
        private final AtomicLong obsoleteEpoch = new AtomicLong();
        private final java.util.concurrent.atomic.AtomicBoolean fencing = new java.util.concurrent.atomic.AtomicBoolean();

        /** 角色信息来自实际 Raft 分区，尚未启动或关闭时为零。 */
        @Override public long epoch() { return healthQuarantined ? 0 : consensusEpoch(); }

        /** 管理许可只观察实际共识资格，业务隔离不能把已登记的健康控制栅栏伪造成普通写入。 */
        private long consensusEpoch() {
            var current = server.get(); if (!serving || !state.ready() || !state.coreHealthy() || machine.failed() || current == null || security != null && !security.localIdentityValid()) { return 0; }
            try { var division = current.getDivision(group.getGroupId()); var info = division.getInfo(); return info.isLeaderReady() && division.getRaftConf().getCurrentPeers().stream().anyMatch(peer -> peer.getId().toString().equals(identity.nodeId())) ? info.getCurrentTerm() : 0; }
            catch (IOException failure) { return 0; }
        }

        /** 业务栅栏需要本机业务资格，故障后不能回退到管理许可。 */
        @Override public long readBarrier(long timeoutMillis) throws Exception { return barrier(timeoutMillis, false); }

        /** 健康隔离路径使用单独注册的许可，读写用户仍被隔离。 */
        private long barrier(long timeoutMillis, boolean management) throws Exception {
            long term = management ? consensusEpoch() : epoch(); if (term <= 0) { throw new IOException("not leader"); }
            if (!readPermits.tryAcquire()) { throw new IOException("read fence queue full"); }
            String hash = null;
            try {
                byte[] data = new RatisReadFence(term, identity, calls.incrementAndGet()).encode(); hash = digest(data); readFences.put(hash, new FencePermit(term, management));
                var reply = server.get().submitClientRequestAsync(request(Message.valueOf(ByteString.copyFrom(data)), false, timeoutMillis)).get(timeoutMillis, TimeUnit.MILLISECONDS);
                check(reply); if (term != (management ? consensusEpoch() : epoch())) { throw new IOException("leader epoch changed during read barrier"); } return term;
            } finally { if (hash != null) { readFences.remove(hash); } readPermits.release(); }
        }

        /** 许可类型仅在本机登记，不来自 RPC 载荷或客户端声明。 */
        private record FencePermit(long epoch, boolean management) { }

        /** 未来只在 Raft 多数派提交和状态机应用后成功，RPC 期限不取消日志决议。 */
        @Override public CompletionStage<Void> submit(long term, List<WalCommit> commits) {
            try {
                if (term != epoch()) { throw new IOException("proposal leader changed"); }
                var batch = new WalReplicationBatch(term, identity, commits); byte[] data = batch.encode();
                var expected = new Pending(term, batch.digest());
                if (!pending.compareAndSet(null, expected)) { throw new IOException("previous proposal unresolved"); }
                var future = server.get().submitClientRequestAsync(request(Message.valueOf(ByteString.copyFrom(data)), false, 3000))
                        .thenApply(reply -> { check(reply); return (Void) null; }).orTimeout(3000, TimeUnit.MILLISECONDS);
                future.whenComplete((ignored, failure) -> pending.compareAndSet(expected, null)); return future;
            } catch (Exception failure) { return CompletableFuture.failedFuture(failure); }
        }

        /** 串行入日志时再次校验当前角色、任期与本地准备的原始提案。 */
        void validate(WalReplicationBatch batch) throws IOException {
            var expected = pending.get();
            if (!batch.origin().equals(identity) || batch.epoch() != epoch() || expected == null
                    || expected.epoch() != batch.epoch() || !expected.digest().equals(batch.digest())) { throw new IOException("unprepared or fenced proposal"); }
        }

        /** 读栅栏同样由本地注册并校验任期，任意网络客户端不能伪造源身份。 */
        void validate(RatisReadFence fence, byte[] bytes) throws IOException {
            var permit = readFences.get(digest(bytes));
            if (!fence.origin().equals(identity) || permit == null || permit.epoch() != fence.epoch() || fence.epoch() != (permit.management() ? consensusEpoch() : epoch())) {
                throw new IOException("unregistered or fenced read barrier");
            }
        }

        /** 固定摘要不包含用户凭据，不依赖概率过滤器。 */
        private String digest(byte[] bytes) throws IOException {
            try { return java.util.HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(bytes)); }
            catch (java.security.NoSuchAlgorithmException failure) { throw new IOException(failure); }
        }

        /** 只清理旧任期句柄，延迟通知不能清除当前新任期请求。 */
        void lostLeadership() {
            try {
                var info = server.get().getDivision(group.getGroupId()).getInfo(); long term = info.getCurrentTerm();
                long obsolete = info.isLeaderReady() ? term - 1 : term;
                if (obsolete > 0) { obsoleteEpoch.accumulateAndGet(obsolete, Math::max); scheduleFence(); }
            } catch (IOException ignored) { /* 关闭中的分区不再服务请求。 */ }
        }

        /** 角色回调不能在 Raft 事件线程等待数据库锁，最多调度一个合并后的控制任务。 */
        private void scheduleFence() {
            if (!fencing.compareAndSet(false, true)) { return; }
            control.execute(() -> {
                long handled = 0;
                try {
                    do {
                        handled = obsoleteEpoch.get(); state.fence(handled);
                        var old = pending.get(); if (old != null && old.epoch() <= handled) { pending.compareAndSet(old, null); }
                    } while (obsoleteEpoch.get() > handled);
                } finally { fencing.set(false); if (obsoleteEpoch.get() > handled) { scheduleFence(); } }
            });
        }

        /** Raft 已停止产生事件后排空控制者，不中断在途引擎操作。 */
        void closeControl() throws IOException {
            control.shutdown();
            try { if (!control.awaitTermination(30, TimeUnit.SECONDS)) { throw new IOException("replica control did not drain"); } }
            catch (InterruptedException failure) { Thread.currentThread().interrupt(); throw new IOException(failure); }
        }

        /** 构造只投递本地节点的请求，默认库客户端的领导者重试不参与本链路。 */
        private RaftClientRequest request(Message message, boolean read, long timeout) {
            return RaftClientRequest.newBuilder().setClientId(client).setServerId(RaftPeerId.valueOf(identity.nodeId())).setGroupId(group.getGroupId())
                    .setCallId(calls.incrementAndGet()).setMessage(message).setType(read ? RaftClientRequest.readRequestType() : RaftClientRequest.writeRequestType()).setTimeoutMs(timeout).build();
        }

        /** 明确的协议异常传播为请求失败，不把异常响应当作共识成功。 */
        private static void check(RaftClientReply reply) {
            if (!reply.isSuccess()) { throw new java.util.concurrent.CompletionException(reply.getException() == null ? new IOException("Raft request unsuccessful") : reply.getException()); }
        }
    }
}
