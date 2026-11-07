package com.flyingzc.azir.wal.ha;

import com.zaxxer.hikari.HikariDataSource;
import com.flyingzc.azir.common.IsolationLevel;
import com.flyingzc.azir.wal.*;
import org.apache.ratis.protocol.*;
import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;

/** 显式认证 MySQL 装配与缓存运行状态，不改变原有 Azir 或未托管的节点入口。 */
public final class RatisManagedNode implements AutoCloseable {
    /** 维护预算及多数派探测期限，状态过期不能继续宣称可写。 */
    public record Maintenance(RatisHealthOptions health, RatisGcOptions gc, RatisSnapshotOptions snapshot, long sampleMillis, long staleMillis, long probeMillis) {
        /** 采样是单工作者固定延迟，探测不通过中断取消 WAL。 */
        public Maintenance { Objects.requireNonNull(health); Objects.requireNonNull(gc); Objects.requireNonNull(snapshot); if (sampleMillis < 100 || sampleMillis > 60_000 || probeMillis < 1 || probeMillis > 5000 || staleMillis <= sampleMillis + probeMillis || staleMillis > 86_400_000) { throw new IllegalArgumentException("invalid managed maintenance observation budget"); } }
        /** 默认每秒采样，多数派探测一秒，十五秒过期。 */
        public static Maintenance defaults() { return new Maintenance(RatisHealthOptions.defaults(), RatisGcOptions.defaults(), RatisSnapshotOptions.defaults(), 1000, 15_000, 1000); }
    }
    /** 调用方明确提供目录、三票配置、控制池与 TLS 身份，池始终属于调用方。 */
    public record Config(Path directory, RaftPeerId node, RaftGroup group, Map<String, HikariDataSource> controls, RatisTlsSecurity security,
                         WalOptions transactions, WalBackupOptions backups, MysqlWalReplicaBackendFactory.Options namespaces, RatisTransportLimits transport, Maintenance maintenance) {
        /** 防御复制配置，拒绝缺失路由及不受支持的角色布局。 */
        public Config {
            directory = Objects.requireNonNull(directory).toAbsolutePath().normalize(); Objects.requireNonNull(node); Objects.requireNonNull(group); controls = Map.copyOf(controls); Objects.requireNonNull(security); Objects.requireNonNull(transactions); Objects.requireNonNull(backups); Objects.requireNonNull(namespaces); Objects.requireNonNull(transport); Objects.requireNonNull(maintenance);
            if (directory.getParent() == null || controls.isEmpty() || controls.size() > 1024 || group.getPeer(node) == null || group.getPeers().size() > 9
                    || group.getPeers().stream().filter(peer -> peer.getStartupRole() == RaftPeerRole.FOLLOWER).count() != 3
                    || group.getPeers().stream().anyMatch(peer -> peer.getStartupRole() != RaftPeerRole.FOLLOWER && peer.getStartupRole() != RaftPeerRole.LISTENER)) { throw new IllegalArgumentException("managed node requires three voters and explicit bounded MySQL routes"); }
            var endpoints = new HashSet<String>(); var identities = new HashSet<RaftPeerId>();
            for (var peer : group.getPeers()) { RatisMembershipView.validate(peer); if (!endpoints.add(peer.getAddress()) || !identities.add(peer.getId())) { throw new IllegalArgumentException("duplicate managed peer identity or address"); } }
            for (var shard : controls.keySet()) { new WalMutation(shard, "", null); if (shard.contains("\u0000")) { throw new IllegalArgumentException("invalid managed shard"); } }
        }
        /** 常用默认预算，数据源和 TLS 不能默认猜测。 */
        public static Config defaults(Path directory, RaftPeerId node, RaftGroup group, Map<String, HikariDataSource> controls, RatisTlsSecurity security) { return new Config(directory, node, group, controls, security, WalOptions.defaults(), WalBackupOptions.defaults(), MysqlWalReplicaBackendFactory.Options.defaults(), RatisTransportLimits.defaults(), Maintenance.defaults()); }
        /** 不调用外部数据源或密钥对象的 toString，避免日志输出连接信息。 */
        @Override public String toString() { return "ManagedConfig[node=" + node + ", group=" + group.getGroupId() + ", directory=" + directory + ", shards=" + controls.keySet() + ", credentials=redacted]"; }
    }
    /** 缓存样本对应固定内核代次，近期多数派证明只用于路由提示，实际事务仍执行读屏障。 */
    public record Status(boolean localReady, boolean writableHint, long verifiedEpoch, UUID generation, WalMvccRuntime.Stats storage,
                         RatisWalNode.HealthStats health, RatisWalNode.GcStats gc, RatisWalNode.SnapshotStats snapshot, long ageMillis, boolean stale, String error) { }
    /** 自动事务回调只暴露业务操作，不提供手动提交或回滚。 */
    @FunctionalInterface public interface Work<T> {
        /** 回调只做本事务内操作，外部副作用需另用业务幂等/消息方案。 */
        T run(RatisTransaction.Operations transaction) throws Exception;
    }
    /** 仅在实际确认提交以后返回业务值和凭据。 */
    public record Execution<T>(T value, WalReceipt receipt) {
        /** 业务值可为空，但成功结果始终携带内核返回的完成凭据。 */
        public Execution { Objects.requireNonNull(receipt); }
        /** 只读事务完成不写持久决议，空摘要凭据不能承诺跨重启查询该新事务号。 */
        public boolean durableDecision() { return !receipt.digest().isEmpty(); }
    }
    /** 启动失败且清理尚未完成时保留资源句柄，调用方必须重试清理或终止所属进程。 */
    public static final class StartupException extends RatisStartupException {
        /** 只保留清理能力，不开放未完成启动节点的业务入口。 */
        StartupException(Throwable failure, java.io.Closeable pending) { super(failure, pending); }
    }
    private final RatisWalNode node;
    private final Config config;
    private final ScheduledExecutorService observer;
    private volatile RatisTimedSample<Status> sampled;
    private volatile boolean closing, closed;
    private final java.util.concurrent.locks.ReentrantLock closeGuard = new java.util.concurrent.locks.ReentrantLock();

    /** 在创建后端空间和启动 Raft 之前完成角色、身份和持久维护参数校验。 */
    public static RatisManagedNode open(Config config) throws IOException { return open(config, stage -> { }); }
    /** 包内边界测试复用完整正式装配，公开入口始终使用空钩子。 */
    static RatisManagedNode open(Config config, RatisWalState.Hook hook) throws IOException {
        Objects.requireNonNull(config); preflight(config); RatisWalNode node = null;
        try {
            var factory = new MysqlWalReplicaBackendFactory(config.controls(), config.namespaces());
            node = RatisWalNode.openAuthenticatedForTest(config.directory(), config.node(), config.group(), config.controls().keySet(), factory, config.transactions(), config.backups(), config.security(), config.transport(), hook);
            node.enableHealthManagement(config.maintenance().health()); node.enableVersionGc(config.maintenance().gc()); node.enableSnapshotMaintenance(config.maintenance().snapshot());
            return new RatisManagedNode(node, config);
        } catch (Throwable failure) {
            if (node != null) { try { node.close(); } catch (Throwable cleanup) { var pending = new StartupException(failure, node::close); pending.addSuppressed(cleanup); throw pending; } }
            if (failure instanceof RatisStartupException pending && pending.cleanupPending()) { throw new StartupException(failure, pending::retryCleanup); }
            if (failure instanceof IOException io) { throw io; } if (failure instanceof RuntimeException runtime) { throw runtime; } if (failure instanceof Error error) { throw error; } throw new IOException("managed startup failed", failure);
        }
    }
    /** 持久策略不可因调用方遗漏或改成不同默认值而被悄悄替换。 */
    private static void preflight(Config config) throws IOException {
        if (Files.exists(config.directory(), LinkOption.NOFOLLOW_LINKS) && !Files.isDirectory(config.directory(), LinkOption.NOFOLLOW_LINKS)) { throw new IOException("managed directory is not a private directory"); }
        config.security().validate(config.group(), config.node()); var id = new WalReplicaIdentity(config.group().getGroupId().getUuid(), config.group().getGroupId().getUuid(), config.node().toString());
        RatisTransportIdentity.verify(config.directory(), id, true); var health = RatisHealthRecord.load(config.directory(), id); var gc = RatisGcPolicy.load(config.directory(), id); var snapshot = RatisSnapshotPolicy.load(config.directory(), id);
        if (health != null && !health.options().equals(config.maintenance().health()) || gc != null && !gc.options().equals(config.maintenance().gc()) || snapshot != null && !snapshot.options().equals(config.maintenance().snapshot())) { throw new IOException("managed settings differ from persistent maintenance policy"); }
        if ((gc != null || snapshot != null) && health == null) { throw new IOException("managed maintenance policy dependency missing"); }
    }
    /** 包内测试与正式构造共用生命周期，只有 open 会装配生产后端。 */
    RatisManagedNode(RatisWalNode node, Config config) {
        this.node = node; this.config = config; sampled = RatisTimedSample.initial(new Status(false, false, 0, null, null, node.healthStats(), node.gcStats(), node.snapshotStats(), 0, true, null));
        observer = Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().daemon().name("wal-managed-status-" + config.node()).factory());
        observer.scheduleWithFixedDelay(this::sample, 0, config.maintenance().sampleMillis(), TimeUnit.MILLISECONDS);
    }
    /** 多数派验证和统计采样只在独立线程执行，失败只输出脱敏异常类型。 */
    private void sample() {
        // 新鲜度包含多数派探测和指标采样的耗时，不能在慢采样结束后重新计算有效期。
        long began = System.nanoTime();
        try {
            if (closing) { return; } long epoch = 0; String error = null;
            if (node.ready() && node.leaderEpoch() > 0) { try { epoch = node.probeManagedLeader(config.maintenance().probeMillis()); } catch (Exception failure) { error = failure.getClass().getSimpleName(); } }
            var local = node.managedSample(); var observation = sampled; var previous = observation.value();
            // 代次忙时保留旧采样时刻，不能用新的角色探测给旧存储计数续鲜。
            if (local == null) { sampled = observation.retaining(new Status(node.ready(), false, 0, previous.generation(), previous.storage(), node.healthStats(), node.gcStats(), node.snapshotStats(), 0, previous.stale(), "StorageSampleBusy")); return; }
            sampled = RatisTimedSample.captured(new Status(node.ready(), epoch > 0 && epoch == node.leaderEpoch(), epoch, local.generation(), local.stats(), node.healthStats(), node.gcStats(), node.snapshotStats(), 0, false, error), began);
        } catch (Exception failure) {
            var observation = sampled; var previous = observation.value(); sampled = observation.retaining(new Status(false, false, 0, previous.generation(), previous.storage(), node.healthStats(), node.gcStats(), node.snapshotStats(), 0, previous.stale(), failure.getClass().getSimpleName()));
        }
    }
    /** 无阻塞读取缓存与实时角色门控，过期样本不能继续声称可写。 */
    public Status status() {
        var observation = sampled; var value = observation.value(); long age = observation.ageMillis(System.nanoTime()); boolean stale = age > config.maintenance().staleMillis();
        return new Status(!closing && node.ready(), !closing && !stale && value.writableHint() && node.ready() && value.verifiedEpoch() == node.leaderEpoch(), value.verifiedEpoch(), value.generation(), value.storage(), value.health(), value.gc(), value.snapshot(), age, stale, value.error());
    }
    /** 手动事务持有原内核归属，开始仍使用真实多数派屏障。 */
    public RatisTransaction begin(IsolationLevel isolation) { if (closing) { throw new IllegalStateException("managed node closing"); } return node.beginManagedTransaction(isolation); }
    /** 不自动重跑业务回调，未知提交带原 UUID 传播，资源关闭不假回滚。 */
    public <T> Execution<T> execute(IsolationLevel isolation, Work<T> work) throws Exception {
        Objects.requireNonNull(work); try (var transaction = begin(isolation)) { T result = work.run(transaction.operations()); return new Execution<>(result, transaction.commit()); }
    }
    /** 只确认 COMMITTED，缺失或过期结果继续遵守 UNKNOWN 合同。 */
    public WalCommitLookup lookupCommit(UUID transaction) { if (closing) { throw new IllegalStateException("managed node closing"); } return node.lookupCommit(transaction); }
    /** 规划后先持久保存，再将票据交给操作者执行；保存失败不修改成员。 */
    public RatisMemberReplacement planReplacement(RaftPeerId removed, String address, Path ticket, long timeoutMillis) throws IOException { var plan = node.planReplacement(removed, address, timeoutMillis); RatisReplacementTickets.save(ticket, plan); return plan; }
    /** 从已校验并同步的原票据续做，不擅自生成新加入身份。 */
    public RatisWalNode.ReplacementProgress beginReplacement(Path ticket, long timeoutMillis) throws IOException { return node.beginReplacement(RatisReplacementTickets.load(ticket, config.group().getGroupId().getUuid()), timeoutMillis); }
    /** 同一票据的真实追平和联合配置提交仍由节点协议负责。 */
    public RatisWalNode.ReplacementProgress completeReplacement(Path ticket, long timeoutMillis) throws IOException { return node.completeReplacement(RatisReplacementTickets.load(ticket, config.group().getGroupId().getUuid()), timeoutMillis); }
    /** 采样先排空再关闭节点；失败可重试，不关闭调用方借出的控制池。 */
    @Override public void close() throws IOException {
        closeGuard.lock(); try {
            if (closed) { return; } closing = true; observer.shutdown();
            try { if (!observer.awaitTermination(30, TimeUnit.SECONDS)) { throw new IOException("managed status worker did not drain; retry close"); } } catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new IOException("managed close interrupted", interrupted); }
            node.close(); closed = true;
        } finally { closeGuard.unlock(); }
    }
}
