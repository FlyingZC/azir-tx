package mvcc.storage.mysql;

import mvcc.common.ErrorCode;
import mvcc.common.MvccException;
import mvcc.control.MysqlConnectionContext;
import mvcc.control.MysqlDatabaseAuthority;
import mvcc.publication.mysql.MysqlLocalFirstPublicationLedger;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Comparator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.Objects;

/** 用全局向量下界和中心最终化凭据退役一笔不再可见的跨片大载荷版本。 */
final class MysqlVectorCrossGarbageCollector {
    private final MysqlStorageAdapter foreground;
    private final DataSource shard;
    private final String shardId;
    private final int stripes;
    private final MysqlVectorSnapshotLeaseStore protection;
    private final MysqlLocalFirstPublicationLedger ledger;
    private final int maxInFlight;
    private final int retireBatchSize;
    private volatile String expectedShardIdentity;

    /** 单键跨片映射身份在键锁与参与者锁之后重新读取。 */
    private record Mapping(byte[] key, long txId, int stripe, long startSeq, long sequence,
                           long commitTs, boolean deleted, byte[] payload) {
    }

    /** 分片游标在一次查询中携带稳定业务键，退役前仍在键锁下重查真实映射。 */
    private record Candidate(long txId, byte[] key) {
    }

    /** 必须显式绑定真实参与者、中心最终化权威和相同布局的读者保护。 */
    MysqlVectorCrossGarbageCollector(MysqlStorageAdapter foreground, DataSource shard,
                                     String shardId, int stripes,
                                     MysqlVectorSnapshotLeaseStore protection,
                                     MysqlLocalFirstPublicationLedger ledger) {
        this(foreground, shard, shardId, stripes, protection, ledger, 1);
    }

    /** 独立候选之间可在显式并发预算内并行退役，同键稳定锁仍串行冲突历史。 */
    MysqlVectorCrossGarbageCollector(MysqlStorageAdapter foreground, DataSource shard,
                                     String shardId, int stripes,
                                     MysqlVectorSnapshotLeaseStore protection,
                                     MysqlLocalFirstPublicationLedger ledger, int maxInFlight) {
        this(foreground, shard, shardId, stripes, protection, ledger, maxInFlight, 1);
    }

    /** 一笔有界分片事务可退休多个键，出错时凭据和所有大载荷删除整体回滚。 */
    MysqlVectorCrossGarbageCollector(MysqlStorageAdapter foreground, DataSource shard,
                                     String shardId, int stripes,
                                     MysqlVectorSnapshotLeaseStore protection,
                                     MysqlLocalFirstPublicationLedger ledger,
                                     int maxInFlight, int retireBatchSize) {
        this.foreground = Objects.requireNonNull(foreground, "foreground");
        this.shard = Objects.requireNonNull(shard, "shard");
        this.shardId = Objects.requireNonNull(shardId, "shardId");
        this.protection = Objects.requireNonNull(protection, "protection");
        this.ledger = Objects.requireNonNull(ledger, "ledger");
        if (shardId.isBlank() || !shardId.equals(foreground.shardId())
                || stripes < 1 || stripes > 256 || maxInFlight < 1 || maxInFlight > 32
                || retireBatchSize < 1 || retireBatchSize > 32) {
            throw new IllegalArgumentException("invalid vector cross GC shard layout");
        }
        this.stripes = stripes;
        this.maxInFlight = maxInFlight;
        this.retireBatchSize = retireBatchSize;
    }

    /** 调度器只读真实后台池，用于与发布者分片权威核对。 */
    DataSource dataSource() {
        return shard;
    }

    /** 候选扫描必须按构造时的逻辑分片路由执行。 */
    String shardId() {
        return shardId;
    }

    /** 安全向量的分条数量不得与参与者布局漂移。 */
    int stripeCount() {
        return stripes;
    }

    /** 初始化确认分片真实身份和持久布局，重启不删除任何原凭据。 */
    void initializeProduct() {
        requireCenterAuthority();
        requireShardAuthority();
        requireSafeFloorLayout(protection.currentSafeFloor());
        try (Connection connection = shard.getConnection();
             PreparedStatement layout = connection.prepareStatement("""
                     select stripe_count from mvcc_vector_layout_v1 where singleton_id = 1
                     """)) {
            String identity = MysqlVectorDatabaseIdentity.read(connection);
            try (ResultSet row = layout.executeQuery()) {
                if (!row.next() || row.getInt(1) != stripes) {
                    throw illegal("vector cross GC stripe layout is missing or changed");
                }
            }
            MysqlVectorRetiredVersionStore.ensure(connection);
            try (PreparedStatement create = connection.prepareStatement("""
                    create table if not exists mvcc_vector_cross_gc_cursor_v1 (
                      singleton_id tinyint not null primary key,
                      after_tx_id bigint not null
                    ) engine=InnoDB
                    """)) {
                create.execute();
            }
            try (PreparedStatement insert = connection.prepareStatement("""
                    insert into mvcc_vector_cross_gc_cursor_v1(singleton_id, after_tx_id)
                    values (1, ?) on duplicate key update singleton_id = singleton_id
                    """)) {
                insert.setLong(1, Long.MIN_VALUE);
                insert.executeUpdate();
            }
            expectedShardIdentity = identity;
        } catch (SQLException ex) {
            throw unavailable("initialize vector cross GC failed", ex);
        }
    }

    /** 每轮有界轮转正式跨片映射，失败保留持久回执并在下次环绕时重试。 */
    int collect(int limit) {
        if (limit < 1 || limit > 512) {
            throw new IllegalArgumentException("vector cross GC scan limit must be in 1..512");
        }
        if (expectedShardIdentity == null) {
            throw illegal("vector cross GC was not initialized");
        }
        requireCenterAuthority();
        requireShardAuthority();
        MysqlStripedSnapshotOracle.Snapshot floor = protection.currentSafeFloor();
        requireSafeFloorLayout(floor);
        List<Candidate> candidates;
        try {
            candidates = nextCandidates(limit);
        } catch (SQLException ex) {
            throw unavailable("scan vector cross GC candidates failed", ex);
        }
        Map<Long, MysqlLocalFirstPublicationLedger.FinalizedVectorProof> proofs =
                ledger.finalizedVectorProofs(candidates.stream().map(Candidate::txId).toList());
        var centerIdentity = MysqlDatabaseAuthority.inspect(protection.centerDataSource());
        List<List<Candidate>> batches = partitionCandidates(candidates);
        if (maxInFlight == 1 || batches.size() < 2) {
            int reclaimed = 0;
            for (List<Candidate> batch : batches) {
                reclaimed += retireBatch(batch, proofs, floor, centerIdentity);
            }
            return reclaimed;
        }
        Semaphore budget = new Semaphore(maxInFlight);
        try (var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            List<CompletableFuture<Integer>> tasks = new ArrayList<>(batches.size());
            for (List<Candidate> batch : batches) {
                tasks.add(CompletableFuture.supplyAsync(() -> {
                    budget.acquireUninterruptibly();
                    try {
                        return retireBatch(batch, proofs, floor, centerIdentity);
                    } finally {
                        budget.release();
                    }
                }, workers));
            }
            int reclaimed = 0;
            RuntimeException firstFailure = null;
            for (CompletableFuture<Integer> task : tasks) {
                try {
                    reclaimed += task.join();
                } catch (CompletionException ex) {
                    if (firstFailure == null) {
                        firstFailure = ex.getCause() instanceof RuntimeException runtime
                                ? runtime : new IllegalStateException("cross GC worker failed", ex.getCause());
                    }
                }
            }
            if (firstFailure != null) {
                throw firstFailure;
            }
            return reclaimed;
        }
    }

    /** 同批按二进制业务键排序取锁，降低与业务写入及其他退役批次的死锁概率。 */
    private List<List<Candidate>> partitionCandidates(List<Candidate> candidates) {
        if (candidates.isEmpty()) {
            return List.of();
        }
        List<Candidate> ordered = new ArrayList<>(candidates);
        if (retireBatchSize > 1) {
            ordered.sort(Comparator.comparing(Candidate::key, Arrays::compareUnsigned)
                    .thenComparingLong(Candidate::txId));
        }
        List<List<Candidate>> groups = new ArrayList<>();
        for (int offset = 0; offset < ordered.size(); offset += retireBatchSize) {
            groups.add(List.copyOf(ordered.subList(offset,
                    Math.min(ordered.size(), offset + retireBatchSize))));
        }
        return List.copyOf(groups);
    }

    /** 游标与候选选择在同一分片事务中提交，重启后不会永远重扫表头。 */
    private List<Candidate> nextCandidates(int limit) throws SQLException {
        return MysqlConnectionContext.inTransaction(shard, connection -> {
            if (!expectedShardIdentity.equals(MysqlVectorDatabaseIdentity.read(connection))) {
                throw illegal("vector cross GC shard identity changed while scanning candidates");
            }
            long after;
            try (PreparedStatement cursor = connection.prepareStatement("""
                    select after_tx_id from mvcc_vector_cross_gc_cursor_v1
                    where singleton_id = 1 for update
                    """); ResultSet row = cursor.executeQuery()) {
                if (!row.next()) {
                    throw illegal("vector cross GC candidate cursor is missing");
                }
                after = row.getLong(1);
            }
            List<Candidate> found = new ArrayList<>(limit);
            try (PreparedStatement query = connection.prepareStatement("""
                    select tx_id, user_key from mvcc_vector_committed_v1
                    where tx_id > ? order by tx_id limit ?
                    """)) {
                query.setLong(1, after);
                query.setInt(2, limit);
                try (ResultSet row = query.executeQuery()) {
                    while (row.next()) {
                        found.add(new Candidate(row.getLong(1), row.getBytes(2)));
                    }
                }
            }
            long next = found.size() < limit ? Long.MIN_VALUE : found.getLast().txId();
            try (PreparedStatement update = connection.prepareStatement("""
                    update mvcc_vector_cross_gc_cursor_v1 set after_tx_id = ?
                    where singleton_id = 1
                    """)) {
                update.setLong(1, next);
                if (update.executeUpdate() != 1) {
                    throw illegal("vector cross GC candidate cursor update lost row");
                }
            }
            return List.copyOf(found);
        });
    }

    /** 单笔退役只删安全前驱之前的跨片版本；提交凭据与两种大载荷行原子替换。 */
    int retireOne(long txId) {
        if (expectedShardIdentity == null || txId == 0) {
            throw illegal("vector cross GC is uninitialized or has invalid transaction");
        }
        requireCenterAuthority();
        requireShardAuthority();
        MysqlStripedSnapshotOracle.Snapshot floor = protection.currentSafeFloor();
        requireSafeFloorLayout(floor);
        var proof = ledger.finalizedVectorProof(txId).orElse(null);
        try {
            Mapping discovered;
            try (Connection lookup = shard.getConnection()) {
                discovered = mapping(lookup, txId, false);
            }
            if (discovered == null) {
                return 0;
            }
            return retireCandidate(new Candidate(txId, discovered.key()), proof, floor,
                    MysqlDatabaseAuthority.inspect(protection.centerDataSource()));
        } catch (SQLException ex) {
            throw unavailable("retire vector cross version failed", ex);
        }
    }

    /** 批次复用中心证明与安全切面，真正删除仍逐笔持稳定键锁和本地原子回执。 */
    private int retireCandidate(Candidate candidate,
                                MysqlLocalFirstPublicationLedger.FinalizedVectorProof proof,
                                MysqlStripedSnapshotOracle.Snapshot floor,
                                MysqlDatabaseAuthority.Identity centerIdentity) {
        return retireBatch(List.of(candidate), proof == null ? Map.of() : Map.of(candidate.txId(), proof),
                floor, centerIdentity);
    }

    /** 同一键顺序的一组候选共用一次分片提交，任何异常使全组回滚。 */
    private int retireBatch(List<Candidate> batch,
                            Map<Long, MysqlLocalFirstPublicationLedger.FinalizedVectorProof> proofs,
                            MysqlStripedSnapshotOracle.Snapshot floor,
                            MysqlDatabaseAuthority.Identity centerIdentity) {
        String expected = expectedShardIdentity;
        if (expected == null) {
            return 0;
        }
        List<Candidate> eligible = new ArrayList<>(batch.size());
        for (Candidate candidate : batch) {
            var proof = proofs.get(candidate.txId());
            if (proof == null || !proof.belongsTo(ledger) || proof.txId() != candidate.txId()
                    || !proof.includes(shardId) || proof.commitTs() > floor.decisionEpoch()) {
                continue;
            }
            if (!proof.authorityIdentity().equals(centerIdentity)) {
                throw illegal("vector cross GC finalized proof came from another control database");
            }
            eligible.add(candidate);
        }
        if (eligible.isEmpty()) {
            return 0;
        }
        try {
            return MysqlConnectionContext.inTransaction(shard, connection -> {
                if (!expected.equals(MysqlVectorDatabaseIdentity.read(connection))) {
                    throw illegal("vector cross GC shard identity changed before deletion");
                }
                int reclaimed = 0;
                for (Candidate candidate : eligible) {
                    reclaimed += retireInTransaction(connection, candidate,
                            proofs.get(candidate.txId()), floor);
                }
                return reclaimed;
            });
        } catch (SQLException ex) {
            throw unavailable("retire vector cross version failed", ex);
        }
    }

    /** 每个候选仍持稳定键锁、参与者终态锁和完整版本校验。 */
    private int retireInTransaction(Connection connection, Candidate candidate,
                                    MysqlLocalFirstPublicationLedger.FinalizedVectorProof proof,
                                    MysqlStripedSnapshotOracle.Snapshot floor) throws SQLException {
        lockKey(connection, candidate.key());
        requireParticipant(connection, proof);
        Mapping current = mapping(connection, candidate.txId(), true);
        if (current == null) {
            if (retiredReceiptExists(connection, proof)) {
                return 0;
            }
            throw illegal("vector cross GC lost mapping without retirement receipt");
        }
        if (!Arrays.equals(current.key(), candidate.key())
                || current.commitTs() != proof.commitTs()
                || current.stripe() < 0 || current.stripe() >= stripes
                || current.sequence() <= current.startSeq()) {
            throw illegal("vector cross GC mapping differs from finalized proof");
        }
        long bound = floor.sequence(shardId, current.stripe());
        if (current.sequence() > bound || hasBlockingHistory(connection, current)) {
            return 0;
        }
        Long predecessor = visiblePredecessor(connection, current, bound,
                floor.decisionEpoch());
        if (predecessor == null || predecessor <= current.sequence()) {
            return 0;
        }
        requireOnlyVersionForTransaction(connection, current);
        requireMatchingData(connection, current);
        writeReceipt(connection, current, proof.startTs());
        deleteLargeRows(connection, current);
        return 1;
    }

    /** 并发后台已原子退休同一候选时，凭据允许当前批次安全跳过。 */
    private static boolean retiredReceiptExists(Connection connection,
                                                 MysqlLocalFirstPublicationLedger.FinalizedVectorProof proof)
            throws SQLException {
        try (PreparedStatement query = connection.prepareStatement("""
                select 1 from mvcc_vector_retired_v1
                where tx_id = ? and start_ts = ? and commit_ts = ? for update
                """)) {
            query.setLong(1, proof.txId());
            query.setLong(2, proof.startTs());
            query.setLong(3, proof.commitTs());
            try (ResultSet row = query.executeQuery()) {
                return row.next();
            }
        }
    }

    /** 控制租约与最终化记录必须来自同一真实 MySQL 服务器和数据库。 */
    private void requireCenterAuthority() {
        if (!ledger.databaseIdentity().equals(MysqlDatabaseAuthority.inspect(protection.centerDataSource()))) {
            throw illegal("vector cross GC center proof differs from reader safe floor authority");
        }
    }

    /** 后台连接必须始终指向前台适配器的同一物理分片。 */
    private void requireShardAuthority() {
        if (!MysqlDatabaseAuthority.inspect(foreground.vectorDataSource())
                .equals(MysqlDatabaseAuthority.inspect(shard))) {
            throw illegal("vector cross GC background shard differs from foreground route");
        }
    }

    /** 不能把其他路由或不同分条数的安全向量解释为本分片删除界限。 */
    private void requireSafeFloorLayout(MysqlStripedSnapshotOracle.Snapshot floor) {
        var components = floor.localSequences().get(shardId);
        if (components == null || components.size() != stripes) {
            throw illegal("vector cross GC safe floor differs from shard stripe layout");
        }
    }

    /** 未加锁读取仅用于发现键，最终删除前必须在稳定键锁下重查。 */
    private static Mapping mapping(Connection connection, long txId, boolean lock) throws SQLException {
        String sql = "select user_key, tx_id, stripe_id, start_seq, reserved_seq, commit_ts, deleted, payload "
                + "from mvcc_vector_committed_v1 where tx_id = ?" + (lock ? " for update" : "");
        try (PreparedStatement query = connection.prepareStatement(sql)) {
            query.setLong(1, txId);
            try (ResultSet row = query.executeQuery()) {
                if (!row.next()) {
                    return null;
                }
                Mapping result = new Mapping(row.getBytes("user_key"), row.getLong("tx_id"),
                        row.getInt("stripe_id"), row.getLong("start_seq"),
                        row.getLong("reserved_seq"), row.getLong("commit_ts"),
                        row.getBoolean("deleted"), row.getBytes("payload"));
                if (row.next()) {
                    throw illegal("vector cross GC found multiple keys for one transaction");
                }
                return result;
            }
        }
    }

    /** 稳定业务键锁阻止并发本地写或新跨片预写跨越 GC 判断。 */
    private static void lockKey(Connection connection, byte[] key) throws SQLException {
        try (PreparedStatement lock = connection.prepareStatement("""
                insert into mvcc_key_lock(user_key) values (?)
                on duplicate key update user_key = user_key
                """)) {
            lock.setBytes(1, key);
            lock.executeUpdate();
        }
    }

    /** 分片终态是重放回执的持久锚点，不能接受缺失或不同的本地决议。 */
    private static void requireParticipant(Connection connection,
                                           MysqlLocalFirstPublicationLedger.FinalizedVectorProof proof)
            throws SQLException {
        try (PreparedStatement query = connection.prepareStatement("""
                select start_ts, status, commit_ts from mvcc_participant_txn
                where tx_id = ? for update
                """)) {
            query.setLong(1, proof.txId());
            try (ResultSet row = query.executeQuery()) {
                if (!row.next() || row.getLong("start_ts") != proof.startTs()
                        || !"COMMITTED".equals(row.getString("status"))
                        || row.getLong("commit_ts") != proof.commitTs()) {
                    throw illegal("vector cross GC lost its committed participant terminal");
                }
            }
        }
    }

    /** 活跃意向、回滚标记、跨分条漂移和旧标量行令整笔退役失败关闭。 */
    private static boolean hasBlockingHistory(Connection connection, Mapping current)
            throws SQLException {
        byte[] key = current.key();
        int stripe = current.stripe();
        for (String table : java.util.List.of("mvcc_intent", "mvcc_vector_intent_reservation_v1",
                "mvcc_rollback")) {
            if (exists(connection, "select 1 from " + table + " where user_key = ? limit 1 for update",
                    key, null)) {
                return true;
            }
        }
        for (String table : java.util.List.of("mvcc_vector_local_data_v1",
                "mvcc_vector_committed_v1")) {
            if (exists(connection, "select 1 from " + table
                    + " where user_key = ? and stripe_id <> ? limit 1 for update", key, stripe)) {
                throw illegal("vector cross GC found a key in multiple stripes");
            }
        }
        return exists(connection, """
                select 1 from mvcc_data d where d.user_key = ?
                  and not exists(select 1 from mvcc_vector_committed_v1 c
                    where c.user_key = d.user_key and c.tx_id = d.tx_id
                      and c.commit_ts = d.commit_ts)
                limit 1 for update
                """, key, null);
    }

    /** 读取当前分片锁保护下的存在性，第二参数只用于分条漂移查询。 */
    private static boolean exists(Connection connection, String sql, byte[] key, Integer stripe)
            throws SQLException {
        try (PreparedStatement query = connection.prepareStatement(sql)) {
            query.setBytes(1, key);
            if (stripe != null) {
                query.setInt(2, stripe);
            }
            try (ResultSet row = query.executeQuery()) {
                return row.next();
            }
        }
    }

    /** 可见前驱由安全切面中的本地与中心已决议版本共同取最大序号。 */
    private static Long visiblePredecessor(Connection connection, Mapping current,
                                           long sequenceBound, long decisionBound)
            throws SQLException {
        Long local = latestSequence(connection, """
                select local_seq from mvcc_vector_local_data_v1
                where user_key = ? and stripe_id = ? and local_seq <= ?
                order by local_seq desc limit 1 for update
                """, current.key(), current.stripe(), sequenceBound, null);
        Long cross = latestSequence(connection, """
                select reserved_seq from mvcc_vector_committed_v1
                where user_key = ? and stripe_id = ? and reserved_seq <= ?
                  and commit_ts <= ?
                order by reserved_seq desc limit 1 for update
                """, current.key(), current.stripe(), sequenceBound, decisionBound);
        if (cross != null && (local == null || cross > local)) {
            Mapping successor = mappingByKeySequence(connection, current.key(), cross);
            if (successor == null || successor.stripe() != current.stripe()) {
                throw illegal("vector cross GC lost its visible successor mapping");
            }
            requireMatchingData(connection, successor);
        }
        if (local == null) {
            return cross;
        }
        return cross == null ? local : Math.max(local, cross);
    }

    /** 用于遮盖旧版本的跨片前驱必须仍有完整真实载荷。 */
    private static Mapping mappingByKeySequence(Connection connection, byte[] key, long sequence)
            throws SQLException {
        try (PreparedStatement query = connection.prepareStatement("""
                select user_key, tx_id, stripe_id, start_seq, reserved_seq,
                       commit_ts, deleted, payload
                from mvcc_vector_committed_v1
                where user_key = ? and reserved_seq = ? for update
                """)) {
            query.setBytes(1, key);
            query.setLong(2, sequence);
            try (ResultSet row = query.executeQuery()) {
                if (!row.next()) {
                    return null;
                }
                return new Mapping(row.getBytes("user_key"), row.getLong("tx_id"),
                        row.getInt("stripe_id"), row.getLong("start_seq"),
                        row.getLong("reserved_seq"), row.getLong("commit_ts"),
                        row.getBoolean("deleted"), row.getBytes("payload"));
            }
        }
    }

    /** 当前读只取一行安全前驱，避免把旧 RR 视图当作 GC 判断依据。 */
    private static Long latestSequence(Connection connection, String sql, byte[] key,
                                       int stripe, long bound, Long decisionBound)
            throws SQLException {
        try (PreparedStatement query = connection.prepareStatement(sql)) {
            query.setBytes(1, key);
            query.setInt(2, stripe);
            query.setLong(3, bound);
            if (decisionBound != null) {
                query.setLong(4, decisionBound);
            }
            try (ResultSet row = query.executeQuery()) {
                return row.next() ? row.getLong(1) : null;
            }
        }
    }

    /** 一笔正式跨片写每片只有一个版本，混入其他行不得生成假重放回执。 */
    private static void requireOnlyVersionForTransaction(Connection connection, Mapping current)
            throws SQLException {
        try (PreparedStatement query = connection.prepareStatement("""
                select user_key, commit_ts from mvcc_data where tx_id = ? for update
                """)) {
            query.setLong(1, current.txId());
            try (ResultSet row = query.executeQuery()) {
                if (!row.next() || !Arrays.equals(row.getBytes(1), current.key())
                        || row.getLong(2) != current.commitTs() || row.next()) {
                    throw illegal("vector cross GC found a non-single-key transaction version");
                }
            }
        }
    }

    /** 真正的版本载荷必须与持久映射逐字节一致，不能为损坏历史签发凭据。 */
    private static void requireMatchingData(Connection connection, Mapping current)
            throws SQLException {
        try (PreparedStatement query = connection.prepareStatement("""
                select deleted, value from mvcc_data
                where user_key = ? and tx_id = ? and commit_ts = ? for update
                """)) {
            query.setBytes(1, current.key());
            query.setLong(2, current.txId());
            query.setLong(3, current.commitTs());
            try (ResultSet row = query.executeQuery()) {
                if (!row.next() || row.getBoolean("deleted") != current.deleted()
                        || !Arrays.equals(row.getBytes("value"), current.payload()) || row.next()) {
                    throw illegal("vector cross GC found an incomplete version payload");
                }
            }
        }
    }

    /** 小凭据先写入同一事务，随后删除两份大载荷，提交失败时一起回滚。 */
    private static void writeReceipt(Connection connection, Mapping current, long startTs)
            throws SQLException {
        try (PreparedStatement insert = connection.prepareStatement("""
                insert into mvcc_vector_retired_v1
                  (tx_id, user_key, stripe_id, start_ts, start_seq, reserved_seq,
                   commit_ts, deleted, payload_sha)
                values (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """)) {
            insert.setLong(1, current.txId());
            insert.setBytes(2, current.key());
            insert.setInt(3, current.stripe());
            insert.setLong(4, startTs);
            insert.setLong(5, current.startSeq());
            insert.setLong(6, current.sequence());
            insert.setLong(7, current.commitTs());
            insert.setBoolean(8, current.deleted());
            insert.setBytes(9, MysqlVectorRetiredVersionStore.payloadDigest(
                    current.deleted(), current.payload()));
            if (insert.executeUpdate() != 1) {
                throw illegal("vector cross GC failed to persist retirement receipt");
            }
        }
    }

    /** 回执与大载荷删除同事务完成，任何行数不符都回滚。 */
    private static void deleteLargeRows(Connection connection, Mapping current) throws SQLException {
        try (PreparedStatement data = connection.prepareStatement("""
                delete from mvcc_data where user_key = ? and tx_id = ? and commit_ts = ?
                """); PreparedStatement mapping = connection.prepareStatement("""
                delete from mvcc_vector_committed_v1
                where user_key = ? and tx_id = ? and reserved_seq = ? and commit_ts = ?
                """)) {
            data.setBytes(1, current.key());
            data.setLong(2, current.txId());
            data.setLong(3, current.commitTs());
            mapping.setBytes(1, current.key());
            mapping.setLong(2, current.txId());
            mapping.setLong(3, current.sequence());
            mapping.setLong(4, current.commitTs());
            if (data.executeUpdate() != 1 || mapping.executeUpdate() != 1) {
                throw illegal("vector cross GC lost an atomically retired large row");
            }
        }
    }

    /** 持久映射或数据库身份不符必须停止删除。 */
    private static MvccException illegal(String message) {
        return new MvccException(ErrorCode.ILLEGAL_STATE, message, false, null);
    }

    /** 连接或事务异常后可能已提交回执，调用方必须凭持久状态重试确认。 */
    private static MvccException unavailable(String message, SQLException cause) {
        return new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN, message, true, null, cause);
    }
}
