package com.flyingzc.azir.storage.mysql;

import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.MvccException;
import com.flyingzc.azir.common.TxStatus;

import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** 主分片决议向量与参与者本地序号共同决定跨片版本可见性的隔离读原型。 */
final class MysqlPrimaryVectorReadPrototype implements AutoCloseable {
    private final String primaryShard;
    private final String secondaryShard;
    private final Map<String, DataSource> shards;
    private final MysqlPrimaryDecisionPrototype decisions;
    private final MysqlStripedSnapshotOracle dataOracle;

    /** 固定主次分片决议向量和各片数据向量，RR 可持有旧实例不变。 */
    record Snapshot(List<Long> decisionSequences, Map<String, List<Long>> dataSequences) {
        /** 客户端不能改变已捕获的决议或数据分量。 */
        Snapshot {
            decisionSequences = List.copyOf(decisionSequences);
            dataSequences = Map.copyOf(dataSequences);
            if (decisionSequences.isEmpty() || dataSequences.isEmpty()
                    || decisionSequences.stream().anyMatch(value -> value == null || value < 0)) {
                throw new IllegalArgumentException("invalid primary vector snapshot");
            }
        }

        /** 返回目标主分片决议条在本切面已发布的最大序号。 */
        long decisionSequence(int stripe) {
            return decisionSequences.get(stripe);
        }

        /** 返回本地数据条在本切面已持久完成的最大序号。 */
        long dataSequence(String shard, int stripe) {
            List<Long> values = dataSequences.get(shard);
            if (values == null || stripe < 0 || stripe >= values.size()) {
                throw new IllegalArgumentException("snapshot has no target data stripe");
            }
            return values.get(stripe);
        }
    }

    /** 两个分片均已完成预写后，主分片是唯一终态权威。 */
    MysqlPrimaryVectorReadPrototype(String primaryShard, DataSource primary,
                                    String secondaryShard, DataSource secondary,
                                    int stripes, MysqlPrimaryDecisionPrototype decisions) {
        if (primaryShard == null || primaryShard.isBlank() || secondaryShard == null
                || secondaryShard.isBlank() || primaryShard.equals(secondaryShard)) {
            throw new IllegalArgumentException("invalid primary and secondary routes");
        }
        this.primaryShard = primaryShard;
        this.secondaryShard = secondaryShard;
        this.shards = Map.of(primaryShard, Objects.requireNonNull(primary, "primary"),
                secondaryShard, Objects.requireNonNull(secondary, "secondary"));
        this.decisions = Objects.requireNonNull(decisions, "decisions");
        this.dataOracle = new MysqlStripedSnapshotOracle(shards, stripes, 1);
    }

    /** 先采主决议再采全部数据条，已采到的提交必有覆盖两片意向的序号。 */
    Snapshot captureUnpublished() {
        List<Long> decision = decisions.decisionVector();
        Map<String, List<Long>> data = dataOracle.sampleForPublication();
        return new Snapshot(decision, data);
    }

    /** 旧切面先过滤本地序号，再以主分片决议过滤意向与物化版本。 */
    Optional<byte[]> read(Snapshot snapshot, String shard, String key, int stripe) {
        Objects.requireNonNull(snapshot, "snapshot");
        Objects.requireNonNull(key, "key");
        DataSource dataSource = shards.get(shard);
        if (dataSource == null) {
            throw new IllegalArgumentException("unknown primary vector read route");
        }
        long dataBound = snapshot.dataSequence(shard, stripe);
        try (Connection connection = dataSource.getConnection(); PreparedStatement query = connection.prepareStatement("""
                select local_seq as version_seq, payload, 0 as origin,
                       0 as tx_id, 0 as decision_epoch
                from bench_striped_mvcc_data
                where user_key = ? and stripe_id = ? and local_seq <= ?
                union all
                select reserved_seq as version_seq, payload, 1 as origin,
                       tx_id, decision_epoch
                from bench_striped_mvcc_cross_data
                where user_key = ? and stripe_id = ? and reserved_seq <= ?
                union all
                select reserved_seq as version_seq, payload, 2 as origin,
                       tx_id, 0 as decision_epoch
                from bench_striped_mvcc_cross_intent
                where user_key = ? and stripe_id = ? and reserved_seq <= ?
                order by version_seq desc
                """)) {
            byte[] encoded = key.getBytes(StandardCharsets.UTF_8);
            for (int group = 0; group < 3; group++) {
                int offset = group * 3;
                query.setBytes(offset + 1, encoded);
                query.setInt(offset + 2, stripe);
                query.setLong(offset + 3, dataBound);
            }
            try (ResultSet rows = query.executeQuery()) {
                while (rows.next()) {
                    if (rows.getInt("origin") == 0) {
                        return Optional.of(rows.getBytes("payload"));
                    }
                    long txId = rows.getLong("tx_id");
                    long reservedSequence = rows.getLong("version_seq");
                    var decision = decisions.participantDecision(txId,
                            shard.equals(primaryShard), key, stripe, reservedSequence);
                    if (decision.status() != TxStatus.COMMITTED) {
                        continue;
                    }
                    if (rows.getInt("origin") == 1
                            && rows.getLong("decision_epoch") != decision.decisionSequence()) {
                        throw new MvccException(ErrorCode.ILLEGAL_STATE,
                                "materialized version has a different primary decision", false, txId);
                    }
                    if (decision.decisionSequence()
                            <= snapshot.decisionSequence(decision.decisionStripe())) {
                        return Optional.of(rows.getBytes("payload"));
                    }
                }
                return Optional.empty();
            }
        } catch (SQLException ex) {
            throw new MvccException(ErrorCode.STORAGE_UNAVAILABLE,
                    "read primary vector prototype version failed", true, null, ex);
        }
    }

    /** 关闭仅用于数据向量采集的读线程资源。 */
    @Override
    public void close() {
        dataOracle.close();
    }
}
