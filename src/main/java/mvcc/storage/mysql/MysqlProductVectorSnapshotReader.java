package mvcc.storage.mysql;

import mvcc.common.ErrorCode;
import mvcc.common.MvccException;
import mvcc.common.TxStatus;
import mvcc.metadata.TransactionMetadataStore;
import mvcc.metadata.TxRecord;
import mvcc.storage.Utf8KeyOrder;
import mvcc.storage.VersionedValue;
import mvcc.storage.KeyRange;
import mvcc.storage.ScanPage;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** 在一个真实分片 RR 读视图中合并向量版本与未物化中心意向。 */
final class MysqlProductVectorSnapshotReader {
    private static final int MAX_INTERNAL_PAGE = 128;

    /** 只有适配器持有产品分片连接时才能调用。 */
    private MysqlProductVectorSnapshotReader() {
    }

    /** 已发布切面按本地序号筛选版本，再按中心决议时间判断跨分片可见性。 */
    static Optional<VersionedValue> get(Connection connection,
                                        MysqlStripedSnapshotOracle.Snapshot snapshot,
                                        String shardId, String key, int stripe, long requesterTxId,
                                        TransactionMetadataStore metadata) throws SQLException {
        long bound = snapshot.sequence(shardId, stripe);
        byte[] encodedKey = Utf8KeyOrder.encode(key);
        rejectUnmappedScalarRows(connection, encodedKey, stripe, requesterTxId);
        try (PreparedStatement own = connection.prepareStatement("""
                select i.start_ts, i.deleted, i.value
                from mvcc_intent i
                join mvcc_vector_intent_reservation_v1 r
                  on r.user_key = i.user_key and r.tx_id = i.tx_id
                where i.user_key = ? and i.tx_id = ?
                """)) {
            own.setBytes(1, encodedKey);
            own.setLong(2, requesterTxId);
            try (ResultSet row = own.executeQuery()) {
                if (row.next()) {
                    return Optional.of(new VersionedValue(key, row.getBytes("value"),
                            row.getBoolean("deleted"), row.getLong("start_ts"), true));
                }
            }
        }
        try (PreparedStatement query = connection.prepareStatement("""
                select c.reserved_seq as local_seq, c.commit_ts as decision_ts,
                       d.deleted, d.value, 0 as origin, c.tx_id,
                       0 as intent_start, d.tx_id is null as missing_version
                from mvcc_vector_committed_v1 c
                left join mvcc_data d on d.user_key = c.user_key
                  and d.tx_id = c.tx_id and d.commit_ts = c.commit_ts
                where c.user_key = ? and c.stripe_id = ? and c.reserved_seq <= ?
                union all
                select r.reserved_seq as local_seq, 0 as decision_ts,
                       i.deleted, i.value, 1 as origin, r.tx_id,
                       i.start_ts as intent_start, i.tx_id is null as missing_version
                from mvcc_vector_intent_reservation_v1 r
                left join mvcc_intent i on i.user_key = r.user_key and i.tx_id = r.tx_id
                where r.user_key = ? and r.stripe_id = ? and r.reserved_seq <= ?
                union all
                select l.local_seq, l.local_seq as decision_ts,
                       l.deleted, l.payload as value, 2 as origin, l.tx_id,
                       0 as intent_start, 0 as missing_version
                from mvcc_vector_local_data_v1 l
                where l.user_key = ? and l.stripe_id = ? and l.local_seq <= ?
                order by local_seq desc
                """)) {
            for (int group = 0; group < 3; group++) {
                int offset = group * 3;
                query.setBytes(offset + 1, encodedKey);
                query.setInt(offset + 2, stripe);
                query.setLong(offset + 3, bound);
            }
            try (ResultSet rows = query.executeQuery()) {
                while (rows.next()) {
                    if (rows.getBoolean("missing_version")) {
                        throw new MvccException(ErrorCode.ILLEGAL_STATE,
                                "vector mapping lost its product intent or committed version", false, requesterTxId);
                    }
                    long visibleTs;
                    int origin = rows.getInt("origin");
                    if (origin == 2) {
                        // 本地 1PC 已由分条上界证明可见，不依赖中心决议时间。
                        return Optional.of(new VersionedValue(key, rows.getBytes("value"),
                                rows.getBoolean("deleted"), rows.getLong("local_seq"), false));
                    }
                    if (origin == 0) {
                        visibleTs = rows.getLong("decision_ts");
                    } else {
                        long intentTxId = rows.getLong("tx_id");
                        TxRecord decision = metadata.get(intentTxId)
                                .orElseThrow(() -> new MysqlSnapshotReader.MissingDecisionException(intentTxId));
                        if (decision.status() != TxStatus.COMMITTED || decision.commitTs() == null
                                || decision.startTs() != rows.getLong("intent_start")) {
                            continue;
                        }
                        visibleTs = decision.commitTs();
                    }
                    if (visibleTs <= snapshot.decisionEpoch()) {
                        return Optional.of(new VersionedValue(key, rows.getBytes("value"),
                                rows.getBoolean("deleted"), visibleTs, false));
                    }
                }
                return Optional.empty();
            }
        }
    }

    /** 一页内所有键使用同一分片 RR 视图和同一全局向量，游标来自原始键。 */
    static ScanPage page(Connection connection, MysqlStripedSnapshotOracle.Snapshot snapshot,
                         String shardId, KeyRange range, String afterKeyExclusive, int limit,
                         long requesterTxId, TransactionMetadataStore metadata) throws SQLException {
        int pageLimit = Math.min(limit, MAX_INTERNAL_PAGE);
        List<String> candidates = candidateKeys(connection, range, afterKeyExclusive, pageLimit + 1);
        boolean more = candidates.size() > pageLimit;
        List<String> keys = more ? candidates.subList(0, pageLimit) : candidates;
        Map<String, Integer> stripes = keyStripes(connection, keys, requesterTxId);
        rejectUnmappedScalarRows(connection, keys, requesterTxId);
        Map<String, VersionedValue> visible = visiblePageValues(connection, snapshot,
                shardId, keys, stripes, requesterTxId, metadata);
        List<VersionedValue> values = new ArrayList<>(keys.size());
        for (String key : keys) {
            VersionedValue value = visible.get(key);
            if (value != null && !value.deleted()) {
                values.add(value);
            }
        }
        return new ScanPage(values, more ? keys.getLast() : null);
    }

    /** 五种真实键来源都参与候选发现，旧标量孤儿不能在范围扫描中被静默漏掉。 */
    private static List<String> candidateKeys(Connection connection, KeyRange range,
                                              String afterKeyExclusive, int limit) throws SQLException {
        List<byte[]> bounds = new ArrayList<>();
        StringBuilder predicate = new StringBuilder(" where 1 = 1");
        if (range.startKeyInclusive() != null) {
            predicate.append(" and user_key >= ?");
            bounds.add(Utf8KeyOrder.encode(range.startKeyInclusive()));
        }
        if (range.endKeyExclusive() != null) {
            predicate.append(" and user_key < ?");
            bounds.add(Utf8KeyOrder.encode(range.endKeyExclusive()));
        }
        if (afterKeyExclusive != null) {
            predicate.append(" and user_key > ?");
            bounds.add(Utf8KeyOrder.encode(afterKeyExclusive));
        }
        List<String> sources = List.of("mvcc_vector_local_data_v1", "mvcc_vector_committed_v1",
                "mvcc_vector_intent_reservation_v1", "mvcc_data", "mvcc_intent");
        StringBuilder sql = new StringBuilder("select user_key from (");
        for (int index = 0; index < sources.size(); index++) {
            if (index != 0) {
                sql.append(" union ");
            }
            sql.append("(select distinct user_key from ").append(sources.get(index))
                    .append(predicate).append(" order by user_key limit ?)");
        }
        sql.append(") page_keys order by user_key limit ?");
        try (PreparedStatement query = connection.prepareStatement(sql.toString())) {
            int parameter = 1;
            for (int ignored = 0; ignored < sources.size(); ignored++) {
                for (byte[] bound : bounds) {
                    query.setBytes(parameter++, bound);
                }
                query.setInt(parameter++, limit);
            }
            query.setInt(parameter, limit);
            try (ResultSet rows = query.executeQuery()) {
                List<String> keys = new ArrayList<>();
                while (rows.next()) {
                    byte[] bytes = rows.getBytes(1);
                    String key = new String(bytes, StandardCharsets.UTF_8);
                    if (!java.util.Arrays.equals(bytes, Utf8KeyOrder.encode(key))) {
                        throw new MvccException(ErrorCode.ILLEGAL_STATE,
                                "vector scan found malformed UTF-8 key", false, null);
                    }
                    keys.add(key);
                }
                return keys;
            }
        }
    }

    /** 三类向量行在一次查询中给每个候选键确定唯一的物理分条。 */
    private static Map<String, Integer> keyStripes(Connection connection, List<String> keys,
                                                    long requesterTxId) throws SQLException {
        Map<String, Integer> stripes = new HashMap<>();
        if (keys.isEmpty()) {
            return stripes;
        }
        String in = placeholders(keys.size());
        String sql = "select user_key, stripe_id from mvcc_vector_local_data_v1 where user_key in (" + in
                + ") union select user_key, stripe_id from mvcc_vector_committed_v1 where user_key in (" + in
                + ") union select user_key, stripe_id from mvcc_vector_intent_reservation_v1 where user_key in ("
                + in + ")";
        try (PreparedStatement query = connection.prepareStatement(sql)) {
            int parameter = 1;
            for (int source = 0; source < 3; source++) {
                parameter = bindKeys(query, keys, parameter);
            }
            try (ResultSet rows = query.executeQuery()) {
                while (rows.next()) {
                    String key = decodeKey(rows.getBytes("user_key"), requesterTxId);
                    int stripe = rows.getInt("stripe_id");
                    Integer previous = stripes.putIfAbsent(key, stripe);
                    if (previous != null && previous != stripe) {
                        throw new MvccException(ErrorCode.ILLEGAL_STATE,
                                "vector scan found a key in multiple stripes", false, requesterTxId);
                    }
                }
            }
        }
        if (stripes.size() != keys.size()) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE,
                    "vector scan found a key without vector ordering proof", false, requesterTxId);
        }
        return stripes;
    }

    /** 一次找出整页所有无映射的旧标量行，不能因向量行存在就忽略混合协议历史。 */
    private static void rejectUnmappedScalarRows(Connection connection, List<String> keys,
                                                 long requesterTxId) throws SQLException {
        if (keys.isEmpty()) {
            return;
        }
        String in = placeholders(keys.size());
        String sql = """
                select 1 from mvcc_data d where d.user_key in (%s)
                  and not exists(select 1 from mvcc_vector_committed_v1 v
                    where v.user_key = d.user_key and v.tx_id = d.tx_id and v.commit_ts = d.commit_ts)
                union all
                select 1 from mvcc_intent i where i.user_key in (%s)
                  and not exists(select 1 from mvcc_vector_intent_reservation_v1 v
                    where v.user_key = i.user_key and v.tx_id = i.tx_id)
                limit 1
                """.formatted(in, in);
        try (PreparedStatement query = connection.prepareStatement(sql)) {
            int parameter = bindKeys(query, keys, 1);
            bindKeys(query, keys, parameter);
            try (ResultSet row = query.executeQuery()) {
                if (row.next()) {
                    throw new MvccException(ErrorCode.ILLEGAL_STATE,
                            "vector scan found an unmapped scalar version or intent", false, requesterTxId);
                }
            }
        }
    }

    /** 分条上界与候选键一次传给数据库，三类版本合并后按原有点读规则选首个可见项。 */
    private static Map<String, VersionedValue> visiblePageValues(Connection connection,
            MysqlStripedSnapshotOracle.Snapshot snapshot, String shardId, List<String> keys,
            Map<String, Integer> stripes, long requesterTxId,
            TransactionMetadataStore metadata) throws SQLException {
        Map<String, VersionedValue> visible = new HashMap<>();
        if (keys.isEmpty()) {
            return visible;
        }
        String keyRows = IntStream.range(0, keys.size())
                .mapToObj(index -> "select ? as user_key, ? as stripe_id, ? as bound")
                .collect(Collectors.joining(" union all "));
        String sql = """
                with scan_keys as (%s)
                select user_key, local_seq, decision_ts, deleted, value, origin, tx_id,
                       intent_start, missing_version
                from (
                  select c.user_key, c.reserved_seq as local_seq, c.commit_ts as decision_ts,
                         d.deleted, d.value, 0 as origin, c.tx_id,
                         0 as intent_start, d.tx_id is null as missing_version
                  from mvcc_vector_committed_v1 c
                  join scan_keys k on k.user_key = c.user_key and k.stripe_id = c.stripe_id
                    and c.reserved_seq <= k.bound
                  left join mvcc_data d on d.user_key = c.user_key
                    and d.tx_id = c.tx_id and d.commit_ts = c.commit_ts
                  union all
                  select r.user_key, r.reserved_seq as local_seq, 0 as decision_ts,
                         i.deleted, i.value, 1 as origin, r.tx_id,
                         i.start_ts as intent_start, i.tx_id is null as missing_version
                  from mvcc_vector_intent_reservation_v1 r
                  join scan_keys k on k.user_key = r.user_key and k.stripe_id = r.stripe_id
                    and (r.reserved_seq <= k.bound or r.tx_id = ?)
                  left join mvcc_intent i on i.user_key = r.user_key and i.tx_id = r.tx_id
                  union all
                  select l.user_key, l.local_seq, l.local_seq as decision_ts,
                         l.deleted, l.payload as value, 2 as origin, l.tx_id,
                         0 as intent_start, 0 as missing_version
                  from mvcc_vector_local_data_v1 l
                  join scan_keys k on k.user_key = l.user_key and k.stripe_id = l.stripe_id
                    and l.local_seq <= k.bound
                ) candidates
                order by user_key, case when origin = 1 and tx_id = ? then 1 else 0 end desc,
                         local_seq desc
                """.formatted(keyRows);
        Map<Long, TxRecord> decisions = new HashMap<>();
        try (PreparedStatement query = connection.prepareStatement(sql)) {
            int parameter = 1;
            for (String key : keys) {
                int stripe = stripes.get(key);
                query.setBytes(parameter++, Utf8KeyOrder.encode(key));
                query.setInt(parameter++, stripe);
                query.setLong(parameter++, snapshot.sequence(shardId, stripe));
            }
            query.setLong(parameter++, requesterTxId);
            query.setLong(parameter, requesterTxId);
            try (ResultSet rows = query.executeQuery()) {
                while (rows.next()) {
                    String key = decodeKey(rows.getBytes("user_key"), requesterTxId);
                    if (visible.containsKey(key)) {
                        continue;
                    }
                    if (rows.getBoolean("missing_version")) {
                        throw new MvccException(ErrorCode.ILLEGAL_STATE,
                                "vector mapping lost its product intent or committed version", false, requesterTxId);
                    }
                    int origin = rows.getInt("origin");
                    long sequence = rows.getLong("local_seq");
                    if (origin == 1 && rows.getLong("tx_id") == requesterTxId) {
                        visible.put(key, new VersionedValue(key, rows.getBytes("value"),
                                rows.getBoolean("deleted"), rows.getLong("intent_start"), true));
                        continue;
                    }
                    if (origin == 2) {
                        visible.put(key, new VersionedValue(key, rows.getBytes("value"),
                                rows.getBoolean("deleted"), sequence, false));
                        continue;
                    }
                    long visibleTs;
                    if (origin == 0) {
                        visibleTs = rows.getLong("decision_ts");
                    } else {
                        long txId = rows.getLong("tx_id");
                        TxRecord decision = decisions.computeIfAbsent(txId, ignored -> metadata.get(txId)
                                .orElseThrow(() -> new MysqlSnapshotReader.MissingDecisionException(txId)));
                        if (decision.status() != TxStatus.COMMITTED || decision.commitTs() == null
                                || decision.startTs() != rows.getLong("intent_start")) {
                            continue;
                        }
                        visibleTs = decision.commitTs();
                    }
                    if (visibleTs <= snapshot.decisionEpoch()) {
                        visible.put(key, new VersionedValue(key, rows.getBytes("value"),
                                rows.getBoolean("deleted"), visibleTs, false));
                    }
                }
            }
        }
        return visible;
    }

    /** 绑定批量二进制业务键并返回下一参数位置。 */
    private static int bindKeys(PreparedStatement query, List<String> keys, int parameter)
            throws SQLException {
        for (String key : keys) {
            query.setBytes(parameter++, Utf8KeyOrder.encode(key));
        }
        return parameter;
    }

    /** 每页参数列表只由有界候选键数量生成。 */
    private static String placeholders(int count) {
        return IntStream.range(0, count).mapToObj(ignored -> "?").collect(Collectors.joining(", "));
    }

    /** 数据库键必须能无损往返 UTF-8，防止 Java 字符串别名。 */
    private static String decodeKey(byte[] bytes, long requesterTxId) {
        String key = new String(bytes, StandardCharsets.UTF_8);
        if (!java.util.Arrays.equals(bytes, Utf8KeyOrder.encode(key))) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE,
                    "vector scan found malformed UTF-8 key", false, requesterTxId);
        }
        return key;
    }

    /** 混入无向量映射的旧版本或意向时拒绝猜测跨协议先后顺序。 */
    private static void rejectUnmappedScalarRows(Connection connection, byte[] key,
                                                 int stripe, long requesterTxId)
            throws SQLException {
        try (PreparedStatement query = connection.prepareStatement("""
                select
                  exists(select 1 from mvcc_data d where d.user_key = ?
                    and not exists(select 1 from mvcc_vector_committed_v1 v
                                   where v.user_key = d.user_key and v.tx_id = d.tx_id
                                     and v.commit_ts = d.commit_ts)) as scalar_data,
                  exists(select 1 from mvcc_intent i where i.user_key = ?
                    and not exists(select 1 from mvcc_vector_intent_reservation_v1 v
                                   where v.user_key = i.user_key and v.tx_id = i.tx_id)) as scalar_intent,
                  exists(select 1 from mvcc_vector_committed_v1 v
                         where v.user_key = ? and v.stripe_id <> ?) as wrong_committed_stripe,
                  exists(select 1 from mvcc_vector_intent_reservation_v1 v
                         where v.user_key = ? and v.stripe_id <> ?) as wrong_intent_stripe,
                  exists(select 1 from mvcc_vector_local_data_v1 v
                         where v.user_key = ? and v.stripe_id <> ?) as wrong_local_stripe
                """)) {
            query.setBytes(1, key);
            query.setBytes(2, key);
            query.setBytes(3, key);
            query.setInt(4, stripe);
            query.setBytes(5, key);
            query.setInt(6, stripe);
            query.setBytes(7, key);
            query.setInt(8, stripe);
            try (ResultSet row = query.executeQuery()) {
                if (!row.next() || row.getBoolean("scalar_data") || row.getBoolean("scalar_intent")
                        || row.getBoolean("wrong_committed_stripe")
                        || row.getBoolean("wrong_intent_stripe")
                        || row.getBoolean("wrong_local_stripe")) {
                    throw new MvccException(ErrorCode.ILLEGAL_STATE,
                            "vector read found a version without vector ordering proof", false, requesterTxId);
                }
            }
        }
    }
}
