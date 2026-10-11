package mvcc.storage.mysql;

import mvcc.common.TxStatus;
import mvcc.metadata.TransactionMetadataStore;
import mvcc.metadata.TxRecord;
import mvcc.storage.KeyRange;
import mvcc.storage.ReadContext;
import mvcc.storage.ScanPage;
import mvcc.storage.Utf8KeyOrder;
import mvcc.storage.VersionedValue;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * 在调用者持有的同一本地快照中批量读取版本和意向，不持有跨页 JDBC 游标。
 */
final class MysqlSnapshotReader {
    /**
     * 阻止实例化快照读取工具。
     */
    private MysqlSnapshotReader() {
    }

    /**
     * 读取有界原始键页并叠加本事务写入和已发布的其他事务意向。
     */
    static ScanPage page(Connection connection, ReadContext context, KeyRange range,
                         String afterKeyExclusive, int limit, TransactionMetadataStore metadata) throws SQLException {
        List<String> collected = collectKeys(connection, range, afterKeyExclusive, limit + 1);
        boolean hasMore = collected.size() > limit;
        List<String> keys = hasMore ? collected.subList(0, limit) : collected;
        Map<String, VersionedValue> visible = visibleValues(connection, context, keys, metadata);
        List<VersionedValue> values = new ArrayList<>(keys.size());
        for (String key : keys) {
            VersionedValue value = visible.get(key);
            if (value != null && !value.deleted()) {
                values.add(value);
            }
        }
        // 游标必须来自扫描过的原始键，删除或不可见意向不能使分页停滞。
        return new ScanPage(values, hasMore ? keys.getLast() : null);
    }

    /**
     * 以两次批量查询读取指定键的最新可见版本及全部意向。
     */
    static Map<String, VersionedValue> visibleValues(Connection connection, ReadContext context,
                                                     List<String> keys, TransactionMetadataStore metadata) throws SQLException {
        Map<String, VersionedValue> values = new HashMap<>();
        if (keys.isEmpty()) {
            return values;
        }
        String versionSql = """
                select d.user_key, d.commit_ts, d.deleted, d.value
                from mvcc_data d
                join (select user_key, max(commit_ts) as commit_ts from mvcc_data
                      where user_key in (%s) and commit_ts <= ? group by user_key) latest
                  on latest.user_key = d.user_key and latest.commit_ts = d.commit_ts
                """.formatted(placeholders(keys.size()));
        try (PreparedStatement statement = connection.prepareStatement(versionSql)) {
            bindKeys(statement, keys);
            statement.setLong(keys.size() + 1, context.readTs());
            try (ResultSet rows = statement.executeQuery()) {
                while (rows.next()) {
                    String key = decode(rows.getBytes("user_key"));
                    values.put(key, new VersionedValue(key, rows.getBytes("value"), rows.getBoolean("deleted"),
                            rows.getLong("commit_ts"), false));
                }
            }
        }
        List<ReadIntent> intents = new ArrayList<>();
        Set<Long> foreignTransactions = new LinkedHashSet<>();
        try (PreparedStatement statement = connection.prepareStatement(
                "select user_key, tx_id, start_ts, deleted, value from mvcc_intent where user_key in ("
                        + placeholders(keys.size()) + ")")) {
            bindKeys(statement, keys);
            try (ResultSet rows = statement.executeQuery()) {
                while (rows.next()) {
                    ReadIntent intent = new ReadIntent(decode(rows.getBytes("user_key")), rows.getLong("tx_id"),
                            rows.getLong("start_ts"), rows.getBoolean("deleted"), rows.getBytes("value"));
                    intents.add(intent);
                    if (intent.txId() != context.txId()) {
                        foreignTransactions.add(intent.txId());
                    }
                }
            }
        }
        // 每页先去重再解析，非终态不缓存；下页允许重新观察刚发布的决议。
        Map<Long, TxRecord> terminal = new HashMap<>();
        if (metadata != null) {
            for (long txId : foreignTransactions) {
                // 旧 RR view 可能仍看见已物化的意向，但中心决议已被 GC；不能把缺失误判为未提交。
                TxRecord record = metadata.get(txId).orElseThrow(() -> new MissingDecisionException(txId));
                if (record.status() == TxStatus.COMMITTED || record.status() == TxStatus.ABORTED) {
                    terminal.put(txId, record);
                }
            }
        }
        for (ReadIntent intent : intents) {
            if (intent.txId() == context.txId()) {
                values.put(intent.key(), new VersionedValue(intent.key(), intent.value(), intent.deleted(), intent.startTs(), true));
                continue;
            }
            TxRecord decision = terminal.get(intent.txId());
            if (decision == null || decision.status() != TxStatus.COMMITTED || decision.commitTs() == null
                    || decision.startTs() != intent.startTs() || decision.commitTs() > context.readTs()) {
                continue;
            }
            VersionedValue current = values.get(intent.key());
            if (current == null || current.versionTs() < decision.commitTs()) {
                values.put(intent.key(), new VersionedValue(intent.key(), intent.value(), intent.deleted(), decision.commitTs(), false));
            }
        }
        return values;
    }

    /**
     * 将每个来源限制为一页候选键，避免 UNION 在分页前装载整个键范围。
     */
    private static List<String> collectKeys(Connection connection, KeyRange range, String after, int limit) throws SQLException {
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
        if (after != null) {
            predicate.append(" and user_key > ?");
            bounds.add(Utf8KeyOrder.encode(after));
        }
        String sql = "select user_key from ((select distinct user_key from mvcc_data" + predicate
                + " order by user_key limit ?) union (select user_key from mvcc_intent" + predicate
                + " order by user_key limit ?)) page_keys order by user_key limit ?";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            int parameter = 1;
            for (int source = 0; source < 2; source++) {
                for (byte[] bound : bounds) {
                    statement.setBytes(parameter++, bound);
                }
                statement.setInt(parameter++, limit);
            }
            statement.setInt(parameter, limit);
            try (ResultSet rows = statement.executeQuery()) {
                List<String> keys = new ArrayList<>();
                while (rows.next()) {
                    keys.add(decode(rows.getBytes("user_key")));
                }
                return keys;
            }
        }
    }

    /**
     * 绑定一页 UTF-8 二进制键参数。
     */
    private static void bindKeys(PreparedStatement statement, List<String> keys) throws SQLException {
        for (int index = 0; index < keys.size(); index++) {
            statement.setBytes(index + 1, Utf8KeyOrder.encode(keys.get(index)));
        }
    }

    /**
     * 创建有界参数列表，业务键始终通过 JDBC 参数传递。
     */
    private static String placeholders(int count) {
        return IntStream.range(0, count).mapToObj(ignored -> "?").collect(Collectors.joining(", "));
    }

    /**
     * 解码数据库中经过校验的 UTF-8 业务键。
     */
    private static String decode(byte[] key) {
        return new String(key, StandardCharsets.UTF_8);
    }

    /**
     * 通知调用者关闭旧本地快照并重新读取，不向上层泄漏或放大成整事务重试。
     */
    static final class MissingDecisionException extends RuntimeException {
        /**
         * 保留缺失中心决议的事务编号，供重读失败时报告不可判定的意向。
         */
        MissingDecisionException(long txId) {
            super("metadata decision missing for intent transaction " + txId);
        }
    }

    /**
     * 当前页的意向值，仅在本次读取期间保留。
     */
    private record ReadIntent(String key, long txId, long startTs, boolean deleted, byte[] value) {
    }
}
