package mvcc.control;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/** 严格单主组复制发现入口；只选择配置成员中的 ONLINE PRIMARY，绝不降级到异步副本。 */
public final class MysqlGroupDataSource implements DataSource {
    private final String group;
    private final List<DataSource> members;
    private final Set<String> identities;
    private final AtomicInteger preferred = new AtomicInteger();
    private final java.util.concurrent.atomic.AtomicBoolean quorumTableInitialized = new java.util.concurrent.atomic.AtomicBoolean();

    /** 绑定至少三个独立成员；连接池与节点生命周期仍由应用拥有。 */
    public MysqlGroupDataSource(String group, List<? extends DataSource> members) {
        this(group, discover(members));
    }
    /** 显式配置成员 UUID，允许在一个成员停机时重启应用而不降低法定多数要求。 */
    public MysqlGroupDataSource(String group, Map<String, ? extends DataSource> members) {
        UUID.fromString(group); this.group = group;
        if (members.size() < 3 || members.size() > 9) { throw new IllegalArgumentException("group requires 3..9 configured members"); }
        members.keySet().forEach(UUID::fromString);
        this.members = List.copyOf(new TreeMap<>(members).values()); identities = Set.copyOf(members.keySet());
        try (var ignored = getConnection()) { } catch (SQLException failure) { throw new IllegalArgumentException("group has no verified primary", failure); }
    }
    /** 仅用于全部节点可访问的首次发现，生产重启可使用已保存的明确 UUID 配置。 */
    private static Map<String, DataSource> discover(List<? extends DataSource> members) {
        var found = new LinkedHashMap<String, DataSource>();
        for (var source : members) {
            try (var c = source.getConnection(); var q = c.createStatement(); var r = q.executeQuery("select @@server_uuid")) {
                r.next(); if (found.put(r.getString(1), source) != null) { throw new IllegalArgumentException("duplicate MySQL group member"); }
            } catch (SQLException failure) { throw new IllegalArgumentException("cannot initially identify MySQL group members", failure); }
        }
        return found;
    }
    /** 每次借用都核对真实组视图，缓存的首选索引不能代替服务端角色。 */
    @Override public Connection getConnection() throws SQLException {
        SQLException failure = new SQLException("no quorum-backed ONLINE PRIMARY");
        int first = preferred.get();
        for (int attempt = 0; attempt < members.size(); attempt++) {
            int index = (first + attempt) % members.size(); Connection connection = null;
            try {
                connection = members.get(index).getConnection(); verify(connection); preferred.set(index); return connection;
            } catch (SQLException problem) {
                failure.addSuppressed(problem);
                if (connection != null) { MysqlNamedLock.discard(connection, problem); try { connection.close(); } catch (SQLException close) { failure.addSuppressed(close); } }
            } catch (RuntimeException | Error fatal) {
                if (connection != null) { MysqlNamedLock.discard(connection, fatal); try { connection.close(); } catch (SQLException close) { fatal.addSuppressed(close); } }
                throw fatal;
            }
        }
        throw failure;
    }
    /** 显式用户凭据不能绕过固定成员池的配置。 */
    @Override public Connection getConnection(String username, String password) throws SQLException { throw new SQLFeatureNotSupportedException("configure credentials on member pools"); }
    /** 校验主节点、已知成员法定多数、同步日志持久化与单主模式。 */
    public void verify(Connection connection) throws SQLException {
        MysqlSessionRequirements.require(connection);
        if (!connection.getAutoCommit()) { throw new SQLException("group member pools require auto-commit"); }
        try (var q = connection.createStatement(); var r = q.executeQuery("select @@global.group_replication_group_name,@@global.group_replication_single_primary_mode,@@global.sync_binlog,@@global.innodb_flush_log_at_trx_commit,@@global.read_only,@@global.super_read_only")) {
            if (!r.next() || !group.equals(r.getString(1)) || !r.getBoolean(2) || r.getLong(3) != 1 || r.getLong(4) != 1 || r.getBoolean(5) || r.getBoolean(6)) { throw new SQLException("unsafe group role or durability configuration"); }
        }
        Set<String> online = new HashSet<>(); String local = null; boolean primary = false;
        try (var q = connection.createStatement(); var r = q.executeQuery("select member_id,member_state,member_role,member_id=@@server_uuid from performance_schema.replication_group_members")) {
            while (r.next()) {
                String id = r.getString(1);
                if (!identities.contains(id)) { throw new SQLException("unconfigured group membership change; rebuild the member roster"); }
                if (r.getString(2).equals("ONLINE")) { online.add(id); }
                if (r.getBoolean(4)) { local = id; primary = r.getString(2).equals("ONLINE") && r.getString(3).equals("PRIMARY"); }
            }
        }
        if (!primary || local == null || online.size() < members.size() / 2 + 1) { throw new SQLException("group member is not a quorum-backed primary"); }
        try (var q = connection.createStatement()) {
            q.execute("set session group_replication_consistency='BEFORE_AND_AFTER'");
            if (!quorumTableInitialized.get()) {
                q.execute("create table if not exists azir_group_quorum(id int primary key,pulse bigint not null) engine=InnoDB");
                q.executeUpdate("insert ignore into azir_group_quorum values(1,0)"); quorumTableInitialized.set(true);
            }
            // 成员视图可能尚未反映网络分区；一次同步组提交才能证明当前连接仍有法定多数。
            if (q.executeUpdate("update azir_group_quorum set pulse=pulse+1 where id=1 and pulse<9223372036854775807") != 1) { throw new SQLException("group quorum pulse unavailable"); }
        }
    }
    /** 返回稳定组身份，成员换主不会重置框架命名空间或时间戳。 */
    public String group() { return group; }
    /** 转交日志器设置。 */
    @Override public PrintWriter getLogWriter() throws SQLException { return members.getFirst().getLogWriter(); }
    /** 为全部成员设置相同日志器。 */
    @Override public void setLogWriter(PrintWriter writer) throws SQLException { for (var source : members) { source.setLogWriter(writer); } }
    /** 为全部成员设置有限连接建立超时。 */
    @Override public void setLoginTimeout(int seconds) throws SQLException { for (var source : members) { source.setLoginTimeout(seconds); } }
    /** 返回成员池的连接建立超时。 */
    @Override public int getLoginTimeout() throws SQLException { return members.getFirst().getLoginTimeout(); }
    /** 返回标准数据源日志器。 */
    @Override public Logger getParentLogger() { return Logger.getLogger(MysqlGroupDataSource.class.getName()); }
    /** 只允许解包当前组发现器，不能绕过成员校验。 */
    @Override public <T> T unwrap(Class<T> type) throws SQLException { if (type.isInstance(this)) { return type.cast(this); } throw new SQLException("not a group datasource wrapper"); }
    /** 查询组发现器支持的接口。 */
    @Override public boolean isWrapperFor(Class<?> type) { return type.isInstance(this); }
}
