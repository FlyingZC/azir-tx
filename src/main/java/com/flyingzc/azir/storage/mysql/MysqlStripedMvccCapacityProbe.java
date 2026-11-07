package com.flyingzc.azir.storage.mysql;

import com.zaxxer.hikari.HikariDataSource;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

/** 分条本地提交序号与 MVCC 版本同事务写入的隔离容量上界，不包含全局读协议。 */
public final class MysqlStripedMvccCapacityProbe {
    /** 基准入口不保存全局可变状态。 */
    private MysqlStripedMvccCapacityProbe() {
    }

    /** 只有明确重置的隔离分片数据库才允许创建容量实验表和存储过程。 */
    public static void main(String[] args) throws Exception {
        if (!Boolean.parseBoolean(env("AZIR_TX_RESET", "false"))) {
            throw new IllegalArgumentException("striped MVCC capacity probe requires AZIR_TX_RESET=true");
        }
        int concurrency = Integer.parseInt(env("AZIR_TX_BENCH_CONCURRENCY", "50"));
        int durationSeconds = Integer.parseInt(env("AZIR_TX_BENCH_DURATION_SECONDS", "20"));
        int stripes = Integer.parseInt(env("AZIR_TX_NATIVE_WATERMARK_STRIPES", "64"));
        boolean checked = Boolean.parseBoolean(env("AZIR_TX_STRIPED_CHECKED", "false"));
        if (concurrency < 1 || concurrency > 512 || durationSeconds < 1 || durationSeconds > 600
                || stripes < 1 || stripes > 256) {
            throw new IllegalArgumentException("invalid striped MVCC capacity limits");
        }
        String url = env("AZIR_TX_SHARD1_JDBC", "jdbc:mysql://127.0.0.1:23306/shard_1");
        try (HikariDataSource dataSource = (HikariDataSource) MysqlDataSourceFactory.create(url,
                env("AZIR_TX_USERNAME", "root"), env("AZIR_TX_PASSWORD", "root"))) {
            if (checked) {
                initializeCheckedIsolated(dataSource, stripes);
            } else {
                initializeIsolated(dataSource, stripes);
            }
            run(dataSource, concurrency, durationSeconds, stripes, checked);
        }
    }

    /** 分条行和版本行完全属于本容量探针，不触碰生产 MVCC 表。 */
    static void initializeIsolated(HikariDataSource dataSource, int stripes) throws Exception {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("""
                    create table if not exists bench_striped_mvcc_state (
                      stripe_id smallint not null primary key,
                      committed_seq bigint not null
                    ) engine=InnoDB
                    """);
            statement.execute("""
                    create table if not exists bench_striped_mvcc_data (
                      user_key varbinary(256) not null,
                      local_seq bigint not null,
                      stripe_id smallint not null,
                      payload blob not null,
                      primary key(user_key, local_seq)
                    ) engine=InnoDB
                    """);
            statement.executeUpdate("delete from bench_striped_mvcc_data");
            statement.executeUpdate("delete from bench_striped_mvcc_state");
            try (PreparedStatement insert = connection.prepareStatement("""
                    insert into bench_striped_mvcc_state(stripe_id, committed_seq) values (?, 0)
                    """)) {
                for (int stripe = 0; stripe < stripes; stripe++) {
                    insert.setInt(1, stripe);
                    insert.addBatch();
                }
                insert.executeBatch();
            }
            ensureCommitProcedure(connection);
        }
    }

    /** 已检查模式增加持久键锁和跨分片意向表，保留原无检查容量基线。 */
    static void initializeCheckedIsolated(HikariDataSource dataSource, int stripes) throws Exception {
        initializeIsolated(dataSource, stripes);
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("""
                    create table if not exists bench_striped_mvcc_key_lock (
                      user_key varbinary(256) not null primary key
                    ) engine=InnoDB
                    """);
            statement.execute("""
                    create table if not exists bench_striped_mvcc_intent (
                      user_key varbinary(256) not null primary key,
                      owner_tx_id bigint not null
                    ) engine=InnoDB
                    """);
            // 实验表允许在显式重置时重建，确保新例程不会误用旧列定义。
            statement.execute("drop table if exists bench_striped_mvcc_cross_data");
            statement.execute("""
                    create table if not exists bench_striped_mvcc_cross_data (
                      user_key varbinary(256) not null,
                      reserved_seq bigint not null,
                      tx_id bigint not null,
                      stripe_id smallint not null,
                      start_seq bigint not null,
                      decision_epoch bigint not null,
                      payload blob not null,
                      primary key (user_key, reserved_seq),
                      unique key cross_data_tx_key (tx_id, user_key)
                    ) engine=InnoDB
                    """);
            statement.executeUpdate("delete from bench_striped_mvcc_intent");
            statement.executeUpdate("delete from bench_striped_mvcc_cross_data");
            statement.executeUpdate("delete from bench_striped_mvcc_key_lock");
            ensureCheckedCommitProcedure(connection);
        }
    }

    /** 键锁先于一致读建立，确保冲突检查能看到同键前驱的最新提交。 */
    private static void ensureCheckedCommitProcedure(Connection connection) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement("""
                select 1 from information_schema.routines
                where routine_schema = database()
                  and routine_name = 'bench_striped_mvcc_commit_checked_v2' limit 1
                """); ResultSet row = query.executeQuery()) {
            if (row.next()) {
                return;
            }
        }
        try (PreparedStatement create = connection.prepareStatement("""
                create procedure bench_striped_mvcc_commit_checked_v2(
                    in p_key varbinary(256), in p_payload blob,
                    in p_stripe smallint, in p_start_seq bigint)
                begin
                  declare v_seq bigint default null;
                  declare v_conflict int default 0;
                  declare exit handler for sqlexception begin rollback; resignal; end;
                  start transaction;
                  insert into bench_striped_mvcc_key_lock(user_key) values (p_key)
                    on duplicate key update user_key = user_key;
                  select exists(select 1 from bench_striped_mvcc_intent
                                where user_key = p_key) into v_conflict;
                  if v_conflict = 1 then
                    signal sqlstate '45000' set mysql_errno = 36002,
                      message_text = 'STRIPED_MVCC_INTENT_CONFLICT';
                  end if;
                  select exists(select 1 from bench_striped_mvcc_data
                                where user_key = p_key and local_seq > p_start_seq)
                    into v_conflict;
                  if v_conflict = 1 then
                    signal sqlstate '45000' set mysql_errno = 36003,
                      message_text = 'STRIPED_MVCC_WRITE_CONFLICT';
                  end if;
                  select exists(select 1 from bench_striped_mvcc_cross_data
                                where user_key = p_key and reserved_seq > p_start_seq)
                    into v_conflict;
                  if v_conflict = 1 then
                    signal sqlstate '45000' set mysql_errno = 36003,
                      message_text = 'STRIPED_MVCC_WRITE_CONFLICT';
                  end if;
                  update bench_striped_mvcc_state
                    set committed_seq = last_insert_id(committed_seq + 1)
                    where stripe_id = p_stripe;
                  if row_count() <> 1 then
                    signal sqlstate '45000' set mysql_errno = 36001,
                      message_text = 'STRIPED_MVCC_STATE_MISSING';
                  end if;
                  set v_seq = last_insert_id();
                  insert into bench_striped_mvcc_data(user_key, local_seq, stripe_id, payload)
                    values (p_key, v_seq, p_stripe, p_payload);
                  commit;
                  select v_seq as local_seq;
                end
                """)) {
            create.execute();
        } catch (SQLException ex) {
            if (ex.getErrorCode() != 1304) {
                throw ex;
            }
        }
    }

    /** 一次分片事务取得分条提交序号并以该序号保存同一业务键版本。 */
    private static void ensureCommitProcedure(Connection connection) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement("""
                select 1 from information_schema.routines
                where routine_schema = database() and routine_name = 'bench_striped_mvcc_commit_v1' limit 1
                """); ResultSet row = query.executeQuery()) {
            if (row.next()) {
                return;
            }
        }
        try (PreparedStatement create = connection.prepareStatement("""
                create procedure bench_striped_mvcc_commit_v1(
                    in p_key varbinary(256), in p_payload blob, in p_stripe smallint)
                begin
                  declare v_seq bigint default null;
                  declare exit handler for sqlexception begin rollback; resignal; end;
                  start transaction;
                  update bench_striped_mvcc_state
                    set committed_seq = last_insert_id(committed_seq + 1)
                    where stripe_id = p_stripe;
                  if row_count() <> 1 then
                    signal sqlstate '45000' set mysql_errno = 36001,
                      message_text = 'STRIPED_MVCC_STATE_MISSING';
                  end if;
                  set v_seq = last_insert_id();
                  insert into bench_striped_mvcc_data(user_key, local_seq, stripe_id, payload)
                    values (p_key, v_seq, p_stripe, p_payload);
                  commit;
                  select v_seq as local_seq;
                end
                """)) {
            create.execute();
        } catch (SQLException ex) {
            if (ex.getErrorCode() != 1304) {
                throw ex;
            }
        }
    }

    /** 同一业务载荷下测量本地分条序号加版本的物理容量并核对持久行数。 */
    private static void run(HikariDataSource dataSource, int concurrency, int durationSeconds,
                            int stripes, boolean checked) throws Exception {
        AtomicLong sequence = new AtomicLong(100);
        LongAdder attempts = new LongAdder();
        LongAdder committed = new LongAdder();
        LongAdder failures = new LongAdder();
        AtomicReference<String> firstFailure = new AtomicReference<>();
        ConcurrentLinkedQueue<Long> latencies = new ConcurrentLinkedQueue<>();
        CountDownLatch ready = new CountDownLatch(concurrency);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(concurrency);
        try (var workers = Executors.newFixedThreadPool(concurrency)) {
            for (int index = 0; index < concurrency; index++) {
                workers.submit(() -> {
                    try (Connection connection = dataSource.getConnection();
                         PreparedStatement call = connection.prepareStatement(checked
                                 ? "call bench_striped_mvcc_commit_checked_v2(?, ?, ?, ?)"
                                 : "call bench_striped_mvcc_commit_v1(?, ?, ?)")) {
                        ready.countDown();
                        start.await();
                        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(durationSeconds);
                        while (System.nanoTime() < deadline) {
                            attempts.increment();
                            long started = System.nanoTime();
                            long id = sequence.incrementAndGet();
                            try {
                                call.setBytes(1, ("local-first:" + id).getBytes(StandardCharsets.UTF_8));
                                call.setBytes(2, ("v" + id).getBytes(StandardCharsets.UTF_8));
                                call.setInt(3, (int) (id % stripes));
                                if (checked) {
                                    // 唯一业务键在事务开始时尚无版本，零水位仍执行真实冲突查询。
                                    call.setLong(4, 0);
                                }
                                try (ResultSet row = call.executeQuery()) {
                                    if (!row.next() || row.getLong(1) < 1) {
                                        throw new SQLException("striped MVCC commit returned no local sequence");
                                    }
                                }
                                committed.increment();
                                latencies.add(System.nanoTime() - started);
                            } catch (Exception ex) {
                                failures.increment();
                                firstFailure.compareAndSet(null,
                                        ex.getClass().getSimpleName() + ":" + ex.getMessage());
                            }
                        }
                    } catch (Exception ex) {
                        failures.increment();
                        firstFailure.compareAndSet(null,
                                ex.getClass().getSimpleName() + ":" + ex.getMessage());
                        ready.countDown();
                    } finally {
                        done.countDown();
                    }
                });
            }
            ready.await();
            long started = System.nanoTime();
            start.countDown();
            done.await();
            double seconds = (System.nanoTime() - started) / 1_000_000_000.0;
            long[] sorted = latencies.stream().mapToLong(Long::longValue).sorted().toArray();
            double p99Millis = sorted.length == 0 ? Double.NaN
                    : sorted[Math.max(0, (int) Math.ceil(sorted.length * 0.99) - 1)] / 1_000_000.0;
            long[] persisted = persistedCounts(dataSource);
            if (persisted[0] != committed.sum() || persisted[1] != committed.sum()) {
                throw new IllegalStateException("striped MVCC version or watermark count differs from commits");
            }
            System.out.printf("STRIPED_MVCC_CAPACITY checked=%s concurrency=%d duration_s=%.2f stripes=%d "
                            + "attempts=%d committed=%d failures=%d commit_tps=%.2f p99_ms=%.2f "
                            + "versions=%d watermark_total=%d first_failure=%s%n",
                    checked, concurrency, seconds, stripes, attempts.sum(), committed.sum(), failures.sum(),
                    committed.sum() / seconds, p99Millis, persisted[0], persisted[1],
                    firstFailure.get() == null ? "-" : firstFailure.get());
        }
    }

    /** 版本行数和所有分条序号增量必须同时等于成功提交数。 */
    private static long[] persistedCounts(HikariDataSource dataSource) throws Exception {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement();
             ResultSet versions = statement.executeQuery("select count(*) from bench_striped_mvcc_data")) {
            if (!versions.next()) {
                throw new SQLException("striped MVCC version count returned no row");
            }
            long versionCount = versions.getLong(1);
            try (ResultSet state = statement.executeQuery(
                    "select sum(committed_seq) from bench_striped_mvcc_state")) {
                if (!state.next()) {
                    throw new SQLException("striped MVCC state count returned no row");
                }
                return new long[]{versionCount, state.getLong(1)};
            }
        }
    }

    /** 只读取本探针明确配置的分片连接和容量参数。 */
    private static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }
}
