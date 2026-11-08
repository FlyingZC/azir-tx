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

package com.flyingzc.azir.e2e;

import com.flyingzc.azir.api.Transaction;
import com.flyingzc.azir.common.IsolationLevel;
import com.flyingzc.azir.common.MvccException;
import com.flyingzc.azir.gc.GcManager;
import com.flyingzc.azir.gc.SafePointStore;
import com.flyingzc.azir.gc.mysql.MysqlSafePointStore;
import com.flyingzc.azir.metadata.TransactionMetadataStore;
import com.flyingzc.azir.metadata.mysql.MysqlTransactionMetadataStore;
import com.flyingzc.azir.recovery.BackgroundRecoveryRunner;
import com.flyingzc.azir.recovery.RecoveryManager;
import com.flyingzc.azir.storage.KeyRange;
import com.flyingzc.azir.storage.Mutation;
import com.flyingzc.azir.storage.ParticipantRecord;
import com.flyingzc.azir.storage.ReadContext;
import com.flyingzc.azir.storage.ScanPage;
import com.flyingzc.azir.storage.StorageAdapter;
import com.flyingzc.azir.storage.VersionedValue;
import com.flyingzc.azir.storage.mysql.MysqlDataSourceFactory;
import com.flyingzc.azir.storage.mysql.MysqlStorageAdapter;
import com.flyingzc.azir.tso.mysql.MysqlTimestampOracle;
import com.flyingzc.azir.txn.TransactionManager;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * MySQL MVCC end to end verify entrance.
 */
public final class MysqlMvccE2E {

    /**
     * Construct a MysqlMvccE2E instance.
     */
    private MysqlMvccE2E() {
    }

    /**
     * Run the application entry point.
     *
     * @param args args
     * @throws Exception if the operation cannot complete
     */
    public static void main(final String[] args) throws Exception {
        Config config = Config.fromEnv();
        DataSource controlDataSource = MysqlDataSourceFactory.create(config.controlJdbcUrl(), config.username(), config.password());
        DataSource shard1DataSource = MysqlDataSourceFactory.create(config.shard1JdbcUrl(), config.username(), config.password());
        DataSource shard2DataSource = MysqlDataSourceFactory.create(config.shard2JdbcUrl(), config.username(), config.password());

        try {
            MysqlTimestampOracle tso = new MysqlTimestampOracle(controlDataSource);
            MysqlTransactionMetadataStore metadataStore = new MysqlTransactionMetadataStore(controlDataSource);
            MysqlSafePointStore safePointStore = new MysqlSafePointStore(controlDataSource);
            MysqlStorageAdapter shard1 = new MysqlStorageAdapter("shard-1", shard1DataSource);
            MysqlStorageAdapter shard2 = new MysqlStorageAdapter("shard-2", shard2DataSource);

            shard1.initialize();
            shard2.initialize();
            tso.initialize(100);
            metadataStore.initialize();
            safePointStore.initialize(0L);

            if (config.resetTables()) {
                reset(controlDataSource, "mvcc_txn_finalized", "mvcc_snapshot_lease", "mvcc_txn", "mvcc_tso", "mvcc_safe_point");
                reset(shard1DataSource, "mvcc_intent", "mvcc_data", "mvcc_rollback",
                        "mvcc_participant_txn", "mvcc_key_lock", "mvcc_local_safe_point");
                shard1.initialize();
                reset(shard2DataSource, "mvcc_intent", "mvcc_data", "mvcc_rollback",
                        "mvcc_participant_txn", "mvcc_key_lock", "mvcc_local_safe_point");
                shard2.initialize();
                tso.initialize(100);
                metadataStore.initialize();
                safePointStore.initialize(0L);
            }

            TransactionManager transactionManager = new TransactionManager(tso, metadataStore, safePointStore, Map.of(
                    shard1.shardId(), shard1,
                    shard2.shardId(), shard2));
            RecoveryManager recoveryManager = new RecoveryManager(metadataStore, Map.of(
                    shard1.shardId(), shard1,
                    shard2.shardId(), shard2));
            BackgroundRecoveryRunner recoveryRunner = new BackgroundRecoveryRunner(recoveryManager, Map.of(
                    shard1.shardId(), shard1,
                    shard2.shardId(), shard2));
            GcManager gcManager = new GcManager(metadataStore, tso, safePointStore, Map.of(
                    shard1.shardId(), shard1,
                    shard2.shardId(), shard2));

            scenarioRcRr(transactionManager);
            scenarioScan(transactionManager);
            scenarioCrossShard(transactionManager);
            scenarioRecovery(tso, metadataStore, safePointStore, shard1, shard2, recoveryRunner);
            scenarioGc(transactionManager, gcManager, safePointStore, shard1DataSource);

            System.out.println("E2E PASS");
            System.out.println("safe_point=" + safePointStore.currentSafePoint());
            System.out.println("remaining_versions_for_e2e_gc_001=" + countVersionsForKey(shard1DataSource, "e2e:gc:001"));
        } finally {
            close(controlDataSource);
            close(shard1DataSource);
            close(shard2DataSource);
        }
    }

    /**
     * Execute commit read and repeatable isolation level verify scenes.
     *
     * @param transactionManager transaction manager
     */
    private static void scenarioRcRr(final TransactionManager transactionManager) {
        seed(transactionManager, "shard-1", "e2e:acct:1", "v1");

        Transaction rrReader = transactionManager.begin(IsolationLevel.REPEATABLE_READ);
        requireEquals("v1", read(transactionManager, rrReader, "shard-1", "e2e:acct:1"), "RR initial read");

        Transaction writer = transactionManager.begin(IsolationLevel.READ_COMMITTED);
        transactionManager.put(writer, "shard-1", "e2e:acct:1", bytes("v2"));
        transactionManager.commit(writer);

        requireEquals("v1", read(transactionManager, rrReader, "shard-1", "e2e:acct:1"), "RR should keep snapshot");
        transactionManager.rollback(rrReader);

        Transaction rcReader = transactionManager.begin(IsolationLevel.READ_COMMITTED);
        requireEquals("v2", read(transactionManager, rcReader, "shard-1", "e2e:acct:1"), "RC should see latest committed");
        transactionManager.rollback(rcReader);

        System.out.println("[PASS] RC/RR");
    }

    /**
     * Executing range scan for visibility verify scenes.
     *
     * @param transactionManager transaction manager
     */
    private static void scenarioScan(final TransactionManager transactionManager) {
        seed(transactionManager, "shard-1", "e2e:scan:001", "a");
        seed(transactionManager, "shard-1", "e2e:scan:002", "b");

        Transaction tx = transactionManager.begin(IsolationLevel.READ_COMMITTED);
        transactionManager.put(tx, "shard-1", "e2e:scan:003", bytes("c"));
        List<String> keys = transactionManager.scan(tx, "shard-1", "e2e:scan:000", "e2e:scan:999")
                .stream()
                .map(VersionedValue::key)
                .collect(Collectors.toList());
        requireEquals(List.of("e2e:scan:001", "e2e:scan:002", "e2e:scan:003"), keys, "scan should include own intent");
        transactionManager.rollback(tx);

        System.out.println("[PASS] scan(range)");
    }

    /**
     * Execute the cross-shard commit verify scene.
     *
     * @param transactionManager transaction manager
     */
    private static void scenarioCrossShard(final TransactionManager transactionManager) {
        Transaction tx = transactionManager.begin(IsolationLevel.REPEATABLE_READ);
        transactionManager.put(tx, "shard-1", "e2e:order:1", bytes("created"));
        transactionManager.put(tx, "shard-2", "e2e:stock:1", bytes("reserved"));
        transactionManager.commit(tx);

        Transaction reader = transactionManager.begin(IsolationLevel.READ_COMMITTED);
        requireEquals("created", read(transactionManager, reader, "shard-1", "e2e:order:1"), "cross-shard shard-1");
        requireEquals("reserved", read(transactionManager, reader, "shard-2", "e2e:stock:1"), "cross-shard shard-2");
        transactionManager.rollback(reader);

        System.out.println("[PASS] cross-shard 2PC");
    }

    /**
     * Execute the transaction recovery verify scene.
     *
     * @param tso tso
     * @param metadataStore metadata store
     * @param safePointStore safe point store
     * @param shard1 shard1
     * @param shard2 shard2
     * @param recoveryRunner recovery runner
     * @throws IllegalStateException if the operation cannot complete
     */
    private static void scenarioRecovery(final MysqlTimestampOracle tso,
                                         final TransactionMetadataStore metadataStore,
                                         final SafePointStore safePointStore,
                                         final StorageAdapter shard1,
                                         final StorageAdapter shard2,
                                         final BackgroundRecoveryRunner recoveryRunner) {
        TransactionManager flakyManager = new TransactionManager(tso, metadataStore, safePointStore, Map.of(
                shard1.shardId(), shard1,
                shard2.shardId(), new FailOnceOnCommitStorageAdapter(shard2)));
        TransactionManager readerManager = new TransactionManager(tso, metadataStore, safePointStore, Map.of(
                shard1.shardId(), shard1,
                shard2.shardId(), shard2));

        Transaction tx = flakyManager.begin(IsolationLevel.REPEATABLE_READ);
        flakyManager.put(tx, "shard-1", "e2e:recover:1", bytes("left"));
        flakyManager.put(tx, "shard-2", "e2e:recover:2", bytes("right"));
        try {
            flakyManager.commit(tx);
            throw new IllegalStateException("expected commit failure");
        } catch (final MvccException expected) {
            recoveryRunner.recoverOnce(100);
        }

        ParticipantRecord localRecord = shard2.participantRecord(tx.txId()).orElseThrow();
        requireEquals(com.flyingzc.azir.common.TxStatus.COMMITTED, localRecord.status(), "recovery local decision");
        requireEquals(metadataStore.get(tx.txId()).orElseThrow().commitTs(), localRecord.commitTs(),
                "recovery commit timestamp");

        Transaction reader = readerManager.begin(IsolationLevel.READ_COMMITTED);
        requireEquals("right", read(readerManager, reader, "shard-2", "e2e:recover:2"), "recovery should finish commit");
        readerManager.rollback(reader);

        System.out.println("[PASS] recovery");
    }

    /**
     * Execute history version GC verify scene.
     *
     * @param transactionManager transaction manager
     * @param gcManager gc manager
     * @param safePointStore safe point store
     * @param shard1DataSource shard1 data source
     * @throws Exception if the operation cannot complete
     * @throws IllegalStateException if the operation cannot complete
     */
    private static void scenarioGc(final TransactionManager transactionManager,
                                   final GcManager gcManager,
                                   final SafePointStore safePointStore,
                                   final DataSource shard1DataSource) throws Exception {
        Transaction seed1 = transactionManager.begin(IsolationLevel.READ_COMMITTED);
        transactionManager.put(seed1, "shard-1", "e2e:gc:001", bytes("alive"));
        transactionManager.commit(seed1);

        Transaction deleteTx = transactionManager.begin(IsolationLevel.READ_COMMITTED);
        transactionManager.delete(deleteTx, "shard-1", "e2e:gc:001");
        transactionManager.commit(deleteTx);

        int reclaimed = gcManager.gcOnce(100);
        if (reclaimed < 2) {
            throw new IllegalStateException("expected tombstone chain reclaimed, reclaimed=" + reclaimed);
        }
        if (countVersionsForKey(shard1DataSource, "e2e:gc:001") != 0) {
            throw new IllegalStateException("expected tombstone chain fully reclaimed");
        }
        Transaction reader = transactionManager.begin(IsolationLevel.READ_COMMITTED);
        try {
            requireEquals(null, read(transactionManager, reader, "shard-1", "e2e:gc:001"),
                    "deleted value must remain invisible after GC");
        } finally {
            transactionManager.rollback(reader);
        }
        if (safePointStore.currentSafePoint() <= 0) {
            throw new IllegalStateException("expected safe point to advance");
        }

        System.out.println("[PASS] safe point + GC + tombstone cleanup");
    }

    /**
     * Writes the initial data of the end-to-end scene.
     *
     * @param transactionManager transaction manager
     * @param shardId logical shard identifier
     * @param key business key
     * @param value value to write
     */
    private static void seed(final TransactionManager transactionManager, final String shardId, final String key, final String value) {
        Transaction tx = transactionManager.begin(IsolationLevel.READ_COMMITTED);
        transactionManager.put(tx, shardId, key, bytes(value));
        transactionManager.commit(tx);
    }

    /**
     * Reads the specified key value from the end to the end scene.
     *
     * @param transactionManager transaction manager
     * @param tx transaction context
     * @param shardId logical shard identifier
     * @param key business key
     * @return the operation result
     */
    private static String read(final TransactionManager transactionManager, final Transaction tx, final String shardId, final String key) {
        return transactionManager.get(tx, shardId, key).map(String::new).orElse(null);
    }

    /**
     * Encode the string as a byte array.
     *
     * @param value value to write
     * @return the operation result
     */
    private static byte[] bytes(final String value) {
        return value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    /**
     * The assertion of real values is consistent with expectations.
     *
     * @param expected expected
     * @param actual actual
     * @param message message
     * @throws IllegalStateException if the operation cannot complete
     */
    private static void requireEquals(final Object expected, final Object actual, final String message) {
        if (!java.util.Objects.equals(expected, actual)) {
            throw new IllegalStateException(message + ", expected=" + expected + ", actual=" + actual);
        }
    }

    /**
     * Reset the benchmark tables.
     *
     * @param dataSource database connection source
     * @param tables tables
     * @throws Exception if the operation cannot complete
     */
    private static void reset(final DataSource dataSource, final String... tables) throws Exception {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            for (String table : tables) {
                statement.execute("delete from " + table);
            }
        }
    }

    /**
     * Statistics the history of the key version.
     *
     * @param dataSource database connection source
     * @param key business key
     * @return the operation result
     * @throws Exception if the operation cannot complete
     */
    private static long countVersionsForKey(final DataSource dataSource, final String key) throws Exception {
        try (
                Connection connection = dataSource.getConnection();
                var statement = connection.prepareStatement("select count(*) from mvcc_data where user_key = ?")) {
            statement.setBytes(1, key.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            try (var resultSet = statement.executeQuery()) {
                resultSet.next();
                return resultSet.getLong(1);
            }
        }
    }

    /**
     * Close resources and background executors.
     *
     * @param dataSource database connection source
     * @throws Exception if the operation cannot complete
     */
    private static void close(final DataSource dataSource) throws Exception {
        if (dataSource instanceof AutoCloseable closeable) {
            closeable.close();
        }
    }

    /**
     * Read a string-valued environment variable.
     *
     * @param key business key
     * @param defaultValue default value
     * @return the operation result
     */
    private static String env(final String key, final String defaultValue) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? defaultValue : value;
    }

    /**
     * Benchmark configuration.
     *
     * @param controlJdbcUrl control jdbc url
     * @param shard1JdbcUrl shard1 jdbc url
     * @param shard2JdbcUrl shard2 jdbc url
     * @param username username
     * @param password password
     * @param resetTables reset tables
     */
    private record Config(String controlJdbcUrl,
            String shard1JdbcUrl,
            String shard2JdbcUrl,
            String username,
            String password,
            boolean resetTables) {

        /**
         * Read configuration from environment variables.
         *
         * @return the operation result
         */
        private static Config fromEnv() {
            return new Config(
                    env("AZIR_TX_CONTROL_JDBC", "jdbc:mysql://127.0.0.1:23306/control"),
                    env("AZIR_TX_SHARD1_JDBC", "jdbc:mysql://127.0.0.1:23306/shard_1"),
                    env("AZIR_TX_SHARD2_JDBC", "jdbc:mysql://127.0.0.1:23306/shard_2"),
                    env("AZIR_TX_USERNAME", "root"),
                    env("AZIR_TX_PASSWORD", "root"),
                    Boolean.parseBoolean(env("AZIR_TX_RESET", "true")));
        }
    }

    /**
     * Storage adapter packagingers injected into commit hours.
     */
    private static final class FailOnceOnCommitStorageAdapter implements StorageAdapter {

        private final StorageAdapter delegate;

        private final AtomicBoolean shouldFail = new AtomicBoolean(true);

        /**
         * Construct a FailOnceOnCommitStorageAdapter instance.
         *
         * @param delegate delegate
         */
        private FailOnceOnCommitStorageAdapter(final StorageAdapter delegate) {
            this.delegate = delegate;
        }

        /**
         * Transmit transaction Center bound to keep the same adaptor from binding different authority.
         *
         * @param store store
         */
        @Override
        public void configureMetadataStore(final TransactionMetadataStore store) {
            delegate.configureMetadataStore(store);
        }

        /**
         * Returns the commit intent resolution capability of a real adapter.
         *
         * @return the operation result
         */
        @Override
        public boolean supportsCommittedIntentResolution() {
            return delegate.supportsCommittedIntentResolution();
        }

        /**
         * Returns the atomic direct commit capability of a real adapter.
         *
         * @return the operation result
         */
        @Override
        public boolean supportsAtomicDirectCommit() {
            return delegate.supportsAtomicDirectCommit();
        }

        /**
         * Search for real participant records and keep recovery evidence when local commit unknown outcome.
         *
         * @param txId transaction identifier
         * @return the operation result
         */
        @Override
        public Optional<ParticipantRecord> participantRecord(final long txId) {
            return delegate.participantRecord(txId);
        }

        /**
         * Forwards the visible preparation requirements for a genuine adapter.
         *
         * @return the operation result
         */
        @Override
        public boolean requiresExplicitPreparePhase() {
            return delegate.requiresExplicitPreparePhase();
        }

        /**
         * Return the storage adapter shard identifier.
         *
         * @return the operation result
         */
        @Override
        public String shardId() {
            return delegate.shardId();
        }

        /**
         * Initialize the required database tables or runtime environment.
         */
        @Override
        public void initialize() {
            delegate.initialize();
        }

        /**
         * Read the currently visible data for the key or transaction.
         *
         * @param context context
         * @param key business key
         * @return the operation result
         */
        @Override
        public Optional<VersionedValue> get(final ReadContext context, final String key) {
            return delegate.get(context, key);
        }

        /**
         * Scan the key range using the current snapshot.
         *
         * @param context context
         * @param keyRange key range
         * @return the operation result
         */
        @Override
        public List<VersionedValue> scan(final ReadContext context, final KeyRange keyRange) {
            return delegate.scan(context, keyRange);
        }

        /**
         * Forwards a real page scanned, keeping the syntax of cursor and bounded queries.
         *
         * @param context context
         * @param keyRange key range
         * @param afterKeyExclusive after key exclusive
         * @param limit maximum number of items
         * @return the operation result
         */
        @Override
        public ScanPage scanPage(final ReadContext context, final KeyRange keyRange, final String afterKeyExclusive, final int limit) {
            return delegate.scanPage(context, keyRange, afterKeyExclusive, limit);
        }

        /**
         * Prewrite the prototype transaction.
         *
         * @param txId transaction identifier
         * @param startTs transaction start timestamp
         * @param mutations transaction write set
         */
        @Override
        public void prewrite(final long txId, final long startTs, final List<com.flyingzc.azir.storage.Mutation> mutations) {
            delegate.prewrite(txId, startTs, mutations);
        }

        /**
         * Verify transaction intents and enter the prepare phase.
         *
         * @param txId transaction identifier
         */
        @Override
        public void prepare(final long txId) {
            delegate.prepare(txId);
        }

        /**
         * Finalize transaction intents as committed versions.
         *
         * @param txId transaction identifier
         * @param commitTs durable commit timestamp
         */
        @Override
        public void commit(final long txId, final long commitTs) {
            failOnce();
            delegate.commit(txId, commitTs);
        }

        /**
         * Press write set commit to inject failure as well, avoiding the reloading of commit around the presentation scene.
         *
         * @param txId transaction identifier
         * @param commitTs durable commit timestamp
         * @param mutations transaction write set
         */
        @Override
        public void commit(final long txId, final long commitTs, final List<Mutation> mutations) {
            failOnce();
            delegate.commit(txId, commitTs, mutations);
        }

        /**
         * Keeps the real cleanup policy and injects it once.
         *
         * @param txId transaction identifier
         * @param commitTs durable commit timestamp
         * @param mutations transaction write set
         * @param cleanupIntents cleanup intents
         */
        @Override
        public void commit(final long txId, final long commitTs, final List<Mutation> mutations, final boolean cleanupIntents) {
            failOnce();
            delegate.commit(txId, commitTs, mutations, cleanupIntents);
        }

        /**
         * Direct commit call for real atomic, not a normal commit instead of local terminal state sets.
         *
         * @param txId transaction identifier
         * @param startTs transaction start timestamp
         * @param commitTs durable commit timestamp
         * @param mutations transaction write set
         */
        @Override
        public void directCommit(final long txId, final long startTs, final long commitTs, final List<Mutation> mutations) {
            failOnce();
            delegate.directCommit(txId, startTs, commitTs, mutations);
        }

        /**
         * Press write set to forward intent cleanup.
         *
         * @param txId transaction identifier
         * @param mutations transaction write set
         */
        @Override
        public void cleanupIntents(final long txId, final List<Mutation> mutations) {
            delegate.cleanupIntents(txId, mutations);
        }

        /**
         * Press transaction identifier to forward intent cleanup.
         *
         * @param txId transaction identifier
         */
        @Override
        public void cleanupIntents(final long txId) {
            delegate.cleanupIntents(txId);
        }

        /**
         * All commit reloads share a one-time failure, and failure occurs before the real storage calls.
         *
         * @throws IllegalStateException if the operation cannot complete
         */
        private void failOnce() {
            if (shouldFail.compareAndSet(true, false)) {
                throw new IllegalStateException("simulated commit failure");
            }
        }

        /**
         * Roll back the transaction and remove its intents.
         *
         * @param txId transaction identifier
         */
        @Override
        public void abort(final long txId) {
            delegate.abort(txId);
        }

        /**
         * Find transaction identifiers with pending intents.
         *
         * @param limit maximum number of items
         * @return the operation result
         */
        @Override
        public Set<Long> findPendingTransactionIds(final int limit) {
            return delegate.findPendingTransactionIds(limit);
        }

        /**
         * Collect obsolete versions before the safe point.
         *
         * @param safePoint oldest retained snapshot timestamp
         * @param limit maximum number of items
         * @return the operation result
         */
        @Override
        public int gc(final long safePoint, final int limit) {
            return delegate.gc(safePoint, limit);
        }
    }
}
