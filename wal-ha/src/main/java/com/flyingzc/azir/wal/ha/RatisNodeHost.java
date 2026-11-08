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

package com.flyingzc.azir.wal.ha;

import com.zaxxer.hikari.HikariDataSource;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * Owns configured node resources and releases connection pools only after node cleanup completes.
 */
final class RatisNodeHost implements AutoCloseable {

    private final RatisNodeConfiguration config;

    private final List<HikariDataSource> pools = new ArrayList<>();

    private volatile RatisManagedNode node;

    private RatisStartupException pendingStartup;

    private RatisNodeApplication application;

    private RatisNodeHealthServer health;

    private volatile boolean started, closing, failed;

    private boolean attempted, uncertainStartup;

    /**
     * The construction process does not start a thread or connection, first lets the main install off hooks before starting.
     *
     * @param config config
     */
    RatisNodeHost(final RatisNodeConfiguration config) {
        this.config = config;
    }

    /**
     * The host does not support reopening after failure and members identity, durable policy and recovery remain at the existing hosting entrance.
     *
     * @throws Exception if the operation cannot complete
     * @throws IllegalStateException if the operation cannot complete
     */
    synchronized void start() throws Exception {
        if (attempted || closing) {
            throw new IllegalStateException("node host cannot restart in the same process");
        }
        attempted = true;
        try {
            RatisNodeHealthServer.requireBudgets();
            Constructor<? extends RatisNodeApplication> constructor = applicationConstructor();
            var byName = new LinkedHashMap<String, HikariDataSource>();
            for (var store : config.stores) {
                var pool = store.pool();
                pools.add(pool);
                byName.put(store.name(), pool);
            }
            var routes = new LinkedHashMap<String, HikariDataSource>();
            config.routes.forEach((shard, store) -> routes.put(shard, byName.get(store)));
            var managedConfig = config.managed(routes);
            // The outer layer cannot prove that all the internal resources have been cleared without the return handle of the submersible.
            // At this point, the control pool is maintained until the process to which it belongs exits and the reopening of the directory in the same JVM is prohibited.
            uncertainStartup = true;
            try {
                node = RatisManagedNode.open(managedConfig);
                uncertainStartup = false;
            } catch (final RatisStartupException pending) {
                pendingStartup = pending;
                uncertainStartup = false;
                throw pending;
            }
            // Probe First-instance port, which is not available after the start of business has failed; operations are not fully operational.
            health = new RatisNodeHealthServer(config, this);
            if (constructor != null) {
                application = constructor.newInstance();
                application.start(node);
            }
            started = true;
        } catch (final Exception | Error failure) {
            failed = true;
            throw failure;
        }
    }

    /**
     * Verify plugin type and open unincorporated builder before exposure to database, does not automatically scan class paths.
     *
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     * @throws ReflectiveOperationException if the operation cannot complete
     */
    private Constructor<? extends RatisNodeApplication> applicationConstructor() throws ReflectiveOperationException {
        if (config.applicationClass.isEmpty()) {
            return null;
        }
        var type = Class.forName(config.applicationClass, false, RatisNodeHost.class.getClassLoader()).asSubclass(RatisNodeApplication.class);
        if (!Modifier.isPublic(type.getModifiers()) || Modifier.isAbstract(type.getModifiers())) {
            throw new IllegalArgumentException("application must be a public concrete class");
        }
        return type.getConstructor();
    }

    /**
     * Cache-reading, storage or maintenance errors and expired samples cannot continue to claim that the business is ready.
     *
     * @return the operation result
     */
    Status status() {
        var current = node;
        var status = current == null ? null : current.status();
        var storage = status == null ? null : status.storage();
        boolean maintenanceFailed = status != null && (status.error() != null || status.health().error() != null
                || status.gc().error() != null || status.gc().stale() || status.snapshot().error() != null
                || storage != null && storage.maintenanceFailure() != null);
        boolean materializationFailed = storage != null && storage.materializationFailure() != null;
        boolean backpressure = status != null && status.snapshot().backpressure();
        boolean live = started && !closing && !failed;
        boolean ready = live && status != null && status.localReady() && !status.stale() && storage != null
                && !status.health().quarantined() && !maintenanceFailed && !materializationFailed;
        return new Status(live, ready, ready && status.writableHint() && !backpressure, closing, failed,
                status == null ? Long.MAX_VALUE : status.ageMillis(), storage == null ? -1 : storage.publishedSequence(),
                storage == null ? -1 : storage.materializationLag(), storage == null ? -1 : storage.walBytes(),
                storage == null ? -1 : storage.hotCacheBytes(), storage == null ? -1 : storage.safePoint(),
                storage == null ? -1 : storage.retainedBytes(), storage == null ? -1 : storage.bufferedBytes(),
                status == null ? -1 : status.snapshot().raftLogBytes(), storage == null ? -1 : storage.receiptArchiveBytes(),
                storage == null ? -1 : storage.oldestActiveMillis(), config.security.certificateExpiresAt().toEpochMilli(),
                storage == null ? 0 : storage.activeTransactions(),
                storage == null ? 0 : storage.queuedCommits(), maintenanceFailed, materializationFailed, backpressure);
    }

    /**
     * Operations were stopped and empty; any failure of steps retained the uncleaned handle and could not pretend that resources had been released.
     *
     * @throws Exception if the operation cannot complete
     * @throws IOException if the operation cannot complete
     */
    @Override
    public synchronized void close() throws Exception {
        closing = true;
        if (application != null) {
            application.close();
            application = null;
        }
        if (node != null) {
            node.close();
            node = null;
        }
        if (pendingStartup != null) {
            pendingStartup.retryCleanup();
            pendingStartup = null;
        }
        if (uncertainStartup) {
            throw new IOException("startup cleanup cannot be proven; terminate the owning process before restart");
        }
        for (var iterator = pools.iterator(); iterator.hasNext();) {
            iterator.next().close();
            iterator.remove();
        }
        if (health != null) {
            health.close();
            health = null;
        }
    }

    /**
     * The publicly releasable fixed field is a boolean or a numeric, unsequenced nuclear abnormality message and business data.
     *
     * @param live live
     * @param ready ready
     * @param writableHint writable hint
     * @param closing closing
     * @param failed failed
     * @param sampleAgeMillis sample age millis
     * @param publishedSequence published sequence
     * @param materializationLag materialization lag
     * @param walBytes wal bytes
     * @param hotCacheBytes hot cache bytes
     * @param safePoint oldest retained snapshot timestamp
     * @param retainedBytes retained bytes
     * @param bufferedBytes buffered bytes
     * @param raftLogBytes raft log bytes
     * @param receiptArchiveBytes receipt archive bytes
     * @param oldestActiveMillis oldest active millis
     * @param certificateExpiresAtEpochMillis certificate expires at epoch millis
     * @param activeTransactions active transactions
     * @param queuedCommits queued commits
     * @param maintenanceFailed maintenance failed
     * @param materializationFailed materialization failed
     * @param snapshotBackpressure snapshot backpressure
     */
    record Status(boolean live, boolean ready, boolean writableHint, boolean closing, boolean failed,
            long sampleAgeMillis, long publishedSequence, long materializationLag, long walBytes,
            long hotCacheBytes, long safePoint, long retainedBytes, long bufferedBytes, long raftLogBytes,
            long receiptArchiveBytes, long oldestActiveMillis, long certificateExpiresAtEpochMillis,
            int activeTransactions, int queuedCommits, boolean maintenanceFailed,
            boolean materializationFailed, boolean snapshotBackpressure) {

        /**
         * Fixed JSON fields do not need to introduce a General Serializer, nor will they call external objects toString.
         *
         * @return the operation result
         */
        String json() {
            return "{\"live\":" + live + ",\"ready\":" + ready + ",\"writableHint\":" + writableHint
                    + ",\"closing\":" + closing + ",\"failed\":" + failed + ",\"sampleAgeMillis\":" + sampleAgeMillis
                    + ",\"publishedSequence\":" + publishedSequence + ",\"materializationLag\":" + materializationLag
                    + ",\"walBytes\":" + walBytes + ",\"hotCacheBytes\":" + hotCacheBytes + ",\"activeTransactions\":" + activeTransactions
                    + ",\"safePoint\":" + safePoint + ",\"retainedBytes\":" + retainedBytes + ",\"bufferedBytes\":" + bufferedBytes
                    + ",\"raftLogBytes\":" + raftLogBytes + ",\"receiptArchiveBytes\":" + receiptArchiveBytes
                    + ",\"oldestActiveMillis\":" + oldestActiveMillis + ",\"certificateExpiresAtEpochMillis\":" + certificateExpiresAtEpochMillis
                    + ",\"queuedCommits\":" + queuedCommits + ",\"maintenanceFailed\":" + maintenanceFailed
                    + ",\"materializationFailed\":" + materializationFailed + ",\"snapshotBackpressure\":" + snapshotBackpressure + "}";
        }
    }
}
