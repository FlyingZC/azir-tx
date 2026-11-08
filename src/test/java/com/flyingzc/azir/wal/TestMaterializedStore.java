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

package com.flyingzc.azir.wal;

import com.flyingzc.azir.storage.Utf8KeyOrder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.UUID;

/**
 * Determines the materialization test double, which is used to control failure and check reading, collection staggering.
 */
final class TestMaterializedStore implements WalMaterializedStore {

    final Map<String, NavigableMap<Long, WalValue>> versions = new HashMap<>();

    volatile boolean failApply;

    volatile boolean loseApplyResponse;

    volatile Runnable onRead = () -> {
    };

    volatile Runnable onApply = () -> {
    };

    private UUID engine;

    private String shard;

    private WalCheckpoint checkpoint = new WalCheckpoint(0, "");

    /**
     * Bind the test backend and refuse to reuse a different log identity.
     *
     * @param engineId engine id
     * @param shard logical shard identifier
     * @throws IllegalStateException if the operation cannot complete
     */
    @Override
    public synchronized void initialize(final UUID engineId, final String shard) {
        if (engine != null && (!engine.equals(engineId) || !this.shard.equals(shard))) {
            throw new IllegalStateException("test store binding mismatch");
        }
        engine = engineId;
        this.shard = shard;
    }

    /**
     * Returns the prefix already applied.
     *
     * @return the operation result
     */
    @Override
    public synchronized WalCheckpoint checkpoint() {
        return checkpoint;
    }

    /**
     * Applys the full log and can simulate post-application response loss.
     *
     * @param commits commits
     * @return the operation result
     * @throws IllegalStateException if the operation cannot complete
     */
    @Override
    public synchronized WalCheckpoint apply(final List<WalCommit> commits) {
        onApply.run();
        if (failApply) {
            throw new IllegalStateException("paused materializer");
        }
        for (WalCommit commit : commits) {
            if (commit.sequence() <= checkpoint.sequence()) {
                continue;
            }
            if (commit.sequence() != checkpoint.sequence() + 1) {
                throw new IllegalStateException("log gap");
            }
            for (WalMutation mutation : commit.mutations()) {
                if (mutation.shard().equals(shard)) {
                    versions.computeIfAbsent(mutation.key(), ignored -> new TreeMap<>())
                            .put(commit.sequence(), new WalValue(commit.sequence(), mutation.value()));
                }
            }
            checkpoint = new WalCheckpoint(commit.sequence(), commit.digest());
        }
        if (loseApplyResponse) {
            loseApplyResponse = false;
            throw new IllegalStateException("lost apply response");
        }
        return checkpoint;
    }

    /**
     * Returns snapshot version and allows injection to read delay.
     *
     * @param key business key
     * @param readSequence read sequence
     * @return the operation result
     */
    @Override
    public synchronized Optional<WalValue> get(final String key, final long readSequence) {
        var chain = versions.get(key);
        var entry = chain == null ? null : chain.floorEntry(readSequence);
        onRead.run();
        return entry == null ? Optional.empty() : Optional.of(entry.getValue());
    }

    /**
     * Returns the candidate line containing tombstone in a semi-open space.
     *
     * @param from from
     * @param to to
     * @param readSequence read sequence
     * @return the operation result
     */
    @Override
    public synchronized NavigableMap<String, WalValue> scan(final String from, final String to, final long readSequence) {
        var result = new TreeMap<String, WalValue>(Utf8KeyOrder::compare);
        versions.forEach((key, chain) -> {
            if ((from == null || Utf8KeyOrder.compare(key, from) >= 0)
                    && (to == null || Utf8KeyOrder.compare(key, to) < 0)) {
                var entry = chain.floorEntry(readSequence);
                if (entry != null) {
                    result.put(key, entry.getValue());
                }
            }
        });
        onRead.run();
        return result;
    }

    /**
     * Retains the active safe point anchor, tombstone anchor and old history retirement, replay still protected by checkpoints.
     *
     * @param safePoint oldest retained snapshot timestamp
     * @param keyLimit key limit
     * @return the operation result
     */
    @Override
    public synchronized int gc(final long safePoint, final int keyLimit) {
        int removed = 0;
        int keys = 0;
        var iterator = versions.values().iterator();
        while (iterator.hasNext()) {
            var chain = iterator.next();
            if (keys++ >= keyLimit) {
                break;
            }
            Long anchor = chain.floorKey(safePoint);
            if (anchor != null) {
                boolean retired = chain.get(anchor).deleted();
                removed += chain.headMap(anchor, retired).size();
                chain.headMap(anchor, retired).clear();
                if (chain.isEmpty()) {
                    iterator.remove();
                }
            }
        }
        return removed;
    }
}
