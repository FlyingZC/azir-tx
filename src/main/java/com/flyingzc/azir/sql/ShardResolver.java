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

package com.flyingzc.azir.sql;

import java.util.List;
import java.util.Objects;

/**
 * The sQL table and primary key to the physical shard resolution interface.
 */
@FunctionalInterface
public interface ShardResolver {

    /**
     * Parsing the physical shard number by watch name and primary key.
     *
     * @param table table
     * @param key business key
     * @return the operation result
     */
    String resolveShard(String table, String key);

    /**
     * Returns all physical shard to access when scanned the specified logical table.
     *
     * @param table table
     * @return the operation result
     */
    default List<String> resolveShards(final String table) {
        return List.of(resolveShard(table, ""));
    }

    /**
     * Creates a solver for the fixed return single shard.
     *
     * @param shardId logical shard identifier
     * @return the operation result
     */
    static ShardResolver singleShard(final String shardId) {
        return (table, key) -> shardId;
    }

    /**
     * Creates a solver for Hashi to select shard by primary key.
     *
     * @param shardIds shard ids
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    static ShardResolver hashByKey(final List<String> shardIds) {
        List<String> copiedShardIds = List.copyOf(shardIds);
        if (copiedShardIds.isEmpty()) {
            throw new IllegalArgumentException("shardIds must not be empty");
        }
        return new ShardResolver() {

            /**
             * Huxe selects single writing or check shard by primary key.
             *
             * @param table table
             * @param key business key
             * @return the operation result
             */
            @Override
            public String resolveShard(final String table, final String key) {
                return copiedShardIds.get(Math.floorMod(Objects.hashCode(key), copiedShardIds.size()));
            }

            /**
             * A full-form scan must cover all shard s managed by the Hashi rule.
             *
             * @param table table
             * @return the operation result
             */
            @Override
            public List<String> resolveShards(final String table) {
                return copiedShardIds;
            }
        };
    }
}
