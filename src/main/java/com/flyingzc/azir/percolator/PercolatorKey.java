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

package com.flyingzc.azir.percolator;

import com.flyingzc.azir.storage.Utf8KeyOrder;
import java.util.Objects;

/**
 * The complete address of the Percolator key, shard number is used for the sole location of the Primary.
 *
 * @param shard logical shard identifier
 * @param key business key
 */
public record PercolatorKey(String shard, String key) implements Comparable<PercolatorKey> {

    /**
     * Validate shard name and MySQL binary key length to avoid cut-off or coded collisions.
     *
     * @param shard logical shard identifier
     * @param key business key
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public PercolatorKey {
        Objects.requireNonNull(shard, "shard");
        if (!shard.matches("[A-Za-z0-9._-]{1,64}")) {
            throw new IllegalArgumentException("shard must match [A-Za-z0-9._-]{1,64}");
        }
        Objects.requireNonNull(key, "key");
        if (key.length() > 512 || Utf8KeyOrder.encode(key).length > 512) {
            throw new IllegalArgumentException("key exceeds 512 UTF-8 bytes");
        }
    }

    /**
     * Press the fixed shard and UTF-8 key to lock and reduce the dead lock with shard batch transaction.
     *
     * @param other other
     * @return the operation result
     */
    @Override
    public int compareTo(final PercolatorKey other) {
        int order = shard.compareTo(other.shard);
        return order == 0 ? Utf8KeyOrder.compare(key, other.key) : order;
    }
}
