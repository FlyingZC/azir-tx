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

import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * Immutable shard key change, empty value is specifically for the deletion of tombstone.
 *
 * @param shard logical shard identifier
 * @param key business key
 * @param value value to write
 */
public record WalMutation(String shard, String key, byte[] value) {

    /**
     * Validate key encodes the upper limit and copys the caller array to avoid external changes in log contents.
     *
     * @param shard logical shard identifier
     * @param key business key
     * @param value value to write
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public WalMutation {
        Objects.requireNonNull(shard, "shard");
        Objects.requireNonNull(key, "key");
        if (shard.isEmpty() || shard.getBytes(StandardCharsets.UTF_8).length > 128
                || key.getBytes(StandardCharsets.UTF_8).length > 512
                || !StandardCharsets.UTF_8.newEncoder().canEncode(shard)
                || !StandardCharsets.UTF_8.newEncoder().canEncode(key)) {
            throw new IllegalArgumentException("invalid UTF-8 shard or key");
        }
        value = value == null ? null : value.clone();
    }

    /**
     * Return value replica, keep the write set immutable frozen.
     *
     * @return the operation result
     */
    @Override
    public byte[] value() {
        return value == null ? null : value.clone();
    }

    /**
     * Return whether this is a deletion tombstone.
     *
     * @return the operation result
     */
    public boolean deleted() {
        return value == null;
    }

    /**
     * Returns immutable payload length, capacity validate does not require copying of the whole data.
     *
     * @return the operation result
     */
    public int valueLength() {
        return value == null ? 0 : value.length;
    }

    /**
     * Returns the size of the resource account, which contains a conservative fixed cost per version.
     *
     * @return the operation result
     */
    public long retainedBytes() {
        return 128L + shard.getBytes(StandardCharsets.UTF_8).length
                + key.getBytes(StandardCharsets.UTF_8).length + (value == null ? 0 : value.length);
    }
}
