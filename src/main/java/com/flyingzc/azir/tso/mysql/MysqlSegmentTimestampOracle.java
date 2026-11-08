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

package com.flyingzc.azir.tso.mysql;

import com.flyingzc.azir.tso.TimestampOracle;

import javax.sql.DataSource;
import java.util.concurrent.atomic.LongAdder;

/**
 * Compatibility entry point for the former range allocator. Every call now delegates to the durable global sequence: per-client range caches violate cross-instance real-time ordering. The
 * batch-size argument is retained only for source compatibility.
 */
public final class MysqlSegmentTimestampOracle implements TimestampOracle, MysqlTimestampAuthority {

    private final MysqlTimestampOracle delegate;

    private final LongAdder allocations = new LongAdder();

    /**
     * Validate the legacy batch-size argument and create a strictly durable timestamp allocator.
     *
     * @param dataSource database connection source
     * @param batchSize batch size
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public MysqlSegmentTimestampOracle(final DataSource dataSource, final int batchSize) {
        if (batchSize < 1) {
            throw new IllegalArgumentException("batchSize must be >= 1");
        }
        delegate = new MysqlTimestampOracle(dataSource);
    }

    /**
     * Use the same control pool as the underlying per-call timestamp allocator.
     *
     * @param dataSource database connection source
     * @return the operation result
     */
    @Override
    public boolean usesDataSource(final DataSource dataSource) {
        return delegate.usesDataSource(dataSource);
    }

    /**
     * Initialize the global sequence in the same database without reserving a local timestamp range.
     *
     * @param initialValue initial value
     */
    public void initialize(final long initialValue) {
        delegate.initialize(initialValue);
    }

    /**
     * Allocate each timestamp from the durable sequence shared by all clients, reusing the publication-barrier connection.
     *
     * @return the operation result
     */
    @Override
    public long nextTimestamp() {
        long timestamp = delegate.nextTimestamp();
        allocations.increment();
        return timestamp;
    }

    /**
     * Keep the legacy statistics method name; it now counts successful individual database allocations, each of size one.
     *
     * @return the operation result
     */
    public long segmentAllocations() {
        return allocations.sum();
    }
}
