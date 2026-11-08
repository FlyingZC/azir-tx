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

package com.flyingzc.azir.tso;

import java.util.Objects;

/**
 * Compatibility wrapper that delegates every allocation to the authoritative TSO. Client-side prefetching is disabled because it violates cross-instance ordering.
 */
public final class BatchingTimestampOracle implements TimestampOracle {

    private final TimestampOracle delegate;

    /**
     * Construct a BatchingTimestampOracle instance.
     *
     * @param delegate delegate
     * @param batchSize batch size
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public BatchingTimestampOracle(final TimestampOracle delegate, final int batchSize) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        if (batchSize < 1) {
            throw new IllegalArgumentException("batchSize must be >= 1");
        }
    }

    /**
     * Allocate the next globally increasing timestamp.
     *
     * @return the operation result
     */
    @Override
    public long nextTimestamp() {
        return delegate.nextTimestamp();
    }
}
