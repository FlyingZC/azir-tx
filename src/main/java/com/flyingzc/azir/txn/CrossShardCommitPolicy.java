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

package com.flyingzc.azir.txn;

/**
 * Cross-shard commit policy, defining step concurrent and step commit behavior.
 *
 * @param parallelPrewrite parallel prewrite
 * @param parallelPrepare parallel prepare
 * @param parallelCommit parallel commit
 * @param parallelRollback parallel rollback
 * @param asyncCommit async commit
 * @param asyncCommitWorkers async commit workers
 */
public record CrossShardCommitPolicy(boolean parallelPrewrite,
        boolean parallelPrepare,
        boolean parallelCommit,
        boolean parallelRollback,
        boolean asyncCommit,
        int asyncCommitWorkers) {

    /**
     * Construct a CrossShardCommitPolicy instance.
     *
     * @param parallelPrewrite parallel prewrite
     * @param parallelPrepare parallel prepare
     * @param parallelCommit parallel commit
     * @param parallelRollback parallel rollback
     * @param asyncCommit async commit
     * @param asyncCommitWorkers async commit workers
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public CrossShardCommitPolicy {
        if (asyncCommitWorkers < 1) {
            throw new IllegalArgumentException("asyncCommitWorkers must be >= 1");
        }
    }

    /**
     * Returns a serial cross-shard commit policy.
     *
     * @return the operation result
     */
    public static CrossShardCommitPolicy serial() {
        return new CrossShardCommitPolicy(false, false, false, false, false, 1);
    }

    /**
     * Returns a parallel cross-shard commit policy.
     *
     * @return the operation result
     */
    public static CrossShardCommitPolicy parallel() {
        return new CrossShardCommitPolicy(true, true, true, true, false, 1);
    }

    /**
     * Returns the step-by-step cross-shard commit policy.
     *
     * @param asyncCommitWorkers async commit workers
     * @return the operation result
     */
    public static CrossShardCommitPolicy asyncParallel(final int asyncCommitWorkers) {
        return new CrossShardCommitPolicy(true, true, true, true, true, asyncCommitWorkers);
    }
}
