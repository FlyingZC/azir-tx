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

package com.flyingzc.azir.metadata;

import com.flyingzc.azir.tso.TimestampOracle;

import java.util.OptionalLong;
import java.util.Set;
import java.util.function.LongConsumer;

/**
 * Select to allocate timestamp and durable storage cross-shard commit decision in the same control operation.
 */
public interface FastTransactionCommit {

    /**
     * Authority returns the empty result when it does not match; after start-up anomalies must be handled by the caller by the unknown decision.
     *
     * @param timestampOracle timestamp oracle
     * @param txId transaction identifier
     * @param participants participants
     * @param beforePublish before publish
     * @param afterCommitted after committed
     * @return the operation result
     */
    OptionalLong tryCommit(TimestampOracle timestampOracle, long txId, Set<String> participants,
                           Runnable beforePublish, LongConsumer afterCommitted);
}
