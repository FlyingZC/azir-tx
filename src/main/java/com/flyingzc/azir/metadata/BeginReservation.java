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

/**
 * Transaction start result after durable storage snapshot lease.
 *
 * @param txId transaction identifier
 * @param startTs transaction start timestamp
 * @param deadlineMillis deadline millis
 * @param metadataPersisted metadata persisted
 */
public record BeginReservation(long txId, long startTs, long deadlineMillis, boolean metadataPersisted) {
}
