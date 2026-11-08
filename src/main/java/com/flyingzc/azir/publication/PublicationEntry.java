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

package com.flyingzc.azir.publication;

/**
 * Durable storage transaction identity, terminal state and take-over deadline for each commit timestamp.
 *
 * @param txId transaction identifier
 * @param commitTs durable commit timestamp
 * @param status status
 * @param expiresAtMillis expires at millis
 */
public record PublicationEntry(long txId, long commitTs, PublicationEntryStatus status, long expiresAtMillis) {
}
