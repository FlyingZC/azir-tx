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

import java.util.Objects;
import java.util.UUID;

/**
 * A non-mixable backend recovery identity, binds backup, engines, shard and confirms the commit border.
 *
 * @param backupId backup id
 * @param operationId operation id
 * @param engineId engine id
 * @param shard logical shard identifier
 * @param boundary boundary
 * @param replica replica
 */
public record WalRestoreSpec(UUID backupId, UUID operationId, UUID engineId, String shard, WalCheckpoint boundary, WalReplicaIdentity replica) {

    /**
     * The operation identity and format of the original single machine recovery remain unchanged.
     *
     * @param backupId backup id
     * @param operationId operation id
     * @param engineId engine id
     * @param shard logical shard identifier
     * @param boundary boundary
     */
    public WalRestoreSpec(final UUID backupId, final UUID operationId, final UUID engineId, final String shard, final WalCheckpoint boundary) {
        this(backupId, operationId, engineId, shard, boundary, null);
    }

    /**
     * Keep a clear operation identity for the independent storage adaptation test and apply the recovery entrance to bind the target directory separately.
     *
     * @param backupId backup id
     * @param engineId engine id
     * @param shard logical shard identifier
     * @param boundary boundary
     */
    public WalRestoreSpec(final UUID backupId, final UUID engineId, final String shard, final WalCheckpoint boundary) {
        this(backupId, backupId, engineId, shard, boundary);
    }

    /**
     * All retry s must be carried with the same identity and border, and cannot take over part recovery with another backup.
     *
     * @param backupId backup id
     * @param operationId operation id
     * @param engineId engine id
     * @param shard logical shard identifier
     * @param boundary boundary
     * @param replica replica
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public WalRestoreSpec {
        Objects.requireNonNull(backupId);
        Objects.requireNonNull(operationId);
        Objects.requireNonNull(engineId);
        Objects.requireNonNull(boundary);
        if (replica != null && !replica.engineId().equals(engineId)) {
            throw new IllegalArgumentException("restore replica engine mismatch");
        }
        new WalMutation(shard, "", null);
        if (shard.contains("\u0000") || boundary.sequence() < 0
                || !(boundary.sequence() == 0 ? boundary.digest().isEmpty() : boundary.digest().matches("[a-f0-9]{64}"))) {
            throw new IllegalArgumentException("invalid restore identity");
        }
    }
}
