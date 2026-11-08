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

package com.flyingzc.azir.wal.ha;

import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.storage.RaftStorage;
import java.io.IOException;
import java.nio.file.Path;

/**
 * Synchronise files created and atomic replaced directories after the official log and voting file is force.
 */
final class RaftDurability {

    /**
     * Prevent instantiation of this utility class.
     */
    private RaftDurability() {
    }

    /**
     * The initial synchronisation completes the local directory chain, followed by an ordinary addition that only syncs a name change within the current.
     *
     * @param storage storage
     * @throws IOException if the operation cannot complete
     */
    static void initialize(final RaftStorage storage) throws IOException {
        Path current = storage.getStorageDir().getCurrentDir().toPath();
        for (int i = 0; i < 5 && current != null; i++, current = current.getParent()) {
            RatisGenerationRecord.forceDirectory(current);
        }
    }

    /**
     * Synchronizing directories for voting metadata, configurations and log segments cannot replace directory entries with closed files.
     *
     * @param server server
     * @param group group
     * @throws IOException if the operation cannot complete
     */
    static void current(final RaftServer server, final RaftGroupId group) throws IOException {
        RatisGenerationRecord.forceDirectory(server.getDivision(group).getRaftStorage().getStorageDir().getCurrentDir().toPath());
    }
}
