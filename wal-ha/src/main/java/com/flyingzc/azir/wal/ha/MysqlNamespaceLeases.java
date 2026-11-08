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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.io.IOException;

/**
 * Share space with all JVM factories lease, Node directory locks to cross the same nodes of process.
 */
final class MysqlNamespaceLeases {

    private static final Map<String, Entry> ENTRIES = new HashMap<>();

    /**
     * Prevent instantiation of this utility class.
     */
    private MysqlNamespaceLeases() {
    }

    /**
     * Atomic at a time registers all shard s and cannot only protect the first one that has been opened.
     *
     * @param names names
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    static synchronized Guard open(final Collection<String> names) throws IOException {
        var keys = Set.copyOf(names);
        for (String key : keys) {
            var entry = ENTRIES.get(key);
            if (entry != null && entry.retiring) {
                throw new IOException("namespace retirement in progress");
            }
        }
        for (String key : keys) {
            ENTRIES.computeIfAbsent(key, ignored -> new Entry()).leases++;
        }
        return new Guard(keys, false);
    }

    /**
     * Confirm that all pieces are inactive before deleting any of them.
     *
     * @param names names
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    static synchronized Guard retire(final Collection<String> names) throws IOException {
        var keys = Set.copyOf(names);
        for (String key : keys) {
            var entry = ENTRIES.get(key);
            if (entry != null && (entry.leases > 0 || entry.retiring)) {
                throw new IOException("cannot retire a leased namespace");
            }
        }
        for (String key : keys) {
            ENTRIES.computeIfAbsent(key, ignored -> new Entry()).retiring = true;
        }
        return new Guard(keys, true);
    }

    /**
     * No retirement at active lease, no new lease can be started when retirement is active.
     */
    private static final class Entry {

        int leases;

        boolean retiring;
    }

    /**
     * Removes the memory register after all closes, without leaving history tombstone in a stack.
     */
    static final class Guard implements AutoCloseable {

        final Set<String> keys;

        final boolean retiring;

        boolean closed;

        /**
         * Guard can only be created by the above-mentioned atomic registration.
         *
         * @param keys keys
         * @param retiring retiring
         */
        Guard(final Set<String> keys, final boolean retiring) {
            this.keys = keys;
            this.retiring = retiring;
        }

        /**
         * Release idempotently All shard registrations.
         */
        @Override
        public void close() {
            synchronized (MysqlNamespaceLeases.class) {
                if (closed) {
                    return;
                }
                closed = true;
                for (String key : keys) {
                    var entry = ENTRIES.get(key);
                    if (retiring) {
                        entry.retiring = false;
                    } else {
                        entry.leases--;
                    }
                    if (entry.leases == 0 && !entry.retiring) {
                        ENTRIES.remove(key);
                    }
                }
            }
        }
    }
}
