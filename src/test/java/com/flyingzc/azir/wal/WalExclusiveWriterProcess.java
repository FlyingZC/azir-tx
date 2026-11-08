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

import java.io.IOException;
import java.nio.file.Path;
import java.util.Set;

/**
 * Use another JVM verify system level lock to avoid an internal overlap lock table for verify JVM only.
 */
public final class WalExclusiveWriterProcess {

    /**
     * Prohibits the example of monopolization lock probe.
     */
    private WalExclusiveWriterProcess() {
    }

    /**
     * Returns 7 for locking, returns 0 for rejection by an existing writer.
     *
     * @param args args
     * @throws Exception if the operation cannot complete
     */
    public static void main(final String[] args) throws Exception {
        try (var log = new WalLog(Path.of(args[0]), Set.of("a"), 1_000_000)) {
            System.out.println("ACQUIRED");
            System.exit(7);
        } catch (final IOException locked) {
            System.out.println("LOCKED");
        }
    }
}
