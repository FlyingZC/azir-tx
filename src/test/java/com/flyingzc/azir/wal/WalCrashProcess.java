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

import com.flyingzc.azir.common.IsolationLevel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

/**
 * Stop at commit border in the real son JVM, Father process uses to kill verify not executed closed recovery.
 */
public final class WalCrashProcess {

    /**
     * Prohibits case-independent processes probe.
     */
    private WalCrashProcess() {
    }

    /**
     * Writes an cross-shard transaction to block a parent process after specifying the durable border.
     *
     * @param args args
     * @throws Exception if the operation cannot complete
     */
    public static void main(final String[] args) throws Exception {
        Path directory = Path.of(args[0]);
        String stageName = args[1];
        var hook = (WalMvccRuntime.FaultHook) stage -> {
            if (stage.name().equals(stageName)) {
                signalAndWait(directory);
            }
        };
        try (var runtime = WalMvccRuntime.openForTest(directory, Set.of("a", "b"), Map.of(), WalOptions.defaults(), hook)) {
            var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
            Files.writeString(directory.resolve("transaction-id"), tx.id().toString());
            runtime.put(tx, "a", "balance", new byte[]{90});
            runtime.put(tx, "b", "balance", new byte[]{110});
            runtime.commit(tx);
            if (stageName.equals("ACK")) {
                signalAndWait(directory);
            }
        }
    }

    /**
     * Notification of the arrival of the father ' s process boundary and irregular withdrawal or release of document locks during obstruction.
     *
     * @param directory directory
     * @throws IllegalStateException if the operation cannot complete
     */
    private static void signalAndWait(final Path directory) {
        try {
            Files.writeString(directory.resolve("ready"), "ready");
            new CountDownLatch(1).await();
        } catch (final Exception ex) {
            throw new IllegalStateException(ex);
        }
    }
}
