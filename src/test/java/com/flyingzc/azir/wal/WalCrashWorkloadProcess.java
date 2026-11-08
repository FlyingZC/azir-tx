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
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;

/**
 * Provides continuous cross-shard writing and independently verifiable attempts and confirmation records for random killings at any given moment.
 */
public final class WalCrashWorkloadProcess {

    /**
     * The independent process probe does not allow for exemplification.
     */
    private WalCrashWorkloadProcess() {
    }

    /**
     * The first three confirmations are followed by notification of the father ' s process, and the rest is written into the father ' s process.
     *
     * @param args args
     * @throws Exception if the operation cannot complete
     */
    public static void main(final String[] args) throws Exception {
        Path root = Path.of(args[0]);
        try (
                var events = new PrintWriter(Files.newBufferedWriter(root.resolve("events.tsv"), StandardCharsets.UTF_8), true);
                var runtime = WalMvccRuntime.open(root, Set.of("a", "b"), WalOptions.defaults())) {
            for (int ordinal = 0;; ordinal++) {
                var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
                String key = "k" + ordinal % 7;
                String value = ordinal % 9 == 0 ? "<NULL>" : "v" + ordinal;
                events.println("ATTEMPT\t" + tx.id() + "\t0\t" + key + "\t" + value);
                for (String shard : Set.of("a", "b")) {
                    if (value.equals("<NULL>")) {
                        runtime.delete(tx, shard, key);
                    } else {
                        runtime.put(tx, shard, key, value.getBytes(StandardCharsets.UTF_8));
                    }
                }
                var receipt = runtime.commit(tx);
                events.println("ACK\t" + tx.id() + "\t" + receipt.commitSequence() + "\t" + key + "\t" + value);
                if (ordinal == 2) {
                    Files.writeString(root.resolve("ready"), "ready");
                }
            }
        }
    }
}
