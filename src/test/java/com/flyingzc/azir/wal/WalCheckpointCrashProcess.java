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
 * Stop at the real checkpoint where you publish or write some of the files, and the fathering process will kill recovery.
 */
public final class WalCheckpointCrashProcess {

    /**
     * The independent process probe does not allow for exemplification.
     */
    private WalCheckpointCrashProcess() {
    }

    /**
     * Two cross-shard transaction confirmed, then stopped at the specified checkpoint border and could not be replaced by normal closure.
     *
     * @param args args
     * @throws AssertionError if the operation cannot complete
     * @throws Exception if the operation cannot complete
     */
    public static void main(final String[] args) throws Exception {
        Path root = Path.of(args[0]);
        String target = args[1];
        var io = new WalLog.IoHook() {

            /**
             * Cuts the corresponding file to a given location and ensures that the parent process kills part of the body.
             *
             * @param name name
             * @param position position
             * @param remaining remaining
             * @return the operation result
             */
            @Override
            public int checkpointWriteLimit(final String name, final long position, final int remaining) {
                return matches(target, name) ? Math.min(remaining, 32) : remaining;
            }

            /**
             * Part of the content is actually written before informing the father of the process.
             *
             * @param name name
             * @param position position
             */
            @Override
            public void afterCheckpointWrite(final String name, final long position) {
                if (matches(target, name)) {
                    signal(root);
                }
            }
        };
        try (
                var runtime = WalMvccRuntime.openForTest(root, Set.of("a", "b"), Map.of(), WalOptions.defaults(),
                        stage -> {
                            if (stage.name().equals(target)) {
                                signal(root);
                            }
                        }, io, System::nanoTime)) {
            for (String value : new String[]{"old", "new"}) {
                var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
                runtime.put(tx, "a", "k", value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
                runtime.put(tx, "b", "k", value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
                runtime.commit(tx);
                Files.writeString(root.resolve(value + "-id"), tx.id().toString());
            }
            runtime.collectGarbage(32);
            runtime.checkpoint();
            throw new AssertionError("checkpoint barrier not reached");
        }
    }

    /**
     * Injection will only match this specified checkpoint file and will not interfere with the transaction log that was previously confirmed.
     *
     * @param target target
     * @param file file
     * @return the operation result
     */
    private static boolean matches(final String target, final String file) {
        return target.equals("PARTIAL_IMAGE") && file.equals("snapshot.bin")
                || target.equals("PARTIAL_RECEIPT") && file.startsWith("receipts-")
                || target.equals("PARTIAL_POINTER") && file.startsWith("CURRENT-");
    }

    /**
     * Repeated waiting to be forcefully killed and irregular exit document locks or completions were not completed for writing.
     *
     * @param root root
     * @throws IllegalStateException if the operation cannot complete
     */
    private static void signal(final Path root) {
        try {
            Files.writeString(root.resolve("ready"), "ready");
            new CountDownLatch(1).await();
        } catch (final Exception failure) {
            throw new IllegalStateException(failure);
        }
    }
}
