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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;

/**
 * Examples of new modes that can be directly run, demonstrating local durable commit and cross-shard transfers.
 */
public final class WalMvccExample {

    /**
     * Example parameter formats; transaction period uses milliseconds and default values are used when omitted.
     */
    private static final String USAGE = "用法：WalMvccExample [WAL目录] [--timeout-millis 毫秒数]";

    /**
     * Prohibits the entry of example examples.
     */
    private WalMvccExample() {
    }

    /**
     * Starts the example with an optional WAL directory and transaction period, creating a temporary isolated directory when no directory has been specified.
     *
     * @param args args
     * @throws Exception if the operation cannot complete
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public static void main(final String[] args) throws Exception {
        Path directory = null;
        WalOptions options = WalOptions.defaults();
        for (int index = 0; index < args.length; index++) {
            switch (args[index]) {
                case "--timeout-millis" -> {
                    if (++index >= args.length) {
                        throw new IllegalArgumentException("--timeout-millis 缺少毫秒数。" + USAGE);
                    }
                    try {
                        options = options.withTimeout(Long.parseLong(args[index]));
                    } catch (final IllegalArgumentException failure) {
                        throw new IllegalArgumentException("事务期限必须为 1..86400000 的整数毫秒数。" + USAGE, failure);
                    }
                }
                default -> {
                    if (args[index].startsWith("--") || directory != null) {
                        throw new IllegalArgumentException("未知参数或重复的 WAL 目录：" + args[index] + "。" + USAGE);
                    }
                    directory = Path.of(args[index]);
                }
            }
        }
        // All validate arguments are created only after success, avoiding invalid debug configuration leaving a temporary file.
        if (directory == null) {
            directory = Files.createTempDirectory("azir-wal-example-");
        }
        System.out.println("transactionTimeoutMillis=" + options.transactionTimeoutMillis());
        try (var runtime = WalMvccRuntime.open(directory, Set.of("left", "right"), options)) {
            var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
            int left = runtime.get(tx, "left", "balance").map(WalMvccExample::integer).orElse(100);
            int right = runtime.get(tx, "right", "balance").map(WalMvccExample::integer).orElse(100);
            runtime.put(tx, "left", "balance", bytes(left - 10));
            runtime.put(tx, "right", "balance", bytes(right + 10));
            WalReceipt receipt = runtime.commit(tx);
            var reader = runtime.begin(IsolationLevel.REPEATABLE_READ);
            System.out.println("walDirectory=" + directory.toAbsolutePath());
            System.out.println("receipt=" + receipt);
            System.out.println("left=" + integer(runtime.get(reader, "left", "balance").orElseThrow())
                    + ", right=" + integer(runtime.get(reader, "right", "balance").orElseThrow()));
            runtime.commit(reader);
            System.out.println("stats=" + runtime.stats());
        }
    }

    /**
     * Encoding the balance as UTF-8 bytes.
     *
     * @param value value to write
     * @return the operation result
     */
    private static byte[] bytes(final int value) {
        return Integer.toString(value).getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Decodes the balance, leaving a simple example of data type in a visible way.
     *
     * @param value value to write
     * @return the operation result
     */
    private static int integer(final byte[] value) {
        return Integer.parseInt(new String(value, StandardCharsets.UTF_8));
    }
}
