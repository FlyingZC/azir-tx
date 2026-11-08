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

import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;

/**
 * Unique process entry without modifying the original Azir assembly or using a health port as an transaction for writing.
 */
public final class RatisNodeMain {

    /**
     * The entry class does not create examples.
     */
    private RatisNodeMain() {
    }

    /**
     * Foreground Runs for process manager hosting; startup failure must exit the JVM to which it belongs and cannot be restarted in the original directory.
     *
     * @param args args
     */
    public static void main(final String[] args) {
        if (args.length != 2 || !args[0].equals("--config")) {
            System.err.println("usage: azir-wal-node --config /absolute/path/node.properties");
            System.exit(2);
            return;
        }
        try {
            var configuration = RatisNodeConfiguration.load(Path.of(args[1]));
            var host = new RatisNodeHost(configuration);
            Runtime.getRuntime().addShutdownHook(Thread.ofPlatform().name("wal-node-shutdown").unstarted(() -> close(host)));
            host.start();
            System.out.println("WAL node started; use local /ready and /writable probes for cached status");
            new CountDownLatch(1).await();
        } catch (final Exception | Error failure) {
            // Anomalous connection devices and plug-ins may carry passwords or business parameters without printing messages, stacks or cause chains.
            String phase = failure instanceof RatisNodeConfiguration.ConfigurationException invalid ? "; phase=" + invalid.phase() : "";
            System.err.println("WAL node startup/run failed; type=" + failure.getClass().getSimpleName() + phase);
            // Only close hooks will be cleared to avoid repeat waiting and taking two shut-down budget after the current line has failed.
            System.exit(1);
        }
    }

    /**
     * The shutdown failed to make a clear error, processed by the process manager's stop period and without interruption in transiting WAL to disguise success.
     *
     * @param host host
     */
    private static void close(final RatisNodeHost host) {
        try {
            host.close();
        } catch (final Exception | Error failure) {
            System.err.println("WAL node cleanup incomplete; terminate process before restart; type=" + failure.getClass().getSimpleName());
        }
    }
}
