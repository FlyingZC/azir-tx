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

/**
 * The byte, batch and single-telephone budget for stand-alone backups and recovery, do not change the normal transaction configuration.
 *
 * @param maxBytes max bytes
 * @param batchKeys batch keys
 * @param batchBytes batch bytes
 * @param timeoutMillis timeout millis
 */
public record WalBackupOptions(long maxBytes, int batchKeys, long batchBytes, long timeoutMillis) {

    /**
     * Rejects the configuration that cannot be executed by bounded, and single batch of data does not exceed 16 MiB.
     *
     * @param maxBytes max bytes
     * @param batchKeys batch keys
     * @param batchBytes batch bytes
     * @param timeoutMillis timeout millis
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public WalBackupOptions {
        if (maxBytes < 1024 || batchKeys < 1 || batchKeys > 1024 || batchBytes < 1 || batchBytes > 16L * 1024 * 1024
                || timeoutMillis < 1 || timeoutMillis > 86_400_000) {
            throw new IllegalArgumentException("invalid backup limits");
        }
    }

    /**
     * Defaults up to 16 GiB, 256 key and 16 MiB to maintain windows for a maximum of five minutes, with visible adjustments.
     *
     * @return the operation result
     */
    public static WalBackupOptions defaults() {
        return new WalBackupOptions(16L * 1024 * 1024 * 1024, 256, 16L * 1024 * 1024, 300_000);
    }
}
