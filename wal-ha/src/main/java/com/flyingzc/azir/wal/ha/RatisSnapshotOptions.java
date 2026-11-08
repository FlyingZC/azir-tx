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

/**
 * Automatic mirror, log watermark and tail configuration with time in milliseconds and capacity in bytes; end at high watermark leave safe collection.
 *
 * @param intervalMillis interval millis
 * @param entryGap entry gap
 * @param maxIntervalMillis max interval millis
 * @param logHighWaterBytes log high water bytes
 * @param preserveEntries preserve entries
 */
public record RatisSnapshotOptions(long intervalMillis, long entryGap, long maxIntervalMillis, long logHighWaterBytes, long preserveEntries) {

    /**
     * The upper boundary is used for collaborative backpression, and the actual disk limit must also include a line of en route logs and mirror files.
     *
     * @param intervalMillis interval millis
     * @param entryGap entry gap
     * @param maxIntervalMillis max interval millis
     * @param logHighWaterBytes log high water bytes
     * @param preserveEntries preserve entries
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public RatisSnapshotOptions {
        if (intervalMillis < 100 || intervalMillis > 60_000 || entryGap < 1 || entryGap > 10_000_000
                || maxIntervalMillis < intervalMillis || maxIntervalMillis > 86_400_000 || logHighWaterBytes < 32L * 1024 * 1024
                || logHighWaterBytes > 1L << 50 || preserveEntries < 0 || preserveEntries > 10_000_000) {
            throw new IllegalArgumentException("invalid snapshot lifecycle limits");
        }
    }

    /**
     * Ten thousand logs or five minutes per second of observation, high log watermark 256 MiB, left at the end of 1024.
     *
     * @return the operation result
     */
    public static RatisSnapshotOptions defaults() {
        return new RatisSnapshotOptions(1000, 10_000, 300_000, 256L * 1024 * 1024, 1024);
    }
}
