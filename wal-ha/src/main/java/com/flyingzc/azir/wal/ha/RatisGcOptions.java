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
 * Replica version collection single-wheel budget, safe point is calculated by active transaction, and cannot be imposed by configuration.
 *
 * @param intervalMillis interval millis
 * @param keyLimit key limit
 * @param rowLimit row limit
 * @param workMillis work millis
 * @param staleAfterMillis stale after millis
 */
public record RatisGcOptions(long intervalMillis, int keyLimit, int rowLimit, long workMillis, long staleAfterMillis) {

    /**
     * Time budget is collaborative budget, and database calls are also subject to JDBC timeout of the official adapter.
     *
     * @param intervalMillis interval millis
     * @param keyLimit key limit
     * @param rowLimit row limit
     * @param workMillis work millis
     * @param staleAfterMillis stale after millis
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public RatisGcOptions {
        if (intervalMillis < 100 || intervalMillis > 60_000 || keyLimit < 1 || keyLimit > 4096 || rowLimit < 1 || rowLimit > 4096
                || workMillis < 1 || workMillis > 5000 || staleAfterMillis <= intervalMillis || staleAfterMillis > 86_400_000) {
            throw new IllegalArgumentException("invalid replica version GC limits");
        }
    }

    /**
     * A maximum of sixty-four keys per second, database and memory up to one thousand twenty-four version each.
     *
     * @return the operation result
     */
    public static RatisGcOptions defaults() {
        return new RatisGcOptions(1000, 64, 1024, 100, 15_000);
    }
}
