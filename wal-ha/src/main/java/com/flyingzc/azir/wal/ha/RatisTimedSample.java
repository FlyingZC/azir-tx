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

import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Publishes an immutable observation together with its generation and monotonic sampling time.
 *
 * @param <T> value type
 * @param value value to write
 * @param capturedNanos captured nanos
 * @param available available
 */
record RatisTimedSample<T>(T value, long capturedNanos, boolean available) {

    /**
     * The absence of a sample status independently indicates that the legitimate nanoTime Zero cannot be mistaken for not having been sampled.
     *
     * @param value value to write
     * @param capturedNanos captured nanos
     * @param available available
     */
    RatisTimedSample {
        Objects.requireNonNull(value);
    }

    /**
     * The state structure was provided on start-up, but no time proof of successful sampling is available.
     *
     * @param <T> value type
     * @param value value to write
     * @return the operation result
     */
    static <T> RatisTimedSample<T> initial(final T value) {
        return new RatisTimedSample<>(value, 0, false);
    }

    /**
     * The time is bound by this observation; the majority certification involved should be introduced at the beginning of the detection, avoiding slow-renewal sampling.
     *
     * @param <T> value type
     * @param value value to write
     * @param capturedNanos captured nanos
     * @return the operation result
     */
    static <T> RatisTimedSample<T> captured(final T value, final long capturedNanos) {
        return new RatisTimedSample<>(value, capturedNanos, true);
    }

    /**
     * Busy or failed only to update the diagnosis and retain the age of the original successful sample.
     *
     * @param value value to write
     * @return the operation result
     */
    RatisTimedSample<T> retaining(final T value) {
        return new RatisTimedSample<>(value, capturedNanos, available);
    }

    /**
     * Caller takes a full snapshot and calculates age without reading another variable time field.
     *
     * @param nowNanos now nanos
     * @return the operation result
     */
    long ageMillis(final long nowNanos) {
        return available ? Math.max(0, TimeUnit.NANOSECONDS.toMillis(nowNanos - capturedNanos)) : Long.MAX_VALUE;
    }
}
