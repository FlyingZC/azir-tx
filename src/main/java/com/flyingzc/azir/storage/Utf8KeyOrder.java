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

package com.flyingzc.azir.storage;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

/**
 * Unsigned UTF-8 key encoding and sorting rules consistent with MySQL Varbinary.
 */
public final class Utf8KeyOrder {

    /**
     * Stops the case-based key order tool.
     */
    private Utf8KeyOrder() {
    }

    /**
     * Encoding business keys and refusing to be replaced by UTF-8 with an illegal proxy character for the same byte series.
     *
     * @param key business key
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public static byte[] encode(final String key) {
        Objects.requireNonNull(key, "key");
        byte[] encoded = key.getBytes(StandardCharsets.UTF_8);
        if (!key.equals(new String(encoded, StandardCharsets.UTF_8))) {
            throw new IllegalArgumentException("key contains malformed UTF-16");
        }
        return encoded;
    }

    /**
     * Compares two business keys by unsigned UTF-8 bytes.
     *
     * @param left left
     * @param right right
     * @return the operation result
     */
    public static int compare(final String left, final String right) {
        return Arrays.compareUnsigned(encode(left), encode(right));
    }
}
