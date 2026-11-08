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

package com.flyingzc.azir.sql;

import java.nio.charset.StandardCharsets;

/**
 * Decoding interface between SQL string values and MVCC bytes.
 */
public interface SqlValueCodec {

    /**
     * Encoding SQL string values as storage bytes.
     *
     * @param value value to write
     * @return the operation result
     */
    byte[] encode(String value);

    /**
     * Decodes the storage bytes as a SQL string value.
     *
     * @param value value to write
     * @return the operation result
     */
    String decode(byte[] value);

    /**
     * Returns the decoding of a UTF-8 string.
     *
     * @return the operation result
     */
    static SqlValueCodec utf8() {
        return Utf8SqlValueCodec.INSTANCE;
    }

    /**
     * UTF-8 string decoded.
     */
    final class Utf8SqlValueCodec implements SqlValueCodec {

        private static final Utf8SqlValueCodec INSTANCE = new Utf8SqlValueCodec();

        /**
         * Constructs the UTF-8 decoding process.
         */
        private Utf8SqlValueCodec() {
        }

        /**
         * Encoding a string as UTF-8 bytes.
         *
         * @param value value to write
         * @return the operation result
         */
        @Override
        public byte[] encode(final String value) {
            return value.getBytes(StandardCharsets.UTF_8);
        }

        /**
         * Decoding the UTF-8 bytes as a string.
         *
         * @param value value to write
         * @return the operation result
         */
        @Override
        public String decode(final byte[] value) {
            return new String(value, StandardCharsets.UTF_8);
        }
    }
}
