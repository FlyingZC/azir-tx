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

import java.util.UUID;

/**
 * Receipt negative filter, still required to check UUID and content for impact. The probability shot cannot be commit.
 */
final class WalBloom {

    private final byte[] bits;

    /**
     * Creates a sequenced filter, which must be of two full series of several times the size of the array.
     *
     * @param bits bits
     * @throws IllegalArgumentException if the operation cannot complete
     */
    WalBloom(final byte[] bits) {
        if (bits.length < 128 || Integer.bitCount(bits.length) != 1) {
            throw new IllegalArgumentException("invalid bloom size");
        }
        this.bits = bits;
    }

    /**
     * A limited number of digits is allocated to the expected receipt, exceeding budget by increasing the error rate without creating a false negative.
     *
     * @param records records
     * @param maximumBytes maximum bytes
     * @return the operation result
     */
    static WalBloom allocate(final long records, final int maximumBytes) {
        return new WalBloom(new byte[capacity(records, maximumBytes)]);
    }

    /**
     * Calculates the size before allocating bit arrays, from which point of view you can check the full encoding budget.
     *
     * @param records records
     * @param maximumBytes maximum bytes
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    static int capacity(final long records, final int maximumBytes) {
        if (records < 0 || maximumBytes < 128 || Integer.bitCount(maximumBytes) != 1) {
            throw new IllegalArgumentException("invalid bloom capacity");
        }
        int bytes = 128;
        while (bytes < maximumBytes && bytes * 8L < Math.min(records, Long.MAX_VALUE / 12) * 12L) {
            bytes *= 2;
        }
        return bytes;
    }

    /**
     * Marks an durable storage receipt into the filter.
     *
     * @param id id
     */
    void add(final UUID id) {
        probe(id, true);
    }

    /**
     * The return needs to continue to be accurately searched, the false negatives can disrupt the weight and result queries and must be blocked by testing.
     *
     * @param id id
     * @return the operation result
     */
    boolean mightContain(final UUID id) {
        return probe(id, false);
    }

    /**
     * Returns defensive replica to incorporate filters into the entire checkpoint validate.
     *
     * @return the operation result
     */
    byte[] bytes() {
        return bits.clone();
    }

    /**
     * Returns the size of a series of bits, and the capacity check cannot duplicate the whole filter for this purpose.
     *
     * @return the operation result
     */
    int byteSize() {
        return bits.length;
    }

    /**
     * Generates seven-bit positions in two mixed hashs, which are constructed to share exactly the same bit calculation with the query.
     *
     * @param id id
     * @param insert insert
     * @return the operation result
     */
    private boolean probe(final UUID id, final boolean insert) {
        long first = mix(id.getMostSignificantBits());
        long step = mix(id.getLeastSignificantBits()) | 1;
        long mask = bits.length * 8L - 1;
        for (int i = 0; i < 7; i++) {
            long position = (first + i * step) & mask;
            int index = (int) (position >>> 3);
            int flag = 1 << (position & 7);
            if (insert) {
                bits[index] |= (byte) flag;
            } else if ((bits[index] & flag) == 0) {
                return false;
            }
        }
        return true;
    }

    /**
     * Mixeds every half of the UUID to avoid a concentration in order or close input.
     *
     * @param value value to write
     * @return the operation result
     */
    private static long mix(long value) {
        value = (value ^ (value >>> 30)) * 0xbf58476d1ce4e5b9L;
        value = (value ^ (value >>> 27)) * 0x94d049bb133111ebL;
        return value ^ (value >>> 31);
    }
}
