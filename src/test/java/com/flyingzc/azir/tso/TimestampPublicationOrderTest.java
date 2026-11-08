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

package com.flyingzc.azir.tso;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Verify Compatible batch Packers cannot re-introduce the release time caused by the customer's cache.
 */
class TimestampPublicationOrderTest {

    /**
     * The two packagings and the direct caller staggered their numbers, always from the same authority sequence.
     */
    @Test
    void compatibilityWrappersPreserveAuthorityOrder() {
        var authority = new AtomicTimestampOracle(0);
        var first = new BatchingTimestampOracle(authority, 100);
        var second = new BatchingTimestampOracle(authority, 200);
        Assertions.assertEquals(1, first.nextTimestamp());
        Assertions.assertEquals(2, second.nextTimestamp());
        Assertions.assertEquals(3, first.nextTimestamp());
        Assertions.assertEquals(4, authority.nextTimestamp());
        Assertions.assertEquals(5, second.nextTimestamp());
    }
}
