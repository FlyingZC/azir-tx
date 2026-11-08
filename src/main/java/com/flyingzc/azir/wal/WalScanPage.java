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

import com.flyingzc.azir.storage.Utf8KeyOrder;
import java.util.Collections;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * The limited scanned page on snapshot, the cursor is empty to indicate that there is no follow-up page.
 *
 * @param rows rows
 * @param nextAfterKey next after key
 */
public record WalScanPage(NavigableMap<String, byte[]> rows, String nextAfterKey) {

    /**
     * Copys the page data and fixes the order of the key to prevent the caller from changing the internal candidate set.
     *
     * @param rows rows
     * @param nextAfterKey next after key
     */
    public WalScanPage {
        var copy = new TreeMap<String, byte[]>(Utf8KeyOrder::compare);
        rows.forEach((key, value) -> copy.put(key, value.clone()));
        rows = Collections.unmodifiableNavigableMap(copy);
    }
}
