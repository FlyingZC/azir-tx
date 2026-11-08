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

import java.util.List;

/**
 * Bounded snapshot scan page; cursor for the original key of the final scan, an empty page may also have a next page.
 *
 * @param values values
 * @param nextKey next key
 */
public record ScanPage(List<VersionedValue> values, String nextKey) {

    /**
     * Fixed list of this page values, empty cursor means range is over.
     *
     * @param values values
     * @param nextKey next key
     */
    public ScanPage {
        values = List.copyOf(values);
    }

    /**
     * Returns the visible value of this page that is not deleted.
     *
     * @return the operation result
     */
    @Override
    public List<VersionedValue> values() {
        return values;
    }

    /**
     * Returns the exclusionary starting point on the next page, with an empty value indicating the end of the scan.
     *
     * @return the operation result
     */
    @Override
    public String nextKey() {
        return nextKey;
    }
}
