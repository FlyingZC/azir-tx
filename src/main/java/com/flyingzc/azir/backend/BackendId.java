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

package com.flyingzc.azir.backend;

import java.util.Objects;

/**
 * Backend durable identity; the same startTs cannot be used between different models or data spaces.
 *
 * @param implementation implementation
 * @param namespace persistent protocol namespace
 * @param startTs transaction start timestamp
 */
public record BackendId(String implementation, String namespace, long startTs) {

    /**
     * Validate complete transaction identity for cross-process queries.
     *
     * @param implementation implementation
     * @param namespace persistent protocol namespace
     * @param startTs transaction start timestamp
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public BackendId {
        Objects.requireNonNull(implementation, "implementation");
        Objects.requireNonNull(namespace, "namespace");
        if (startTs <= 0) {
            throw new IllegalArgumentException("startTs must be positive");
        }
    }
}
