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
 * An optional and credible business assembly SPI, with a clearly specified class name only configuration, does not scan or auto-execut the routing plugin.
 */
public interface RatisNodeApplication extends AutoCloseable {

    /**
     * The builder shall have no side effects; start receiving hosting transaction API and the operation cannot read and write directly through the frame materialization Tables.
     *
     * @param node node
     * @throws Exception if the operation cannot complete
     */
    void start(RatisManagedNode node) throws Exception;

    /**
     * Stop the order and bounded drop-out to support clean-up and repeat-off after partial start-up failed; host keeps node if failure occurs.
     *
     * @throws Exception if the operation cannot complete
     */
    @Override
    void close() throws Exception;
}
