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

package com.flyingzc.azir.tso.mysql;

import javax.sql.DataSource;

/**
 * Mark timestamp authority for sharing the same data source with MySQL control.
 */
public interface MysqlTimestampAuthority {

    /**
     * Only when you determine that the same data source object is used will you be allowed to use the server end-up concurrent sheet.
     *
     * @param dataSource database connection source
     * @return the operation result
     */
    boolean usesDataSource(DataSource dataSource);
}
