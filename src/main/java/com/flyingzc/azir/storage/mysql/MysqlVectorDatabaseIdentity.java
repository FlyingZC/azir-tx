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

package com.flyingzc.azir.storage.mysql;

import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.MvccException;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Run only with the actual MySQL server UUID and selected database for control and shard authority.
 */
final class MysqlVectorDatabaseIdentity {

    /**
     * Identity does not save the connection or process local route cache.
     */
    private MysqlVectorDatabaseIdentity() {
    }

    /**
     * The JDBC URL is the same or different than the identity combination of a physical server and database.
     *
     * @param source database connection source
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     */
    static String read(final DataSource source) {
        try (Connection connection = source.getConnection()) {
            return read(connection);
        } catch (final SQLException ex) {
            throw new MvccException(ErrorCode.STORAGE_UNAVAILABLE,
                    "inspect vector GC database identity failed", true, null, ex);
        }
    }

    /**
     * Review Physical identity on the same JDBC connection that actually implements GC, closes the gap between inspection and use.
     *
     * @param connection connection
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     * @throws SQLException if the operation cannot complete
     */
    static String read(final Connection connection) throws SQLException {
        try (
                PreparedStatement query = connection.prepareStatement("select @@server_uuid, database()");
                ResultSet row = query.executeQuery()) {
            if (!row.next() || row.getString(1) == null || row.getString(2) == null) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE,
                        "vector GC requires a selected MySQL database", false, null);
            }
            return row.getString(1) + "\u0000" + row.getString(2);
        }
    }
}
