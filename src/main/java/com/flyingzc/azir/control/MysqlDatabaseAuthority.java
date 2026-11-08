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

package com.flyingzc.azir.control;

import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.MvccException;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;

/**
 * Using the server UUID, independent of the current database verify connection pool does point to the same MySQL durable authority.
 */
public final class MysqlDatabaseAuthority {

    /**
     * Only static database identity validate is exposed.
     */
    private MysqlDatabaseAuthority() {
    }

    /**
     * Rejects background connection to repeat with the pool or point to another MySQL authority.
     *
     * @param foreground foreground
     * @param background background
     * @param route route
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public static void requireIndependentSameDatabase(final DataSource foreground, final DataSource background,
                                                      final String route) {
        Objects.requireNonNull(foreground, "foreground");
        Objects.requireNonNull(background, "background");
        if (foreground == background || !inspect(foreground).equals(inspect(background))) {
            throw new IllegalArgumentException("background MySQL authority differs from foreground: " + route);
        }
    }

    /**
     * Ask real connection, not just believe the JDBC URL in your configuration.
     *
     * @param source database connection source
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     */
    public static Identity inspect(final DataSource source) {
        Objects.requireNonNull(source, "source");
        if (source instanceof ProtocolMysqlDataSource protocol) {
            return protocol.authorityIdentity();
        }
        try (Connection connection = source.getConnection()) {
            return inspectConnection(connection);
        } catch (final SQLException ex) {
            throw new MvccException(ErrorCode.STORAGE_UNAVAILABLE,
                    "inspect MySQL database authority failed", false, null, ex);
        }
    }

    /**
     * Read identity from original connection with an verify driver configuration, without using a sample that may point to another example.
     *
     * @param connection connection
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     * @throws SQLException if the operation cannot complete
     */
    public static Identity inspectConnection(final Connection connection) throws SQLException {
        Objects.requireNonNull(connection, "connection");
        try (Statement query = connection.createStatement(); ResultSet row = query.executeQuery("select @@server_uuid, database()")) {
            if (!row.next() || row.getString(1) == null || row.getString(2) == null) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE, "MySQL connection has no server or selected database", false, null);
            }
            return new Identity(row.getString(1), row.getString(2));
        }
    }

    /**
     * The server rejects any difference between database and the previous background mix.
     *
     * @param serverUuid server uuid
     * @param database database
     */
    public record Identity(String serverUuid, String database) {

        /**
         * Unable to guess the agreed time line when database name cannot be determined.
         *
         * @param serverUuid server uuid
         * @param database database
         * @throws IllegalArgumentException if the operation cannot complete
         */
        public Identity {
            if (serverUuid == null || serverUuid.isBlank() || database == null || database.isBlank()) {
                throw new IllegalArgumentException("invalid MySQL database authority identity");
            }
        }
    }
}
