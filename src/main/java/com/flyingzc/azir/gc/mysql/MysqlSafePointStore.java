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

package com.flyingzc.azir.gc.mysql;

import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.MvccException;
import com.flyingzc.azir.gc.SafePointStore;
import com.flyingzc.azir.control.MysqlConnectionContext;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Safe point storage realized based on MySQL.
 */
public final class MysqlSafePointStore implements SafePointStore {

    private static final String STORE_ID = "global";

    private final DataSource dataSource;

    /**
     * Construct a MysqlSafePointStore instance.
     *
     * @param dataSource database connection source
     */
    public MysqlSafePointStore(final DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /**
     * Initialize the required database tables or runtime environment.
     *
     * @param initialSafePoint initial safe point
     */
    public void initialize(final long initialSafePoint) {
        try (
                Connection connection = MysqlConnectionContext.connection(dataSource);
                PreparedStatement create = connection.prepareStatement("""
                        create table if not exists mvcc_safe_point (
                          store_id varchar(64) not null primary key,
                          safe_point bigint not null
                        )
                        """);
                PreparedStatement insert = connection.prepareStatement("""
                        insert into mvcc_safe_point(store_id, safe_point)
                        values (?, ?)
                        on duplicate key update safe_point = greatest(safe_point, values(safe_point))
                        """)) {
            create.execute();
            insert.setString(1, STORE_ID);
            insert.setLong(2, initialSafePoint);
            insert.executeUpdate();
        } catch (final SQLException ex) {
            throw storageError("initialize safe point failed", ex);
        }
    }

    /**
     * Return the current safe point.
     *
     * @return the operation result
     */
    @Override
    public long currentSafePoint() {
        try (
                Connection connection = MysqlConnectionContext.connection(dataSource);
                PreparedStatement statement = connection.prepareStatement("""
                        select safe_point from mvcc_safe_point where store_id = ?
                        """)) {
            statement.setString(1, STORE_ID);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    return 0L;
                }
                return resultSet.getLong("safe_point");
            }
        } catch (final SQLException ex) {
            throw storageError("load safe point failed", ex);
        }
    }

    /**
     * Try to advance the safe point to the candidate timestamp.
     *
     * @param candidateSafePoint candidate safe point
     * @return the operation result
     */
    @Override
    public long advanceTo(final long candidateSafePoint) {
        try {
            return MysqlConnectionContext.inTransaction(dataSource, connection -> {
                try (
                        PreparedStatement select = connection.prepareStatement("""
                                select safe_point from mvcc_safe_point where store_id = ? for update
                                """);
                        PreparedStatement update = connection.prepareStatement("""
                                update mvcc_safe_point set safe_point = ? where store_id = ?
                                """)) {
                    select.setString(1, STORE_ID);
                    long current;
                    try (ResultSet resultSet = select.executeQuery()) {
                        if (!resultSet.next()) {
                            throw new MvccException(ErrorCode.ILLEGAL_STATE, "safe point row missing", false, null);
                        }
                        current = resultSet.getLong("safe_point");
                    }
                    long next = Math.max(current, candidateSafePoint);
                    update.setLong(1, next);
                    update.setString(2, STORE_ID);
                    update.executeUpdate();
                    return next;
                }
            });
        } catch (final SQLException ex) {
            throw storageError("advance safe point failed", ex);
        }
    }

    /**
     * Map the storage failure to an MVCC exception.
     *
     * @param message message
     * @param ex ex
     * @return the operation result
     */
    private MvccException storageError(final String message, final SQLException ex) {
        return new MvccException(ErrorCode.STORAGE_UNAVAILABLE, message, true, null, ex);
    }
}
