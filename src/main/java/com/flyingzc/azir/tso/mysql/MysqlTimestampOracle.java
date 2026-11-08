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

import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.MvccException;
import com.flyingzc.azir.tso.TimestampOracle;
import com.flyingzc.azir.control.MysqlConnectionContext;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Allocates globally ordered timestamps through a durable MySQL sequence protected by a row lock.
 */
public final class MysqlTimestampOracle implements TimestampOracle, MysqlTimestampAuthority {

    private static final String SEQUENCE_NAME = "global";

    private final DataSource dataSource;

    /**
     * Construct a MysqlTimestampOracle instance.
     *
     * @param dataSource database connection source
     */
    public MysqlTimestampOracle(final DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /**
     * Merger control only accepts the same pool example, and other configurations follow a sequential timestamp allocation path.
     *
     * @param candidate candidate
     * @return the operation result
     */
    @Override
    public boolean usesDataSource(final DataSource candidate) {
        return dataSource == candidate;
    }

    /**
     * Initialize the required database tables or runtime environment.
     *
     * @param initialValue initial value
     */
    public void initialize(final long initialValue) {
        try (
                Connection connection = MysqlConnectionContext.connection(dataSource);
                PreparedStatement create = connection.prepareStatement("""
                        create table if not exists mvcc_tso (
                          sequence_name varchar(64) not null primary key,
                          current_value bigint not null
                        ) engine=InnoDB
                        """);
                PreparedStatement insert = connection.prepareStatement("""
                        insert into mvcc_tso(sequence_name, current_value)
                        values (?, ?)
                        on duplicate key update current_value = greatest(current_value, values(current_value))
                        """)) {
            create.execute();
            insert.setString(1, SEQUENCE_NAME);
            insert.setLong(2, initialValue);
            insert.executeUpdate();
        } catch (final SQLException ex) {
            throw storageError("initialize tso failed", ex, null);
        }
    }

    /**
     * Allocate the next globally increasing timestamp.
     *
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     */
    @Override
    public long nextTimestamp() {
        try (Connection connection = MysqlConnectionContext.connection(dataSource)) {
            if (!connection.getAutoCommit()) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE,
                        "tso allocation requires auto-commit connection", false, null);
            }
            // Single autocommit UPDATE atomic keeps timestamp, LAST_INSERT_ID(expr) bound the result to the current connection.
            try (
                    PreparedStatement update = connection.prepareStatement("""
                            update mvcc_tso
                            set current_value = last_insert_id(current_value + 1)
                            where sequence_name = ? and current_value < ?
                            """);
                    PreparedStatement select = connection.prepareStatement("select last_insert_id()")) {
                update.setString(1, SEQUENCE_NAME);
                update.setLong(2, Long.MAX_VALUE);
                if (update.executeUpdate() != 1) {
                    throw new MvccException(ErrorCode.ILLEGAL_STATE,
                            "tso row missing or timestamp sequence exhausted", false, null);
                }
                try (ResultSet result = select.executeQuery()) {
                    if (!result.next()) {
                        throw new MvccException(ErrorCode.ILLEGAL_STATE,
                                "tso allocation returned no timestamp", false, null);
                    }
                    return result.getLong(1);
                }
            }
        } catch (final SQLException ex) {
            throw storageError("allocate tso failed", ex, null);
        }
    }

    /**
     * Map the storage failure to an MVCC exception.
     *
     * @param message message
     * @param ex ex
     * @param txId transaction identifier
     * @return the operation result
     */
    private MvccException storageError(final String message, final SQLException ex, final Long txId) {
        return new MvccException(ErrorCode.STORAGE_UNAVAILABLE, message, true, txId, ex);
    }
}
