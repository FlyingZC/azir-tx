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

import com.mysql.cj.jdbc.JdbcConnection;
import com.mysql.cj.jdbc.ha.MultiHostMySQLConnection;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Check the physical driver configuration before any authority SQL. Naming lock protocol cannot rely on session that can be transparently replaced.
 */
final class MysqlSessionRequirements {

    /**
     * Prohibits the exemplifying connection binding tool.
     */
    private MysqlSessionRequirements() {
    }

    /**
     * Limited network waiting, single nodes JDBC session and a ban on transparent reconnection are prerequisites for the establishment of control locks and session variables.
     *
     * @param connection connection
     * @throws SQLException if the operation cannot complete
     */
    static void require(final Connection connection) throws SQLException {
        if (connection.isWrapperFor(MultiHostMySQLConnection.class)) {
            throw new SQLException("protocol sessions cannot use multi-host JDBC failover");
        }
        JdbcConnection physical = connection.unwrap(JdbcConnection.class);
        var properties = physical.getPropertySet();
        if (Boolean.TRUE.equals(properties.getBooleanProperty("autoReconnect").getValue())
                || Boolean.TRUE.equals(properties.getBooleanProperty("autoReconnectForPools").getValue())
                || Boolean.TRUE.equals(properties.getBooleanProperty("reconnectAtTxEnd").getValue())) {
            throw new SQLException("protocol sessions cannot transparently reconnect");
        }
        Integer connect = properties.getIntegerProperty("connectTimeout").getValue();
        Integer socket = properties.getIntegerProperty("socketTimeout").getValue();
        if (connect == null || connect < 1 || socket == null || socket < 1) {
            throw new SQLException("protocol sessions require finite JDBC connect and socket timeouts");
        }
        // Checks the original URL to supplement the proxy-driven packaging detector, and does not reflect in the error that the URL may contain receipt.
        String url = connection.getMetaData().getURL();
        String prefix = "jdbc:mysql://";
        if (url == null || !url.startsWith(prefix)) {
            throw new SQLException("protocol sessions require a single-host Connector/J URL");
        }
        int end = url.indexOf('/', prefix.length());
        if (end < 0) {
            end = url.length();
        }
        if (url.substring(prefix.length(), end).contains(",")) {
            throw new SQLException("protocol sessions cannot use a multi-host JDBC URL");
        }
    }
}
