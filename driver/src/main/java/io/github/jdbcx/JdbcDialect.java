/*
 * Copyright 2022-2024, Zhichun Wu
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
package io.github.jdbcx;

import java.sql.Connection;
import java.sql.SQLException;

public interface JdbcDialect {
    default VariableTag getVariableTag() {
        return VariableTag.BRACE;
    }

    default ValueFactory getValueFactory(Format format) {
        return ValueFactory.getInstance();
    }

    ResultMapper getMapper();

    /**
     * Gets table representing the given URL. This is typically used in a query like
     * shown below.
     * 
     * <pre>
     * select * from {{ bridge.db.mysql1: select 1 }}
     * </pre>
     * 
     * The inner query will be translated to
     * {@code 'http://bridge-server:8080/<uuid>.csv'} by default. This various on
     * different databases, for examples:
     * <ul>
     * <li>On DuckDB, it's
     * {@code read_csv('http://bridge-server:8080/<uuid>.csv', auto_detect=true)}
     * </li>
     * <li>On ClickHouse, url table function will be used, so it becomes
     * {@code url('http://bridge-server:8080/<uuid>.csv', 'CSVWithNames')}</li>
     * </ul>
     *
     * @param conn connection
     * @param url  url
     * @return table representing the given URL
     * @throws SQLException
     */
    default String getRemoteTable(Connection conn, String url) throws SQLException {
        if (Checker.isNullOrEmpty(url)) {
            throw new SQLException("Non-empty URL is required");
        }

        StringBuilder builder = new StringBuilder(url.length() + 2);
        return builder.append('\'').append(url).append('\'').toString();
    }
}
