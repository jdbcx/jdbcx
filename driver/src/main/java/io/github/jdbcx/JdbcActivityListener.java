/*
 * Copyright 2022-2025, Zhichun Wu
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

import java.sql.DatabaseMetaData;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public interface JdbcActivityListener {
    default DatabaseMetaData onMetaData(DatabaseMetaData metaData) {
        return metaData;
    }

    default ParameterMetaData onMetaData(ParameterMetaData metaData) {
        return metaData;
    }

    default ResultSetMetaData onMetaData(ResultSetMetaData metaData) {
        return metaData;
    }

    default Result<?> onQuery(String query) throws SQLException { // NOSONAR
        return Result.of(query);
    }

    default String onQuery(PreparedStatement statement, String query) throws SQLException {
        return query;
    }

    default Result<?> onResult(Result<?> rs) throws SQLException { // NOSONAR
        return rs;
    }

    default void onError(SQLException exception) throws SQLException {
    }
}
