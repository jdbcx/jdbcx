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
package io.github.jdbcx.executor;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import io.github.jdbcx.Checker;
import io.github.jdbcx.Field;
import io.github.jdbcx.Result;
import io.github.jdbcx.Row;
import io.github.jdbcx.data.StringValue;
import io.github.jdbcx.executor.jdbc.CombinedResultSet;
import io.github.jdbcx.executor.jdbc.ReadOnlyResultSet;

public class JdbcExecutor extends AbstractExecutor {
    private static final List<Field> dryRunFields = Collections
            .unmodifiableList(Arrays.asList(Field.of("connection"), FIELD_QUERY, FIELD_TIMEOUT_MS, FIELD_OPTIONS));

    public JdbcExecutor(Properties props) {
        super(props);
    }

    public Object execute(String query, Connection conn, Properties props) throws SQLException {
        if (getDryRun(props)) {
            return new ReadOnlyResultSet(null,
                    Result.of(Row.of(dryRunFields, new StringValue(conn), new StringValue(query),
                            new StringValue(getTimeout(props)), new StringValue(props))));
        } else if (Checker.isNullOrBlank(query)) {
            return new CombinedResultSet();
        }

        // final int parallelism = getParallelism(props);
        final int timeoutSec = getTimeout(props) / 1000;
        final Statement stmt = conn.createStatement(); // NOSONAR
        if (timeoutSec > 0) {
            stmt.setQueryTimeout(timeoutSec);
        }
        if (stmt.execute(query)) {
            return stmt.getResultSet();
        } else {
            try {
                return stmt.getLargeUpdateCount();
            } catch (SQLFeatureNotSupportedException e) {
                return (long) stmt.getUpdateCount();
            } finally {
                stmt.close();
            }
        }
    }
}
