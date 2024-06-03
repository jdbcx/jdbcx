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
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import io.github.jdbcx.Checker;
import io.github.jdbcx.Field;
import io.github.jdbcx.Option;
import io.github.jdbcx.Result;
import io.github.jdbcx.Row;
import io.github.jdbcx.ValueFactory;
import io.github.jdbcx.VariableTag;
import io.github.jdbcx.executor.jdbc.CombinedResultSet;
import io.github.jdbcx.executor.jdbc.ReadOnlyResultSet;
import io.github.jdbcx.value.LongValue;
import io.github.jdbcx.value.StringValue;

public class JdbcExecutor extends AbstractExecutor {
    private static final List<Field> dryRunFields = Collections
            .unmodifiableList(Arrays.asList(Field.of("connection"), FIELD_QUERY, FIELD_TIMEOUT_MS, FIELD_OPTIONS));

    public static final String RESULT_FIRST = "first";
    public static final String RESULT_FIRST_QUERY = "firstQuery";
    public static final String RESULT_FIRST_UPDATE = "firstUpdate";
    public static final String RESULT_LAST = "last";
    public static final String RESULT_LAST_QUERY = "lastQuery";
    public static final String RESULT_LAST_UPDATE = "lastUpdate";
    public static final String RESULT_MERGED_QUERIES = "mergedQueries";
    public static final String RESULT_MERGED_UPDATES = "mergedUpdates";
    public static final String RESULT_SUMMARY = "summary";

    public static final Option OPTION_RESULT = Option
            .of(new String[] { "result", "Only the preferred one will be returned when there are multiple results.",
                    RESULT_FIRST, RESULT_FIRST_QUERY, RESULT_FIRST_UPDATE, RESULT_LAST, RESULT_LAST_QUERY,
                    RESULT_LAST_UPDATE, RESULT_MERGED_QUERIES, RESULT_MERGED_UPDATES, RESULT_SUMMARY });

    static final List<Field> SUMMARY_FIELDS = Collections
            .unmodifiableList(Arrays.asList(Field.of("seq", JDBCType.INTEGER, false),
                    Field.of("type", JDBCType.VARCHAR, false), Field.of("rows", JDBCType.BIGINT, false)));
    static final String TYPE_QUERY = "query";
    static final String TYPE_UPDATE = "update";

    static final void closeResultSet(ResultSet rs) throws SQLException {
        if (rs != null) {
            rs.close();
        }
    }

    static final long getUpdateCount(Statement stmt, boolean closeStatement) throws SQLException {
        try {
            return stmt.getLargeUpdateCount();
        } catch (UnsupportedOperationException | SQLFeatureNotSupportedException e) {
            return stmt.getUpdateCount();
        } finally {
            if (closeStatement) {
                stmt.close();
            }
        }
    }

    static final Object getFirstResult(Statement stmt, String query) throws SQLException {
        if (stmt.execute(query)) {
            return stmt.getResultSet();
        } else {
            try {
                return getUpdateCount(stmt);
            } finally {
                stmt.close();
            }
        }
    }

    static final ResultSet getFirstQueryResult(Statement stmt, String query) throws SQLException {
        if (stmt.execute(query)) {
            return stmt.getResultSet();
        } else {
            while (true) {
                if (stmt.getMoreResults()) {
                    return stmt.getResultSet();
                } else if (stmt.getUpdateCount() == -1) {
                    stmt.close();
                    return new CombinedResultSet();
                }
            }
        }
    }

    static final long getFirstUpdateCount(Statement stmt, String query) throws SQLException {
        if (stmt.execute(query)) {
            while (stmt.getMoreResults()) {
                // do nothing
            }
        }
        long count = getUpdateCount(stmt, true);
        return count < 0L ? 0L : count;
    }

    static final Object getLastResult(Statement stmt, String query) throws SQLException {
        ResultSet rs = stmt.execute(query) ? stmt.getResultSet() : null;
        long count = rs == null ? getUpdateCount(stmt) : 0L;

        while (true) {
            if (stmt.getMoreResults(Statement.KEEP_CURRENT_RESULT)) {
                closeResultSet(rs);
                rs = stmt.getResultSet();
                count = 0L;
            } else {
                long value = getUpdateCount(stmt);
                if (value == -1L) {
                    break;
                }
                closeResultSet(rs);
                rs = null;
                count = value;
            }
        }

        if (rs != null) {
            return rs;
        } else {
            stmt.close();
            return count;
        }
    }

    static final ResultSet getLastQueryResult(Statement stmt, String query) throws SQLException {
        ResultSet rs = stmt.execute(query) ? stmt.getResultSet() : null;
        while (true) {
            if (stmt.getMoreResults(Statement.KEEP_CURRENT_RESULT)) {
                if (rs != null) {
                    rs.close();
                }
                rs = stmt.getResultSet();
            } else if (stmt.getUpdateCount() == -1) {
                break;
            }
        }

        if (rs == null) {
            stmt.close();
            rs = new CombinedResultSet();
        }
        return rs;
    }

    static final long getLastUpdateCount(Statement stmt, String query) throws SQLException {
        long count = stmt.execute(query) ? 0L : getUpdateCount(stmt);

        while (true) {
            if (!stmt.getMoreResults()) {
                long value = getUpdateCount(stmt);
                if (value == -1L) {
                    break;
                }
                count = value;
            }
        }

        stmt.close();
        return count;
    }

    static final ResultSet getMergedQueryResult(Statement stmt, String query) throws SQLException {
        List<ResultSet> results = new ArrayList<>();
        if (stmt.execute(query)) {
            results.add(stmt.getResultSet());
        }
        while (true) {
            if (stmt.getMoreResults(Statement.KEEP_CURRENT_RESULT)) {
                results.add(stmt.getResultSet());
            } else if (stmt.getUpdateCount() == -1) {
                break;
            }
        }

        return new CombinedResultSet(results);
    }

    static final long getMergedUpdateCount(Statement stmt, String query) throws SQLException {
        long count = stmt.execute(query) ? 0L : getUpdateCount(stmt);

        while (true) {
            if (!stmt.getMoreResults()) {
                long value = getUpdateCount(stmt);
                if (value == -1L) {
                    break;
                }
                count += value;
            }
        }

        stmt.close();
        return count;
    }

    static final ResultSet getResultSummary(Statement stmt, String query) throws SQLException {
        final List<Row> rows = new ArrayList<>();
        final ValueFactory factory = ValueFactory.getInstance();

        int seq = 1;
        if (stmt.execute(query)) {
            try (ResultSet rs = stmt.getResultSet()) {
                long count = 0L;
                while (rs.next()) {
                    count++;
                }
                rows.add(Row.of(SUMMARY_FIELDS, LongValue.of(factory, false, true, seq++),
                        StringValue.of(factory, false, 0, TYPE_QUERY),
                        LongValue.of(factory, false, true, count)));
            }
        } else {
            rows.add(Row.of(SUMMARY_FIELDS, LongValue.of(factory, false, true, seq++),
                    StringValue.of(factory, false, 0, TYPE_UPDATE),
                    LongValue.of(factory, false, true, getUpdateCount(stmt))));
        }

        while (true) {
            if (stmt.getMoreResults()) {
                try (ResultSet rs = stmt.getResultSet()) {
                    long count = 0L;
                    while (rs.next()) {
                        count++;
                    }
                    rows.add(Row.of(SUMMARY_FIELDS, LongValue.of(factory, false, true, seq++),
                            StringValue.of(factory, false, 0, TYPE_QUERY),
                            LongValue.of(factory, false, true, count)));
                }
            } else {
                long value = getUpdateCount(stmt);
                if (value == -1L) {
                    break;
                }
                rows.add(Row.of(SUMMARY_FIELDS, LongValue.of(factory, false, true, seq++),
                        StringValue.of(factory, false, 0, TYPE_UPDATE),
                        LongValue.of(factory, false, true, getUpdateCount(stmt))));
            }
        }

        stmt.close();
        return new ReadOnlyResultSet(stmt, Result.of(SUMMARY_FIELDS, rows));
    }

    public static final long getUpdateCount(Statement stmt) throws SQLException {
        return getUpdateCount(stmt, false);
    }

    public String getResultType(Properties props) {
        return props != null ? props.getProperty(OPTION_RESULT.getName(), defaultResultType) : defaultResultType;
    }

    protected final String defaultResultType;

    public JdbcExecutor(VariableTag tag, Properties props) {
        super(tag, props);

        this.defaultResultType = OPTION_RESULT.getValue(props);
    }

    public Object execute(String query, Connection conn, Properties props) throws SQLException {
        if (getDryRun(props)) {
            return new ReadOnlyResultSet(null,
                    Result.of(Row.of(dryRunFields, new Object[] { conn, query, getTimeout(props), props })));
        } else if (Checker.isNullOrBlank(query)) {
            return new CombinedResultSet();
        }

        // final int parallelism = getParallelism(props);
        final int timeoutSec = getTimeout(props) / 1000;
        final Statement stmt = conn.createStatement(); // NOSONAR
        if (timeoutSec > 0) {
            stmt.setQueryTimeout(timeoutSec);
        }

        final String resultType = getResultType(props);
        final Object result;
        switch (resultType) {
            case RESULT_FIRST:
                result = getFirstResult(stmt, query);
                break;
            case RESULT_FIRST_QUERY:
                result = getFirstQueryResult(stmt, query);
                break;
            case RESULT_FIRST_UPDATE:
                result = getFirstUpdateCount(stmt, query);
                break;
            case RESULT_LAST:
                result = getLastResult(stmt, query);
                break;
            case RESULT_LAST_QUERY:
                result = getLastQueryResult(stmt, query);
                break;
            case RESULT_LAST_UPDATE:
                result = getLastUpdateCount(stmt, query);
                break;
            case RESULT_MERGED_QUERIES:
                result = getMergedQueryResult(stmt, query);
                break;
            case RESULT_MERGED_UPDATES:
                result = getMergedUpdateCount(stmt, query);
                break;
            case RESULT_SUMMARY:
                result = getResultSummary(stmt, query);
                break;
            default:
                throw new SQLException("Unsupported parameter result=" + resultType);
        }
        return result;
    }
}
