/*
 * Copyright 2022-2026, Zhichun Wu
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
import io.github.jdbcx.Logger;
import io.github.jdbcx.LoggerFactory;
import io.github.jdbcx.Option;
import io.github.jdbcx.Result;
import io.github.jdbcx.Row;
import io.github.jdbcx.Utils;
import io.github.jdbcx.ValueFactory;
import io.github.jdbcx.VariableTag;
import io.github.jdbcx.executor.jdbc.CombinedResultSet;
import io.github.jdbcx.executor.jdbc.ReadOnlyResultSet;
import io.github.jdbcx.value.LongValue;
import io.github.jdbcx.value.StringValue;

public class JdbcExecutor extends AbstractExecutor {
    private static final Logger log = LoggerFactory.getLogger(JdbcExecutor.class);

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
        } catch (SQLFeatureNotSupportedException | UnsupportedOperationException | NoSuchMethodError e) {
            return stmt.getUpdateCount();
        } finally {
            if (closeStatement) {
                stmt.close();
            }
        }
    }

    static final Object getFirstResult(Statement stmt, String query, long[] stats) throws SQLException {
        if (stmt.execute(query)) {
            stats[0]++;
            return stmt.getResultSet();
        } else {
            stats[1]++;
            try {
                long rows = getUpdateCount(stmt);
                stats[2] += rows;
                return rows;
            } finally {
                stmt.close();
            }
        }
    }

    static final ResultSet getFirstQueryResult(Statement stmt, String query, long[] stats) throws SQLException {
        if (stmt.execute(query)) {
            stats[0]++;
            return stmt.getResultSet();
        } else {
            stats[1]++;
            stats[2] += getUpdateCount(stmt);
            while (true) {
                final long rows;
                if (stmt.getMoreResults()) {
                    stats[0]++;
                    return stmt.getResultSet();
                } else if ((rows = stmt.getUpdateCount()) == -1) {
                    stmt.close();
                    return new CombinedResultSet();
                } else {
                    stats[1]++;
                    stats[2] += rows;
                }
            }
        }
    }

    static final long getFirstUpdateCount(Statement stmt, String query, long[] stats) throws SQLException {
        if (stmt.execute(query)) {
            stats[0]++;
            while (stmt.getMoreResults()) {
                stats[0]++;
            }
        }
        long count = getUpdateCount(stmt, true);
        if (count < 0L) {
            return 0L;
        } else {
            stats[1]++;
            stats[2] += count;
            return count;
        }
    }

    static final Object getLastResult(Statement stmt, String query, long[] stats) throws SQLException {
        ResultSet rs;
        long count;
        if (stmt.execute(query)) {
            stats[0]++;
            rs = stmt.getResultSet();
            count = 0L;
        } else {
            stats[1]++;
            rs = null;
            count = getUpdateCount(stmt);
        }

        while (true) {
            if (stmt.getMoreResults(Statement.KEEP_CURRENT_RESULT)) {
                stats[0]++;
                Utils.closeQuietly(rs);
                closeResultSet(rs);
                rs = stmt.getResultSet();
                count = 0L;
            } else {
                long value = getUpdateCount(stmt);
                if (value == -1L) {
                    break;
                }
                stats[1]++;
                stats[2] += value;
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

    static final ResultSet getLastQueryResult(Statement stmt, String query, long[] stats) throws SQLException {
        ResultSet rs;
        if (stmt.execute(query)) {
            stats[0]++;
            rs = stmt.getResultSet();
        } else {
            stats[1]++;
            rs = null;
        }
        while (true) {
            final long rows;
            if (stmt.getMoreResults(Statement.KEEP_CURRENT_RESULT)) {
                stats[0]++;
                if (rs != null) {
                    rs.close();
                }
                rs = stmt.getResultSet();
            } else if ((rows = getUpdateCount(stmt)) == -1L) {
                break;
            } else {
                stats[1]++;
                stats[2] += rows;
            }
        }

        if (rs == null) {
            stmt.close();
            rs = new CombinedResultSet();
        }
        return rs;
    }

    static final long getLastUpdateCount(Statement stmt, String query, long[] stats) throws SQLException {
        long count;
        if (stmt.execute(query)) {
            stats[0]++;
            count = 0L;
        } else {
            stats[1]++;
            count = getUpdateCount(stmt);
            stats[2] += count;
        }

        while (true) {
            if (stmt.getMoreResults()) {
                stats[0]++;
            } else {
                long value = getUpdateCount(stmt);
                if (value == -1L) {
                    break;
                }
                stats[1]++;
                stats[2] += value;
                count = value;
            }
        }

        stmt.close();
        return count;
    }

    static final ResultSet getMergedQueryResult(Statement stmt, String query, long[] stats) throws SQLException {
        List<ResultSet> results = new ArrayList<>();
        if (stmt.execute(query)) {
            stats[0]++;
            results.add(stmt.getResultSet());
        } else {
            stats[1]++;
            stats[2] += getUpdateCount(stmt);
        }
        while (true) {
            final long rows;
            if (stmt.getMoreResults(Statement.KEEP_CURRENT_RESULT)) {
                stats[0]++;
                results.add(stmt.getResultSet());
            } else if ((rows = getUpdateCount(stmt)) == -1L) {
                break;
            } else {
                stats[1]++;
                stats[2] += rows;
            }
        }

        return new CombinedResultSet(results);
    }

    static final long getMergedUpdateCount(Statement stmt, String query, long[] stats) throws SQLException {
        long count;
        if (stmt.execute(query)) {
            stats[0]++;
            count = 0L;
        } else {
            stats[1]++;
            count = getUpdateCount(stmt);
            stats[2] += count;
        }
        while (true) {
            if (stmt.getMoreResults()) {
                stats[0]++;
            } else {
                long value = getUpdateCount(stmt);
                if (value == -1L) {
                    break;
                }
                stats[1]++;
                stats[2] += value;
                count += value;
            }
        }

        stmt.close();
        return count;
    }

    static final ResultSet getResultSummary(Statement stmt, String query, long[] stats) throws SQLException {
        final List<Row> rows = new ArrayList<>();
        final ValueFactory factory = ValueFactory.getInstance();

        int seq = 1;
        if (stmt.execute(query)) {
            stats[0]++;
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
            stats[1]++;
            long rowCount = getUpdateCount(stmt);
            stats[2] += rowCount;
            rows.add(Row.of(SUMMARY_FIELDS, LongValue.of(factory, false, true, seq++),
                    StringValue.of(factory, false, 0, TYPE_UPDATE),
                    LongValue.of(factory, false, true, rowCount)));
        }

        while (true) {
            if (stmt.getMoreResults()) {
                stats[0]++;
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
                stats[1]++;
                stats[2] += value;
                rows.add(Row.of(SUMMARY_FIELDS, LongValue.of(factory, false, true, seq++),
                        StringValue.of(factory, false, 0, TYPE_UPDATE),
                        LongValue.of(factory, false, true, value)));
            }
        }

        stmt.close();
        return new ReadOnlyResultSet(stmt, Result.of(SUMMARY_FIELDS, rows));
    }

    static final Object execute(Connection conn, JdbcQueryRequest request) throws SQLException {
        final Statement stmt = conn.createStatement(); // NOSONAR
        if (request.queryTimeoutSec > 0) {
            stmt.setQueryTimeout(request.queryTimeoutSec);
            log.debug("Set query timeout for [%s] to %d seconds", stmt, request.queryTimeoutSec);
        }

        final Object result;
        switch (request.resultType) {
            case RESULT_FIRST:
                result = getFirstResult(stmt, request.query, request.stats);
                break;
            case RESULT_FIRST_QUERY:
                result = getFirstQueryResult(stmt, request.query, request.stats);
                break;
            case RESULT_FIRST_UPDATE:
                result = getFirstUpdateCount(stmt, request.query, request.stats);
                break;
            case RESULT_LAST:
                result = getLastResult(stmt, request.query, request.stats);
                break;
            case RESULT_LAST_QUERY:
                result = getLastQueryResult(stmt, request.query, request.stats);
                break;
            case RESULT_LAST_UPDATE:
                result = getLastUpdateCount(stmt, request.query, request.stats);
                break;
            case RESULT_MERGED_QUERIES:
                result = getMergedQueryResult(stmt, request.query, request.stats);
                break;
            case RESULT_MERGED_UPDATES:
                result = getMergedUpdateCount(stmt, request.query, request.stats);
                break;
            case RESULT_SUMMARY:
                result = getResultSummary(stmt, request.query, request.stats);
                break;
            default:
                throw new SQLException("Unsupported parameter result=" + request.resultType);
        }
        return result;
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
        }

        query = loadInputFile(props, query);
        if (Checker.isNullOrBlank(query)) {
            return new CombinedResultSet();
        }

        return execute(conn,
                new JdbcQueryRequest(query, getResultType(props), getTimeout(props) / 1000, getParallelism(props)));
    }
}
