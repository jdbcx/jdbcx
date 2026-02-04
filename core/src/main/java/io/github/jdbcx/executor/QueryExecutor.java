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
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import io.github.jdbcx.Checker;
import io.github.jdbcx.Constants;
import io.github.jdbcx.Field;
import io.github.jdbcx.Logger;
import io.github.jdbcx.LoggerFactory;
import io.github.jdbcx.Option;
import io.github.jdbcx.QueryGroup;
import io.github.jdbcx.QueryTask;
import io.github.jdbcx.Result;
import io.github.jdbcx.Row;
import io.github.jdbcx.Threads;
import io.github.jdbcx.Utils;
import io.github.jdbcx.VariableTag;

public class QueryExecutor extends AbstractExecutor {
    private static final Logger log = LoggerFactory.getLogger(QueryExecutor.class);

    private static final List<Field> dryRunFields = Collections
            .unmodifiableList(Arrays.asList(Field.of("path"), FIELD_QUERY, FIELD_OPTIONS));
    private static final List<Field> resultFields = Collections
            .unmodifiableList(
                    Arrays.asList(Field.of("thread"), Field.of("connection", JDBCType.INTEGER), Field.of("source"),
                            Field.of("group", JDBCType.INTEGER), FIELD_QUERY, Field.of("query_count", JDBCType.BIGINT),
                            Field.of("update_count", JDBCType.BIGINT), Field.of("total_operations", JDBCType.BIGINT),
                            Field.of("affected_rows", JDBCType.BIGINT), Field.of("elapsed_ms", JDBCType.BIGINT)));

    public static final Option OPTION_PATH = Option.of(new String[] { "path", "File path(s) (supports globbing)" });
    public static final Option OPTION_RESULT = JdbcExecutor.OPTION_RESULT.update()
            .defaultValue(JdbcExecutor.RESULT_SUMMARY).build();

    static long[] execute(Connection conn, String query) throws SQLException {
        if (query.isEmpty()) {
            return new long[] { 0L, 0L };
        }

        try (Statement stmt = conn.createStatement()) {
            boolean hasResultSet = stmt.execute(query);
            long affectedRows = !hasResultSet ? JdbcExecutor.getUpdateCount(stmt) : -1L;
            long reads = 0L;
            long updates = 0L;
            long rows = affectedRows > 0L ? affectedRows : 0L;
            while (true) {
                if (hasResultSet) {
                    reads++;
                } else {
                    updates++;
                }

                try {
                    if (!(hasResultSet = stmt.getMoreResults())) { // NOSONAR
                        if ((affectedRows = JdbcExecutor.getUpdateCount(stmt)) == -1L) {
                            break;
                        } else {
                            rows += affectedRows;
                        }
                    }
                } catch (SQLFeatureNotSupportedException | UnsupportedOperationException | NoSuchMethodError e) {
                    break;
                }
            }

            return new long[] { reads, updates, rows };
        }
    }

    static List<Row> executeQueries(List<QueryTask> tasks, Connection conn, AtomicBoolean failedRef)
            throws SQLException {
        final String thread = Thread.currentThread().getName();
        final int connection = conn.hashCode();
        final List<Row> results = new LinkedList<>();

        String currentSource = Constants.EMPTY_STRING;
        QueryGroup currentGroup = null;
        try {
            for (QueryTask t : tasks) {
                currentSource = t.getSource();
                for (QueryGroup g : t.getGroups()) {
                    currentGroup = g;
                    if (failedRef != null && failedRef.get()) {
                        return Collections.emptyList(); // fail fast
                    }

                    final String queryInfo = g.toString(t.getSource());
                    log.debug("Executing %s...", queryInfo);

                    final long startTime = System.currentTimeMillis();
                    final long[] rounds = execute(conn, g.getQuery());
                    final long reads = rounds[0];
                    final long updates = rounds[1];
                    final long total = reads + updates;
                    final long rows = rounds[2];
                    final long elapsed = System.currentTimeMillis() - startTime;
                    log.debug("Completed %s (%,d reads, %,d updates) in %,d ms", queryInfo, reads, updates, elapsed);
                    if (total < 1) {
                        if (failedRef != null) {
                            failedRef.compareAndSet(false, true);
                        }
                        return Collections.emptyList();
                    }
                    results.add(Row.of(resultFields,
                            new Object[] { thread, connection, t.getSource(), g.getIndex() + 1, g.getDescription(),
                                    reads, updates, total, rows, elapsed }));
                }
            }
        } catch (SQLException e) {
            throw new SQLException(Utils.format("Failed to execute %s", currentGroup.toString(currentSource)), e);
        } finally {
            Utils.closeQuietly(conn);
        }
        return results;
    }

    protected final String defaultPath;
    protected final String defaultResultType;

    public QueryExecutor(VariableTag tag, Properties props) {
        super(tag, props);

        this.defaultPath = OPTION_PATH.getValue(props, Constants.EMPTY_STRING);
        this.defaultResultType = OPTION_RESULT.getValue(props);
    }

    public String getPath(Properties props) {
        return Utils.normalizePath(Utils.applyVariables(OPTION_PATH.getValue(props, defaultPath),
                VariableTag.valueOf(Option.TAG.getValue(props)), props)).trim();
    }

    public String getResultType(Properties props) {
        return props != null ? props.getProperty(OPTION_RESULT.getName(), defaultResultType) : defaultResultType;
    }

    public Result<?> execute(String query, Supplier<Connection> manager, Properties props) throws SQLException {
        if (getDryRun(props)) {
            return Result.of(Row.of(dryRunFields, new Object[] { getPath(props), query, props }));
        }

        final String path = getPath(props);
        final List<QueryTask> tasks;
        if (Checker.isNullOrEmpty(path)) {
            final String inputFile = getInputFile(props);
            if (Checker.isNullOrEmpty(inputFile)) {
                tasks = QueryTask.ofQuery(query);
            } else {
                tasks = QueryTask.ofFile(inputFile, getInputCharset(props));
            }
        } else {
            tasks = QueryTask.ofPath(path, getInputCharset(props));
        }

        if (tasks.isEmpty()) {
            return Result.of(resultFields);
        }

        final int parallel = getParallelism(props);
        final JdbcQueryRequest request = new JdbcQueryRequest(query, getResultType(props), getTimeout(props) / 1000,
                parallel);
        final List<Row> results;
        if (parallel <= 1) {
            final Connection conn = manager.get();
            if (JdbcExecutor.RESULT_SUMMARY.equals(request.resultType)) {
                results = executeQueries(tasks, conn, null);
            } else {
                final int taskCount = tasks.size();
                int count = 0;
                String currentSource = Constants.EMPTY_STRING;
                QueryGroup currentGroup = null;
                try {
                    for (QueryTask t : tasks) {
                        final int groupCount = t.getGroups().size();
                        final boolean lastTask = (++count == taskCount);
                        currentSource = t.getSource();
                        int index = 0;
                        for (QueryGroup g : t.getGroups()) {
                            currentGroup = g;
                            if (lastTask && (++index == groupCount)) {
                                Object result = JdbcExecutor.execute(conn,
                                        request.update().query(g.getQuery()).build());
                                return result instanceof ResultSet ? Result.of((ResultSet) result) // NOSONAR
                                        : Result.of((Long) result);
                            }
                            final String queryInfo = g.toString(t.getSource());
                            log.debug("Executing %s...", queryInfo);
                            final long startTime = System.currentTimeMillis();
                            final long[] stats = execute(conn, g.getQuery());
                            log.debug("Completed %s (%,d reads, %,d updates, %,d affected rows) in %,d ms", queryInfo,
                                    stats[0], stats[1], stats[2], System.currentTimeMillis() - startTime);
                        }
                    }
                } catch (SQLException e) {
                    throw new SQLException(Utils.format("Failed to execute %s", currentGroup.toString(currentSource)),
                            e);
                }
                throw new SQLException("No valid result");
            }
        } else {
            final AtomicBoolean failedRef = new AtomicBoolean(false);
            final ExecutorService threadPool = Threads.newPool(this, parallel, -1); // NOSONAR
            final List<CompletableFuture<Void>> futures = new LinkedList<>();

            results = Collections.synchronizedList(new LinkedList<>());
            try {
                for (QueryTask t : tasks) {
                    futures.add(CompletableFuture.runAsync(() -> {
                        try {
                            results.addAll(executeQueries(Collections.singletonList(t), manager.get(), failedRef));
                        } catch (Exception e) {
                            throw new IllegalStateException(e);
                        }
                    }, threadPool));
                }
                threadPool.shutdown();

                while (!futures.isEmpty()) {
                    boolean cancel = false;
                    for (Iterator<CompletableFuture<Void>> it = futures.iterator(); it.hasNext();) {
                        CompletableFuture<Void> future = it.next();
                        if (cancel) {
                            future.cancel(true);
                            it.remove();
                        } else if (future.isDone()) {
                            it.remove();
                            try {
                                future.get();
                            } catch (InterruptedException e) { // NOSONAR
                                failedRef.compareAndSet(false, cancel = true);
                                log.warn("Query is interrupted", e);
                            } catch (CancellationException e) {
                                failedRef.compareAndSet(false, cancel = true);
                                log.warn("Query is cancelled", e);
                            } catch (ExecutionException e) {
                                failedRef.compareAndSet(false, cancel = true);
                                log.error("Abort query due to execution failure", e.getCause());
                            }
                        }
                    }
                    Thread.sleep(100L);
                }
            } catch (InterruptedException e) {
                // ignore
                log.warn("Query execution has been interrupted. It may take sometime to shutdown all running tasks.",
                        e);
                Thread.currentThread().interrupt();
            } finally {
                threadPool.shutdownNow();
            }

            if (failedRef.get()) {
                throw new SQLException("Query failed, please check logs for details");
            }
        }
        return Result.of(resultFields, results);
    }
}
