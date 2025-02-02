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
package io.github.jdbcx.executor;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
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
import io.github.jdbcx.Compression;
import io.github.jdbcx.Constants;
import io.github.jdbcx.Field;
import io.github.jdbcx.Logger;
import io.github.jdbcx.LoggerFactory;
import io.github.jdbcx.Option;
import io.github.jdbcx.QueryGroup;
import io.github.jdbcx.QueryTask;
import io.github.jdbcx.Result;
import io.github.jdbcx.Row;
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

    static long[] execute(Connection conn, String query, FileConfiguration conf) throws SQLException {
        if (query.isEmpty()) {
            return new long[] { 0L, 0L };
        }

        try (Statement stmt = conn.createStatement()) {
            boolean hasResultSet = stmt.execute(query);
            long affectedRows = !hasResultSet ? Utils.getAffectedRows(stmt) : -1L;
            long reads = 0L;
            long updates = 0L;
            long rows = affectedRows > 0L ? affectedRows : 0L;
            while (true) {
                if (hasResultSet) {
                    try (ResultSet rs = stmt.getResultSet()) {
                        if (conf != null && !conf.name.isEmpty()) {
                            try (OutputStream out = Compression.getProvider(conf.compressionAlg)
                                    .compress(new FileOutputStream(conf.name), conf.compressionLevel,
                                            conf.compressionBuffer)) {
                                Result.writeTo(Result.of(rs), conf.format, conf.params, out);
                            }
                        }
                    } catch (IOException e) {
                        throw new SQLException(e);
                    }
                    reads++;
                } else {
                    updates++;
                }

                try {
                    if (!(hasResultSet = stmt.getMoreResults())) { // NOSONAR
                        if ((affectedRows = Utils.getAffectedRows(stmt)) == -1L) {
                            break;
                        } else {
                            rows += affectedRows;
                        }
                    }
                } catch (SQLFeatureNotSupportedException | UnsupportedOperationException e) {
                    break;
                }
            }

            return new long[] { reads, updates, rows };
        }
    }

    static List<Row> executeQueries(List<QueryTask> tasks, Connection conn, FileConfiguration conf,
            AtomicBoolean failedRef) throws SQLException {
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
                    final long[] rounds = execute(conn, g.getQuery(), conf);
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
            throw currentGroup != null
                    ? new SQLException(Utils.format("Failed to execute %s",
                            currentGroup.toString(currentSource)), e)
                    : e;
        } finally {
            Utils.closeQuietly(conn);
        }
        return results;
    }

    protected final String defaultPath;

    public QueryExecutor(VariableTag tag, Properties props) {
        super(tag, props);

        this.defaultPath = OPTION_PATH.getValue(props, Constants.EMPTY_STRING);
    }

    public String getPath(Properties props) {
        return Utils.normalizePath(Utils.applyVariables(OPTION_PATH.getValue(props, defaultPath),
                VariableTag.valueOf(Option.TAG.getValue(props)), props)).trim();
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
        final List<Row> results;
        if (parallel <= 1) {
            results = executeQueries(tasks, manager.get(), null, null);
        } else {
            final AtomicBoolean failedRef = new AtomicBoolean(false);
            final ExecutorService threadPool = newThreadPool(this, parallel, -1);
            final List<CompletableFuture<Void>> futures = new LinkedList<>();

            results = Collections.synchronizedList(new LinkedList<>());
            try {
                for (QueryTask t : tasks) {
                    futures.add(CompletableFuture.runAsync(() -> {
                        try {
                            results.addAll(
                                    executeQueries(Collections.singletonList(t), manager.get(), null, failedRef));
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
                            } catch (InterruptedException e) {
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
