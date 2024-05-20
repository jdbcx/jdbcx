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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import io.github.jdbcx.driver.QueryParser;
import io.github.jdbcx.executor.Stream;

public final class Main {
    static final String OUTPUT_FORMAT_TSV = "TSV";
    static final String OUTPUT_FORMAT_TSV_WITH_HEADER = "TSVWithHeaders";

    private static void println() {
        System.out.println(); // NOSONAR
    }

    private static void println(Object msg, Object... args) {
        if (args == null || args.length == 0) {
            System.out.println(msg); // NOSONAR
        } else {
            System.out.println(String.format(Locale.ROOT, Objects.toString(msg), args)); // NOSONAR
        }
    }

    private static void printUsage() {
        String execFile = "jdbcx-bin";
        try {
            File file = Paths.get(Main.class.getProtectionDomain().getCodeSource().getLocation().toURI())
                    .toFile();
            if (file.isFile()) {
                execFile = file.getName();
                if (!Files.isExecutable(file.toPath())) {
                    execFile = "java -jar " + execFile;
                }
            } else {
                execFile = "java -cp " + file.getCanonicalPath() + " " + Main.class.getName();
            }
        } catch (Exception e) {
            // ignore
        }

        final int index = execFile.indexOf(' ');
        println("Usage: %s <JDBC URL> [@FILE or QUERY]",
                index > 0 ? (execFile.substring(0, index) + " [PROPERTIES]" + execFile.substring(index))
                        : (execFile + " [PROPERTIES]"));
        println();
        println("Properties: -Dkey=value [-Dkey=value]*");
        println("  loopCount\tNumber of times to repeat the same query, defaults to 1");
        println("  loopInterval\tInterval in milliseconds between repeated executions, defaults to 0");
        println("  noProperties\tWhether to pass all system properties to the underlying driver, defaults to false");
        println("  outputFormat\tOutput data format(TSV or TSVWithHeaders), defaults to TSV");
        println("  tasks\tMaximum number of tasks permitted to execute concurrently, defaults to 1");
        println("  taskCheckInterval\tInterval in milliseconds to check task completion status, defaults to 10");
        println("  validationQuery\tValidation query, defaults to empty string");
        println("  validationTimeout\tTimeout in seconds for connection validation, defaults to 3");
        println("  verbose\tWhether to show logs, defaults to false");
        println();
        println("Examples:");
        println("  -  %s 'jdbcx:sqlite::memory:' 'select 1'",
                index > 0 ? (execFile.substring(0, index) + " -Dverbose=true" + execFile.substring(index))
                        : (execFile + " -Dverbose=true"));
        println("  -  %s 'jdbcx:script:ch://explorer@play.clickhouse.com:443?ssl=true' @my.js", execFile);
    }

    static String getColumnLabel(ResultSetMetaData md, int columnIndex) throws SQLException {
        String label = md.getColumnLabel(columnIndex);
        if (label == null || label.isEmpty()) {
            label = md.getColumnName(columnIndex);
        }
        return label;
    }

    static void closeQuietly(AutoCloseable resource) {
        if (resource != null) {
            try {
                resource.close();
            } catch (Exception e) {
                // ignore
            }
        }
    }

    static Connection getOrCreateConnection(String url, Properties props, Connection conn, int validationTimeout,
            String validationQuery, boolean verbose) throws SQLException {
        if (conn != null) {
            boolean valid = false;
            try {
                if (Checker.isNullOrEmpty(validationQuery)) {
                    try {
                        valid = conn.isValid(validationTimeout);
                    } catch (Exception e) {
                        if (conn.isClosed()) {
                            conn = null;
                        }
                    }
                } else if (!conn.isClosed()) {
                    try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(validationQuery)) {
                        valid = rs.next();
                    }
                }
            } finally {
                if (!valid) {
                    if (verbose) {
                        println("* Closing [%s]...", conn);
                    }

                    closeQuietly(conn);
                    conn = null;
                }
            }
        }

        if (conn == null && !Checker.isNullOrEmpty(url)) {
            if (verbose) {
                println("* Creating new connection to [%s]...", url);
            }
            conn = DriverManager.getConnection(url, props);
        }
        return conn;
    }

    static long execute(Connection conn, String fileOrQuery, String outputFormat, boolean verbose)
            throws IOException, SQLException {
        final long startTime = verbose ? System.nanoTime() : 0L;

        if (fileOrQuery == null || fileOrQuery.isEmpty()) {
            return -1L;
        } else if (fileOrQuery.charAt(0) == '@') {
            fileOrQuery = Stream.readAllAsString(new FileInputStream(Utils.normalizePath(fileOrQuery.substring(1))));
        }

        try (Statement stmt = conn.createStatement()) {
            long rows = 0L;
            String operation = null;
            final char tab = '\t';
            final char quote = '"';
            if (stmt.execute(fileOrQuery)) {
                // only check the first result set
                try (ResultSet rs = stmt.getResultSet()) {
                    if (verbose || Utils.startsWith(outputFormat, OUTPUT_FORMAT_TSV, true)) {
                        StringBuilder builder = new StringBuilder();
                        ResultSetMetaData md = rs.getMetaData();
                        int len = md.getColumnCount();
                        if (OUTPUT_FORMAT_TSV_WITH_HEADER.equalsIgnoreCase(outputFormat)) {
                            for (int i = 1; i <= len; i++) {
                                String label = getColumnLabel(md, i);
                                if (label.indexOf(tab) != -1 || label.indexOf(quote) != -1) {
                                    builder.append(quote).append(Utils.escape(label, quote, quote)).append(quote)
                                            .append(tab);
                                } else {
                                    builder.append(label).append(tab);
                                }
                            }
                            if (builder.length() > 0) {
                                builder.setLength(builder.length() - 1);
                            }
                            println(builder.toString());
                        }

                        while (rs.next()) {
                            builder.setLength(0);
                            for (int i = 1; i <= len; i++) {
                                String value = String.valueOf(rs.getObject(i));
                                if (value.indexOf(tab) != -1 || value.indexOf(quote) != -1) {
                                    builder.append(quote).append(Utils.escape(value, quote, quote)).append(quote)
                                            .append(tab);
                                } else {
                                    builder.append(value).append(tab);
                                }
                            }
                            if (builder.length() > 0) {
                                builder.setLength(builder.length() - 1);
                            }
                            println(builder.toString());
                            rows++;
                        }

                        operation = "Read";
                    } else {
                        while (rs.next()) {
                            rows++;
                        }
                    }
                }
            } else {
                try {
                    rows = stmt.getLargeUpdateCount();
                } catch (Exception e) {
                    rows = stmt.getUpdateCount();
                }
                operation = "Affected";
            }

            if (verbose) {
                long elapsedNanos = System.nanoTime() - startTime;
                println("* %s %,d rows in %,.2f ms (%,.2f rows/s)", operation, rows, elapsedNanos / 1_000_000D,
                        rows * 1_000_000_000D / elapsedNanos);
            }
            return rows;
        }
    }

    static boolean executeQueries(String url, boolean noProperties, int validationTimeout, String validationQuery,
            List<String[]> queries, String outputFormat, AtomicBoolean failedRef, boolean verbose)
            throws IOException, SQLException {
        if (queries == null || queries.isEmpty()) {
            return false;
        }

        Connection conn = null;
        try {
            for (String[] pair : queries) {
                if (failedRef != null && failedRef.get()) {
                    return false; // fail fast
                } else {
                    if (verbose) {
                        println("* Executing [%s]...", pair[0]);
                    }
                }

                conn = getOrCreateConnection(url, noProperties ? new Properties() : System.getProperties(),
                        conn, validationTimeout, validationQuery, verbose);
                final long rows = execute(conn, pair[1], outputFormat, verbose);
                if (rows < 0L) {
                    if (failedRef != null) {
                        failedRef.compareAndSet(false, true);
                    }
                    return false;
                }
            }
        } finally {
            closeQuietly(conn);
        }
        return true;
    }

    static List<List<String[]>> splitTasks(List<String[]> queries, int tasks) {
        final List<List<String[]>> splittedTasks = new ArrayList<>(tasks);
        final int len = queries.size() / tasks;
        for (int i = 0; i < tasks; i++) {
            splittedTasks.add(new ArrayList<>(len));
        }

        int i = 0;
        for (String[] pair : queries) {
            splittedTasks.get(i % tasks).add(pair);
            i++;
        }
        return Collections.unmodifiableList(splittedTasks);
    }

    public static void main(String[] args) throws Exception {
        if ((args == null || args.length < 1) || args.length > 2) {
            printUsage();
            System.exit(0);
        }

        final long loopCount = Long.getLong("loopCount", 1L);
        final long loopInterval = Long.getLong("loopInterval", 0L);
        final boolean noProperties = Boolean.parseBoolean(System.getProperty("noProperties", Boolean.FALSE.toString()));
        final String outputFormat = System.getProperty("outputFormat", OUTPUT_FORMAT_TSV);
        final int tasks = Integer.getInteger("tasks", 1);
        final long taskCheckInterval = Long.getLong("taskCheckInterval", 10);
        final String validationQuery = System.getProperty("validationQuery", Constants.EMPTY_STRING);
        final int validationTimeout = Integer.getInteger("validationTimeout", 3);
        final boolean verbose = Boolean.parseBoolean(System.getProperty("verbose", Boolean.FALSE.toString()));
        final String url = args[0];
        final String fileOrQuery = args[1];

        final List<String[]> queries;
        if (fileOrQuery == null || fileOrQuery.isEmpty()) {
            queries = Collections.emptyList();
        } else if (fileOrQuery.charAt(0) == '@') {
            List<Path> list = Utils.findFiles(fileOrQuery.substring(1), ".sql");
            List<String[]> merged = new LinkedList<>();
            for (Path p : list) {
                String content = Stream.readAllAsString(new FileInputStream(p.toFile()));
                StringBuilder builder = new StringBuilder(p.toFile().getName()).append('-');
                int offset = builder.length();
                int i = 1;
                for (String[] pair : QueryParser.split(content)) {
                    pair[0] = builder.append(i++).append(": ").append(pair[0]).toString();
                    merged.add(pair);
                    builder.setLength(offset);
                }
            }
            queries = Collections.unmodifiableList(new ArrayList<>(merged));
        } else {
            queries = QueryParser.split(fileOrQuery);
        }

        if (queries.isEmpty()) {
            if (verbose) {
                println("* No query to execute.");
            }
            System.exit(1);
        }

        long count = 0L;
        boolean failed = false;
        do {
            if (tasks <= 1) {
                failed = !executeQueries(url, noProperties, validationTimeout, validationQuery, queries, outputFormat,
                        null, verbose);
            } else {
                final AtomicBoolean failedRef = new AtomicBoolean(failed);
                final List<List<String[]>> splittedTasks = splitTasks(queries, tasks);
                final List<CompletableFuture<Void>> futures = new ArrayList<>(splittedTasks.size());
                for (List<String[]> list : splittedTasks) {
                    futures.add(CompletableFuture.runAsync(() -> {
                        try {
                            executeQueries(url, noProperties, validationTimeout, validationQuery, list,
                                    outputFormat, failedRef, verbose);
                        } catch (Exception e) {
                            throw new IllegalStateException(e);
                        }
                    }));
                }

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
                                cancel = true;
                                Thread.currentThread().interrupt();
                            } catch (CancellationException | ExecutionException e) {
                                cancel = true;
                                e.printStackTrace(); // NOSONAR
                            }
                        }
                    }
                    Thread.sleep(taskCheckInterval);
                }

                if (verbose) {
                    println("* All %d queries completed with %d tasks.", queries.size(), tasks);
                }
                failed = failedRef.get();
            }

            count++;
            if (loopInterval > 0L) {
                if (verbose) {
                    println("* Sleep for %,d ms...", loopInterval);
                }
                Thread.sleep(loopInterval);
            }
        } while (count < loopCount);
        System.exit(failed ? 1 : 0);
    }

    private Main() {
    }
}
