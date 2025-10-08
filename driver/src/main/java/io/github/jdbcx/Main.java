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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.jar.Manifest;

import io.github.jdbcx.driver.ManagedConnection;
import io.github.jdbcx.driver.QueryParser;

public final class Main {
    static final class Arguments {
        final Properties connectionProps;
        final long loopCount;
        final long loopInterval;
        final String outputFile;
        final Compression outputCompression;
        final int compressionLevel;
        final int compressionBuffer;
        final Format outputFormat;
        final Properties outputParams;
        final int tasks;
        final long taskCheckInterval;
        final String validationQuery;
        final int validationTimeout;
        final boolean verbose;
        final String url;
        final List<String[]> queries;

        static List<String[]> extractQueries(String[] args) throws IOException {
            final List<String[]> queries;
            final int len = args.length;
            if (len == 1) {
                queries = Collections.emptyList();
            } else {
                List<String[]> list = new LinkedList<>();
                for (int i = 1; i < len; i++) {
                    String fileOrQuery = args[i];
                    if (fileOrQuery == null || fileOrQuery.isEmpty()) {
                        // skip
                    } else if (fileOrQuery.charAt(0) == '@') {
                        List<Path> paths = Utils.findFiles(fileOrQuery.substring(1), ".sql");
                        List<String[]> merged = new LinkedList<>();
                        for (Path p : paths) {
                            String content = Stream.readAllAsString(new FileInputStream(p.toFile()));
                            StringBuilder builder = new StringBuilder(p.toFile().getName()).append('-');
                            int offset = builder.length();
                            int j = 1;
                            for (String[] pair : QueryParser.split(content)) {
                                pair[0] = builder.append(j++).append(": ").append(pair[0]).toString();
                                merged.add(pair);
                                builder.setLength(offset);
                            }
                        }
                        list.addAll(merged);
                    } else {
                        list.addAll(QueryParser.split(fileOrQuery));
                    }
                }
                queries = Collections.unmodifiableList(new ArrayList<>(list));
            }
            return queries;
        }

        Arguments(String[] args) throws IOException {
            this.connectionProps = new Properties();
            this.connectionProps
                    .putAll(Utils.toKeyValuePairs(System.getProperty("connectionProps", Constants.EMPTY_STRING)));

            this.loopCount = Long.getLong("loopCount", 1L);
            this.loopInterval = Long.getLong("loopInterval", 0L);
            this.outputFile = System.getProperty("outputFile", Constants.EMPTY_STRING);

            final Compression defaultCompression = Compression.fromFileName(outputFile);
            final Format defaultFormat = Format.fromFileName(outputFile);

            this.outputCompression = Compression
                    .valueOf(System
                            .getProperty("outputCompression",
                                    defaultCompression != null ? defaultCompression.name() : Compression.NONE.name())
                            .toUpperCase(Locale.ROOT));
            this.compressionLevel = Integer.getInteger("compressionLevel", -1);
            this.compressionBuffer = Integer.getInteger("compressionBuffer", 0);
            this.outputFormat = Format
                    .valueOf(System
                            .getProperty("outputFormat",
                                    defaultFormat != null ? defaultFormat.name() : Format.TSV.name())
                            .toUpperCase(Locale.ROOT));
            this.outputParams = new Properties();
            this.outputParams.putAll(Utils.toKeyValuePairs(System.getProperty("outputParams", Constants.EMPTY_STRING)));
            this.tasks = Integer.getInteger("tasks", 1);
            this.taskCheckInterval = Long.getLong("taskCheckInterval", 10);
            this.validationQuery = System.getProperty("validationQuery", Constants.EMPTY_STRING);
            this.validationTimeout = Integer.getInteger("validationTimeout", 3);
            this.verbose = Boolean.parseBoolean(System.getProperty("verbose", Boolean.FALSE.toString()));
            this.url = args[0];
            this.queries = extractQueries(args);
        }

        Arguments(Arguments args, List<String[]> queries) {
            this(args.connectionProps, args.loopCount, args.loopInterval, args.outputFile, args.outputCompression,
                    args.compressionLevel, args.compressionBuffer, args.outputFormat, args.outputParams, args.tasks,
                    args.taskCheckInterval, args.url, queries, args.validationQuery, args.validationTimeout,
                    args.verbose);
        }

        Arguments(Properties connectionProps, long loopCount, long loopInterval, String outputFile, // NOSONAR
                Compression outputCompression, int compressionLevel, int compressionBuffer, Format outputFormat,
                Properties outputParams, int tasks, long taskCheckInterval, String url, List<String[]> queries,
                String validationQuery, int validationTimeout, boolean verbose) {
            this.connectionProps = connectionProps;
            this.loopCount = loopCount;
            this.loopInterval = loopInterval;
            this.outputFile = outputFile;
            this.outputCompression = outputCompression;
            this.compressionLevel = compressionLevel;
            this.compressionBuffer = compressionBuffer;
            this.outputFormat = outputFormat;
            this.outputParams = outputParams;
            this.tasks = tasks;
            this.taskCheckInterval = taskCheckInterval;
            this.url = url;
            this.queries = queries;
            this.validationQuery = validationQuery;
            this.validationTimeout = validationTimeout;
            this.verbose = verbose;
        }
    }

    static final class UnclosableOutputStream extends FilterOutputStream {
        UnclosableOutputStream(OutputStream out) {
            super(out);
        }

        @Override
        public void close() throws IOException {
            out.flush();
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            out.write(b, off, len);
        }
    }

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

    private static int printUsage() {
        String execFile = "jdbcx-bin";
        try {
            File file = Paths.get(Main.class.getProtectionDomain().getCodeSource().getLocation().toURI())
                    .toFile();
            if (file.isFile()) {
                execFile = file.getName();
                if (execFile.endsWith(".jar")) {
                    boolean hasMainClass = false;
                    try {
                        Manifest manifest = new Manifest(Main.class.getResourceAsStream("/META-INF/MANIFEST.MF"));
                        String mainClassName = manifest.getMainAttributes().getValue("Main-Class");
                        hasMainClass = Main.class.getName().equals(mainClassName);
                    } catch (IOException e) {
                        // ignore
                    }
                    execFile = hasMainClass ? "java -jar " + execFile
                            : "java -cp " + execFile + " " + Main.class.getName();
                }
            } else {
                execFile = "java -cp " + file.getCanonicalPath() + " " + Main.class.getName();
            }
        } catch (Exception e) {
            // ignore
        }

        final int index = execFile.indexOf(' ');
        final String cliTemplate = index > 0 ? (execFile.substring(0, index) + " %s" + execFile.substring(index))
                : (execFile + " %s");
        println("Usage: %s COMMAND",
                index > 0 ? (execFile.substring(0, index) + " [PROPERTIES]" + execFile.substring(index))
                        : (execFile + " [PROPERTIES]"));
        println("Execute queries against the specified URL, sourcing them from files, standard input, or command-line arguments.");
        println();
        println("Commands:");
        println("  help, -h, --help          Display this help and exit");
        println("  decrypt [ENCRYPTED TEXT]  Decrypt the text from standard input or command-line argument");
        println("  encrypt [ORIGINAL TEXT]   Encrypt the text from standard input or command-line argument");
        println("  keygen                    Generate secure key for encryption");
        println("  token [KEY1=VALUE1;...]   Create digitally signed access token");
        println("  URL [@FILE or QUERY...]   Execute queries against the specified URL, sourcing them from files, standard input, or command-line arguments");
        println();
        println("Properties: -Dkey=value [-Dkey=value]*");
        println("  connectionProps   Comma separated connection properties (e.g. 'ssl=true,sslmode=none')");
        println("  loopCount         Number of times to repeat the same query, defaults to 1");
        println("  loopInterval      Interval in milliseconds between repeated executions, defaults to 0");
        println("  outputFile        Output file name, including its extension, indicates data format and compression method (e.g. out.csv.gz), defaults to empty string");
        println("  outputCompression Output compression method, defaults to NONE");
        println("  compressionLevel  Output compression level, defaults to -1");
        println("  compressionBuffer Output buffer size for compression, defaults to 0");
        println("  outputFormat      Output data format(TSV or TSVWithHeaders), defaults to TSV");
        println("  outputParams      Comma separated output parameters (e.g. 'codec=zstd,level=9' for parquet), defaults to empty string");
        println("  tasks             Maximum number of tasks permitted to execute concurrently, defaults to 1");
        println("  taskCheckInterval Interval in milliseconds to check task completion status, defaults to 10");
        println("  validationQuery   Validation query, defaults to empty string");
        println("  validationTimeout Timeout in seconds for connection validation, defaults to 3");
        println("  verbose           Whether to show logs, defaults to false");
        println();
        println("Examples:");
        println("  -  %s keygen", execFile);
        println("  -  %s token 'issuer=me;subject=you;audience=a1,a2;expires=1440;allowed_ips=127.0.0.1,192.168.1.0/24'",
                Utils.format(cliTemplate, " -Dverbose=true"));
        println("  -  %s encrypt 'plain text to encrypt'", Utils.format(cliTemplate, " -Dverbose=true"));
        println("  -  %s 'jdbcx:duckdb:' 'select 1'", Utils.format(cliTemplate, " -Dverbose=true"));
        println("  -  %s 'jdbcx:script:ch://localhost' '@*.js'", execFile);
        println("  -  %s 'jdbcx:sqlite::memory:' 'select 1' 'select 2'",
                Utils.format(cliTemplate, " -DconnectionProps=secure_delete=true,transaction_mode=EXCLUSIVE"));
        return 0;
    }

    static Connection getOrCreateConnection(String url, Properties props, Connection conn, int validationTimeout,
            String validationQuery) throws SQLException {
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
                    Utils.closeQuietly(conn);
                    conn = null;
                }
            }
        }

        if (conn == null && !Checker.isNullOrEmpty(url)) {
            conn = DriverManager.getConnection(url, props);
        }
        return conn;
    }

    static int[] execute(Connection conn, String query, String outputFile, Format outputFormat, Properties outputParams,
            Compression outputCompression, int compressionLevel, int compressionBuffer)
            throws IOException, SQLException {
        if (query.isEmpty()) {
            return new int[] { 0, 0 };
        }

        try (Statement stmt = conn.createStatement()) {
            boolean hasResultSet = stmt.execute(query);
            long affectedRows = !hasResultSet ? Utils.getAffectedRows(stmt) : -1L;
            int reads = 0;
            int updates = 0;
            while (true) {
                if (hasResultSet) {
                    try (Result<?> result = Result.of(stmt.getResultSet());
                            OutputStream out = Compression.getProvider(outputCompression)
                                    .compress(
                                            outputFile.isEmpty() ? new UnclosableOutputStream(System.out) // NOSONAR
                                                    : new FileOutputStream(outputFile),
                                            compressionLevel, compressionBuffer)) {
                        Result.writeTo(result, outputFormat, outputParams, out);
                    }
                    if (outputFile.isEmpty()) {
                        println();
                    }
                    reads++;
                } else {
                    updates++;
                }

                try {
                    if (!(hasResultSet = stmt.getMoreResults())
                            && (affectedRows = Utils.getAffectedRows(stmt)) == -1L) { // NOSONAR
                        break;
                    }
                } catch (SQLFeatureNotSupportedException | UnsupportedOperationException e) {
                    break;
                }
            }

            return new int[] { reads, updates };
        }
    }

    static boolean executeQueries(Arguments args, AtomicBoolean failedRef) throws IOException, SQLException {
        Connection conn = null;
        try {
            int i = 1;
            for (String[] pair : args.queries) {
                if (failedRef != null && failedRef.get()) {
                    return false; // fail fast
                } else {
                    if (args.verbose) {
                        println("* %d. [%s] Executing...", i, pair[0]);
                    }
                }

                conn = getOrCreateConnection(args.url, args.connectionProps, conn, args.validationTimeout,
                        args.validationQuery);

                final long startTime = args.verbose ? System.nanoTime() : 0L;
                final int[] rounds = execute(conn, pair[1], args.outputFile, args.outputFormat, args.outputParams,
                        args.outputCompression, args.compressionLevel, args.compressionBuffer);
                final int reads = rounds[0];
                final int updates = rounds[1];
                final int total = reads + updates;
                if (args.verbose) {
                    println("* %d. [%s] Executed %,d queries (%,d reads, %,d updates) in %,.2f ms", i, pair[0], total,
                            reads, updates, (System.nanoTime() - startTime) / 1_000_000D);
                }
                if (total < 1) {
                    if (failedRef != null) {
                        failedRef.compareAndSet(false, true);
                    }
                    return false;
                }
                i++;
            }
        } finally {
            Utils.closeQuietly(conn);
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

    static ConfigManager getConfigManager() throws SQLException {
        try (Connection conn = DriverManager
                .getConnection(new StringBuilder(Option.PROPERTY_JDBCX).append(':').toString())) {
            if (conn instanceof ManagedConnection) { // NOSONAR
                return ((ManagedConnection) conn).getManager().getConfigManager();
            }
            throw new SQLFeatureNotSupportedException("Only JDBCX managed connection is supported but we got: " + conn);
        }
    }

    static String getAlgorithmName(Properties props) {
        return Utils.split(ConfigManager.OPTION_ALGORITHM.getValue(props), '/', true, true, false).get(0);
    }

    static int generateToken(String kvps, boolean verbose) throws SQLException {
        Map<String, String> params = new HashMap<>();
        params.putAll(Utils.toKeyValuePairs(kvps, ';', false));
        String issuer = params.remove("issuer");
        String subject = params.remove("subject");
        String audience = params.remove("audience");
        if (Checker.isNullOrBlank(issuer) || Checker.isNullOrBlank(subject)) {
            throw new IllegalArgumentException("Non-blank issuer and subject are required");
        } else {
            issuer = issuer.trim();
            subject = subject.trim();
        }

        String expires = params.remove("expires");
        int expirationMinutes = 0;
        if (!Checker.isNullOrBlank(expires)) {
            expirationMinutes = Integer.parseInt(expires.trim());
        }
        ConfigManager manager = getConfigManager();
        if (verbose) {
            println(Utils.format("* Generating JWT(issuer=%s, subject=%s)...", issuer, subject));
        }
        println(manager.generateToken(issuer, subject, audience, expirationMinutes, params));
        return 0;
    }

    static int generateKey(boolean verbose) throws SQLException {
        Properties props = System.getProperties();
        ConfigManager manager = getConfigManager();
        String algorithm = getAlgorithmName(props);
        String keyBits = ConfigManager.OPTION_KEY_SIZE_BITS.getValue(props);
        if (verbose) {
            println(Utils.format("* Generating key(algorithm=%s, keyBits=%s)...", algorithm, keyBits));
        }
        println(manager.generateKey(algorithm, Integer.parseInt(keyBits)));
        return 0;
    }

    static int decryptText(String text, String associatedData, boolean verbose) throws SQLException {
        Properties props = System.getProperties();
        ConfigManager manager = getConfigManager();
        String algorithm = getAlgorithmName(props);
        String keyFile = ConfigManager.OPTION_SECRET_FILE.getValue(props);
        if (verbose) {
            println(Utils.format("* Decrypting text(algorithm=%s, keyFile=%s)...", algorithm, keyFile));
        }
        println(manager.decrypt(manager.loadKey(Utils.getPath(keyFile, true), algorithm), text, associatedData,
                Constants.DEFAULT_CHARSET));
        return 0;
    }

    static int encryptText(String text, String associatedData, boolean verbose) throws SQLException {
        Properties props = System.getProperties();
        ConfigManager manager = getConfigManager();
        String algorithm = getAlgorithmName(props);
        String keyFile = ConfigManager.OPTION_SECRET_FILE.getValue(props);
        if (verbose) {
            println(Utils.format("* Encrypting text(algorithm=%s, keyFile=%s)...", algorithm, keyFile));
        }
        println(manager.encrypt(manager.loadKey(Utils.getPath(keyFile, true), algorithm), text, associatedData,
                Constants.DEFAULT_CHARSET));
        return 0;
    }

    static int process(String[] arguments) throws InterruptedException, IOException, SQLException {
        if ((arguments == null || arguments.length < 1)) {
            return printUsage();
        } else if (arguments.length == 1) {
            final boolean verbose = Boolean.parseBoolean(System.getProperty("verbose", Boolean.FALSE.toString()));
            final String command = arguments[0].toLowerCase();
            String prompt = "* Enter your query and press Ctrl+D to execute.";
            switch (command) {
                case "help":
                case "-h":
                case "--help": // NOSONAR
                    return printUsage();
                case "decrypt":
                case "encrypt": // NOSONAR
                    prompt = Utils.format("* Enter the text you'd like to %s. Press Ctrl+D to finish.", command);
                    break;
                case "keygen":
                    return generateKey(verbose);
                default:
                    break;
            }
            if (verbose) {
                println(prompt);
            }
            arguments = new String[] { arguments[0], Stream.readAllAsString(System.in) };
        }

        final Arguments args = new Arguments(arguments);
        if ("decrypt".equals(args.url)) {
            return decryptText(arguments[1], arguments.length > 2 ? arguments[2] : null, args.verbose);
        } else if ("encrypt".equals(args.url)) {
            return encryptText(arguments[1], arguments.length > 2 ? arguments[2] : null, args.verbose);
        } else if ("token".equals(args.url)) {
            return generateToken(arguments[1], args.verbose);
        }
        if (args.queries.isEmpty()) {
            if (args.verbose) {
                println("* No query to execute.");
            }
            return 1;
        }

        long count = 0L;
        boolean failed = false;
        do {
            if (args.tasks <= 1) {
                failed = !executeQueries(args, null);
            } else {
                final AtomicBoolean failedRef = new AtomicBoolean(failed);
                final List<List<String[]>> splittedTasks = splitTasks(args.queries, args.tasks);
                final List<CompletableFuture<Void>> futures = new ArrayList<>(splittedTasks.size());
                for (List<String[]> list : splittedTasks) {
                    final Arguments newArgs = new Arguments(args, list);
                    futures.add(CompletableFuture.runAsync(() -> {
                        try {
                            executeQueries(newArgs, failedRef);
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
                    Thread.sleep(args.taskCheckInterval);
                }

                if (args.verbose) {
                    println("* All %d queries completed with %d tasks.", args.queries.size(), args.tasks);
                }
                failed = failedRef.get();
            }

            count++;
            if (args.loopInterval > 0L) {
                if (args.verbose) {
                    println("* Sleep for %,d ms...", args.loopInterval);
                }
                Thread.sleep(args.loopInterval);
            }
        } while (count < args.loopCount);
        return failed ? 1 : 0;
    }

    public static void main(String[] arguments) throws Exception {
        System.exit(process(arguments));
    }

    private Main() {
    }
}
