/*
 * Copyright 2022-2023, Zhichun Wu
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
package io.github.jdbcx.interpreter;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import io.github.jdbcx.Checker;
import io.github.jdbcx.Constants;
import io.github.jdbcx.Logger;
import io.github.jdbcx.LoggerFactory;
import io.github.jdbcx.Option;
import io.github.jdbcx.QueryContext;
import io.github.jdbcx.Result;
import io.github.jdbcx.Utils;
import io.github.jdbcx.executor.CommandLineExecutor;
import io.github.jdbcx.executor.Stream;

public class CodeQLInterpreter extends AbstractInterpreter {
    private static final Logger log = LoggerFactory.getLogger(CodeQLInterpreter.class);

    static final String DEFAULT_COMMAND = "codeql";
    static final String CONFIG_FILE = "qlpack.yml";

    static final Option OPTION_DATABASE = Option.of(new String[] { "database", "Path to the CodeQL database" });
    static final Option OPTION_SEARCH_PATH = Option
            .of(new String[] { "search.path", "Colon separated directories for searching QL packs" });
    static final Option OPTION_QUERY_ARGS = Option
            .of(new String[] { "query.args",
                    "Additional command line options except database and search-path for query" });
    static final Option OPTION_FORMAT = Option
            .of(new String[] { "format", "Output format of query result", "text", "bqrs", "csv", "json" });
    static final Option OPTION_DECODE_ARGS = Option
            .of(new String[] { "decode.args", "Additional command line options for decode" });

    static final Option OPTION_TIMEOUT = Option.EXEC_TIMEOUT.update().defaultValue("60000")
            .build();

    public static final List<Option> OPTIONS = Collections
            .unmodifiableList(Arrays.asList(Option.EXEC_ERROR, OPTION_DATABASE, OPTION_SEARCH_PATH, OPTION_FORMAT,
                    OPTION_DECODE_ARGS, OPTION_QUERY_ARGS, OPTION_TIMEOUT,
                    CommandLineExecutor.OPTION_CLI_PATH.update().defaultValue(DEFAULT_COMMAND).build(),
                    CommandLineExecutor.OPTION_CLI_TEST_ARGS.update().defaultValue("-V").build(),
                    Option.INPUT_CHARSET, Option.OUTPUT_CHARSET, Option.WORK_DIRECTORY));

    public static List<String> getAllQlPacks(Properties props) {
        final List<String> qlpacks = new LinkedList<>();
        try {
            final CodeQLInterpreter i = new CodeQLInterpreter(null, props);
            final String searchPath;
            if (!Checker.isNullOrEmpty(i.defaultSearchPath)) {
                searchPath = Utils.normalizePath(i.defaultSearchPath);
            } else {
                searchPath = i.executor.getWorkDirectory(props).toFile().getAbsolutePath();
            }

            for (String s : Stream
                    .readAllAsString(i.executor.execute(props, null, "resolve", "qlpacks", "--kind", "query",
                            "--no-recursive", "--search-path", searchPath))
                    .split("\n")) {
                if (!Checker.isNullOrBlank(s)) {
                    qlpacks.add(s.split("\\s\\(")[0]);
                }
            }
        } catch (Exception e) {
            log.debug("Failed to get all QL packs", e);
        }
        return qlpacks.isEmpty() ? Collections.emptyList() : Collections.unmodifiableList(new ArrayList<>(qlpacks));
    }

    private final String defaultDatabase;
    private final String defaultSearchPath;
    private final String defaultQueryArgs;
    private final String defaultFormat;
    private final String defaultDecodeArgs;

    private final CommandLineExecutor executor;

    static void deleteFile(File f) {
        if (f == null) {
            return;
        }
        try {
            if (!f.delete()) { // NOSONAR
                log.warn("Failed to delete CodeQL file [%s]", f);
                f.deleteOnExit();
            }
        } catch (Exception e) {
            log.warn("Error occurred when deleting CodeQL file [%s]", f);
        }
    }

    private Result<?> localQueryRun(String query, String queryFile, String bqrsFile, String database, String searchPath,
            String queryArgs, Properties props) throws IOException, TimeoutException {
        final List<String> args = new LinkedList<>();
        args.add("query");
        args.add("run");
        if (!Checker.isNullOrEmpty(queryArgs)) {
            for (String s : queryArgs.split("\\s+")) {
                if (!s.isEmpty()) {
                    args.add(s);
                }
            }
        }

        int timeout = executor.getTimeout(props);
        if (timeout > 0) {
            args.add("--timeout");
            args.add(String.valueOf(TimeUnit.MILLISECONDS.toSeconds(timeout)));
        }
        if (bqrsFile != null) {
            args.add("-o");
            args.add(bqrsFile);
        }
        if (!Checker.isNullOrEmpty(searchPath)) {
            args.add("--search-path");
            args.add(searchPath);
        }
        if (!Checker.isNullOrEmpty(database)) {
            args.add("-d");
            args.add(database);
        }
        args.add(queryFile);

        return executor.getDryRun(props) ? executor.getDryRunResult(args, query, props)
                : Result.of(executor.execute(props, null, args.toArray(Constants.EMPTY_STRING_ARRAY)), null);
    }

    private Result<?> localBqrsDecode(String bqrsFile, String format, String decodeArgs, Properties props)
            throws IOException, TimeoutException {
        final List<String> args = new LinkedList<>();
        args.add("bqrs");
        args.add("decode");
        if (!Checker.isNullOrEmpty(decodeArgs)) {
            for (String s : decodeArgs.split("\\s+")) {
                if (!s.isEmpty()) {
                    args.add(s);
                }
            }
        }

        if (!Checker.isNullOrEmpty(format)) {
            args.add("--format");
            args.add(format);
        }
        args.add(bqrsFile);

        return Result.of(executor.execute(props, null, args.toArray(Constants.EMPTY_STRING_ARRAY)), null);
    }

    public CodeQLInterpreter(QueryContext context, Properties config) {
        super(context);

        OPTION_TIMEOUT.setDefaultValueIfNotPresent(config);

        this.defaultDatabase = OPTION_DATABASE.getValue(config);
        this.defaultSearchPath = OPTION_SEARCH_PATH.getValue(config);
        this.defaultQueryArgs = OPTION_QUERY_ARGS.getValue(config);
        this.defaultFormat = OPTION_FORMAT.getValue(config);
        this.defaultDecodeArgs = OPTION_DECODE_ARGS.getValue(config);
        this.executor = new CommandLineExecutor(DEFAULT_COMMAND, config);
    }

    @SuppressWarnings("resource")
    @Override
    public Result<?> interpret(String query, Properties props) {
        OPTION_TIMEOUT.setDefaultValueIfNotPresent(props);

        File queryFile = null;
        try {
            final Path workDir = executor.getWorkDirectory(props);
            if (!workDir.resolve(CONFIG_FILE).toFile().exists()) {
                throw new IllegalArgumentException(
                        Utils.format("No [%s] found in work directory [%s]", CONFIG_FILE, workDir));
            }

            queryFile = Utils.createTempFile(workDir, Option.PROPERTY_JDBCX, ".ql", false);
            try (OutputStream out = new FileOutputStream(queryFile)) {
                long bytes = Stream.pipe(new ByteArrayInputStream(query.getBytes()), out);
                log.debug("Wrote %d bytes into file [%s]", bytes, queryFile);
            }

            final String format = OPTION_FORMAT.getValue(props, defaultFormat);

            String filePath = queryFile.getAbsolutePath();
            String bqrsFilePath = null;
            if (!Checker.isNullOrBlank(format) && !"text".equals(format)) {
                bqrsFilePath = filePath.substring(0, filePath.length() - 3).concat(".bqrs");
            }

            try {
                final Result<?> result = localQueryRun(query, filePath, bqrsFilePath,
                        Utils.normalizePath(OPTION_DATABASE.getValue(props, defaultDatabase)),
                        OPTION_SEARCH_PATH.getValue(props, defaultSearchPath),
                        OPTION_QUERY_ARGS.getValue(props, defaultQueryArgs), props);

                if (executor.getDryRun(props)) {
                    return result;
                }
                if (bqrsFilePath == null) {
                    return result;
                } else if ("bqrs".equals(format)) {
                    log.debug("Generating result in bqrs format... %s", result.get(String.class));
                    return Result.of(new FileInputStream(bqrsFilePath), null);
                } else {
                    log.debug("Generating result in %s format... %s", format, result.get(String.class));
                    return localBqrsDecode(bqrsFilePath, format, OPTION_DECODE_ARGS.getValue(props, defaultDecodeArgs),
                            props);
                }
            } finally {
                if (bqrsFilePath != null) {
                    deleteFile(new File(bqrsFilePath));
                }
            }
        } catch (Exception e) {
            return handleError(e, query, props);
        } finally {
            deleteFile(queryFile);
        }
    }
}
