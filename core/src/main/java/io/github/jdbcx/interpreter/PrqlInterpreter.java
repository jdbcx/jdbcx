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
package io.github.jdbcx.interpreter;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

import io.github.jdbcx.Checker;
import io.github.jdbcx.Constants;
import io.github.jdbcx.Option;
import io.github.jdbcx.QueryContext;
import io.github.jdbcx.Result;
import io.github.jdbcx.Utils;
import io.github.jdbcx.executor.CommandLineExecutor;

public class PrqlInterpreter extends AbstractInterpreter {
    static final String DEFAULT_COMMAND = "prqlc";
    static final boolean DEFAULT_FALLBACK = true;

    static final Option OPTION_TIMEOUT = Option.EXEC_TIMEOUT.update().defaultValue("10000")
            .build();
    static final Option OPTION_COMPILE_TARGET = Option.of(new String[] { "compile.target",
            "PRQL compile target without \"sql.\" prefix, empty string is same as \"any\"" });
    static final Option OPTION_COMPILE_OPTIONS = Option
            .of(new String[] { "compile.options", "Comma separated compile options for PRQLC besides target" });

    public static final List<Option> OPTIONS = Collections
            .unmodifiableList(
                    Arrays.asList(Option.EXEC_ERROR, OPTION_COMPILE_TARGET, OPTION_COMPILE_OPTIONS, OPTION_TIMEOUT,
                            CommandLineExecutor.OPTION_CLI_PATH.update().defaultValue(DEFAULT_COMMAND).build(),
                            CommandLineExecutor.OPTION_CLI_TEST_ARGS.update().defaultValue("-V").build(),
                            Option.INPUT_FILE, Option.INPUT_CHARSET, Option.OUTPUT_CHARSET));

    private final String defaultCompileTarget;
    private final String defaultCompileOptions;
    private final CommandLineExecutor executor;

    public PrqlInterpreter(QueryContext context, Properties config) {
        super(context);

        OPTION_TIMEOUT.setDefaultValueIfNotPresent(config);

        String target = OPTION_COMPILE_TARGET.getValue(config).toLowerCase(Locale.ROOT);
        this.defaultCompileTarget = Checker.isNullOrEmpty(target) || "any".equals(target) ? Constants.EMPTY_STRING
                : target;
        this.defaultCompileOptions = OPTION_COMPILE_TARGET.getValue(config).trim();
        this.executor = new CommandLineExecutor(DEFAULT_COMMAND, getVariableTag(), config);
    }

    @Override
    public Result<?> interpret(String query, Properties props) {
        OPTION_TIMEOUT.setDefaultValueIfNotPresent(props);

        InputStream input = null;
        try {
            List<String> list = new ArrayList<>();

            final String compileTarget = OPTION_COMPILE_TARGET.getValue(props, defaultCompileTarget);
            list.add("compile");
            if (!Checker.isNullOrEmpty(compileTarget)) {
                list.add("-t");
                list.add("sql.".concat(compileTarget));
            }
            list.addAll(Utils.split(OPTION_COMPILE_OPTIONS.getValue(props, defaultCompileOptions),
                    ','));
            if (executor.getDryRun(props)) {
                return executor.getDryRunResult(Collections.unmodifiableList(list), query, props);
            }
            input = executor.execute(props,
                    new ByteArrayInputStream(query.getBytes(executor.getInputCharset(props))),
                    list.toArray(Constants.EMPTY_STRING_ARRAY));
            return Result.of(input, null);
        } catch (Exception e) {
            return handleError(e, query, props, input);
        }
    }
}
