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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import io.github.jdbcx.Constants;
import io.github.jdbcx.Option;
import io.github.jdbcx.QueryContext;
import io.github.jdbcx.Result;
import io.github.jdbcx.executor.CommandLineExecutor;

public class ShellInterpreter extends AbstractInterpreter {
    static final Option OPTION_PATH = CommandLineExecutor.OPTION_CLI_PATH.update()
            .defaultValue(Constants.IS_WINDOWS ? "cmd /c" : "/bin/sh -c").build();
    static final Option OPTION_TIMEOUT = Option.EXEC_TIMEOUT.update().defaultValue("30000")
            .build();

    public static final List<Option> OPTIONS = Collections
            .unmodifiableList(Arrays.asList(OPTION_PATH, Option.EXEC_ERROR, OPTION_TIMEOUT, Option.INPUT_CHARSET,
                    Option.OUTPUT_CHARSET));

    private final CommandLineExecutor executor;

    public ShellInterpreter(QueryContext context, Properties config) {
        super(context);

        OPTION_TIMEOUT.setValue(config);

        this.executor = new CommandLineExecutor(OPTION_PATH.getValue(config), false, config);
    }

    @Override
    public Result<?> interpret(String query, Properties props) {
        try {
            return Result.of(executor.execute(props, null, query),
                    Option.RESULT_STRING_SPLIT_CHAR.getValue(props).getBytes(Option.OUTPUT_CHARSET.getValue(props)));
        } catch (Exception e) {
            return handleError(e, query, props);
        }
    }
}
