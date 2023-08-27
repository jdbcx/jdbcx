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

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

import io.github.jdbcx.Checker;
import io.github.jdbcx.Constants;
import io.github.jdbcx.Option;
import io.github.jdbcx.QueryContext;
import io.github.jdbcx.Result;
import io.github.jdbcx.executor.ScriptExecutor;

public class ScriptInterpreter extends AbstractInterpreter {
    static final Option OPTION_TIMEOUT = Option.EXEC_TIMEOUT.update().defaultValue("15000")
            .build();

    public static final String KEY_VARS = "vars";

    public static final Option OPTION_VAR_HELPER = Option
            .of(new String[] { "var.helper", "Variable name of the helper object", "helper" });

    public static final List<Option> OPTIONS = Collections.unmodifiableList(Arrays.asList(Option.EXEC_ERROR,
            OPTION_TIMEOUT, ScriptExecutor.OPTION_LANGUAGE, ScriptExecutor.OPTION_BINDING_ERROR, OPTION_VAR_HELPER));

    private final ScriptExecutor executor;

    @SuppressWarnings("unchecked")
    public ScriptInterpreter(QueryContext context, Properties config, ClassLoader loader) {
        super(context);

        OPTION_TIMEOUT.setDefaultValueIfNotPresent(config);

        String varHelper = OPTION_VAR_HELPER.getValue(config);

        Object map = context.get(KEY_VARS);
        Map<String, Object> vars = map instanceof Map ? (Map<String, Object>) map : new HashMap<>();
        if (!Checker.isNullOrEmpty(varHelper)) {
            vars.put(varHelper, ScriptHelper.getInstance());
        }
        executor = new ScriptExecutor(ScriptExecutor.OPTION_LANGUAGE.getValue(config), config, vars, loader);
    }

    @Override
    public Result<?> interpret(String query, Properties props) {
        OPTION_TIMEOUT.setDefaultValueIfNotPresent(props);

        try {
            final Object result = executor.execute(query, props, null);
            if (result instanceof Result) {
                return (Result<?>) result;
            } else if (result instanceof InputStream) {
                return process(query, (InputStream) result, props);
            } else if (result instanceof Reader) {
                return process(query, (Reader) result, props);
            } else if (result instanceof ResultSet) {
                return Result.of((ResultSet) result);
            } else if (result instanceof Iterable) {
                return Result.of(Result.DEFAULT_FIELDS, (Iterable<?>) result);
            } else if (result instanceof Object[]) {
                return Result.of((Object[]) result);
            } else {
                return Result.of(result != null ? result.toString() : Constants.EMPTY_STRING);
            }
        } catch (IOException | TimeoutException e) {
            return handleError(e, query, props);
        }
    }
}
