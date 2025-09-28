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

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.function.UnaryOperator;

import io.github.jdbcx.Checker;
import io.github.jdbcx.Constants;
import io.github.jdbcx.ErrorHandler;
import io.github.jdbcx.Interpreter;
import io.github.jdbcx.Option;
import io.github.jdbcx.QueryContext;
import io.github.jdbcx.Result;
import io.github.jdbcx.Row;
import io.github.jdbcx.VariableTag;

abstract class AbstractInterpreter implements Interpreter {
    private final QueryContext context;
    private final VariableTag tag;

    protected AbstractInterpreter(QueryContext context) {
        this.context = context;

        if (context != null) {
            this.tag = context.getVariableTag();
        } else {
            this.tag = VariableTag.BRACE;
        }
    }

    protected final Result<InputStream> handleError(Throwable error, String query, String charset, Properties props,
            AutoCloseable... resources) {
        return Result.of(new ErrorHandler(query, props).attach(resources).handle(error, charset), null);
    }

    protected final Result<String> handleError(Throwable error, String query, Properties props,
            AutoCloseable... resources) {
        return Result.of(new ErrorHandler(query, props).attach(resources).handle(error));
    }

    protected Result<?> process(String query, InputStream result, Properties props, boolean binary) {
        return binary ? Result.of(result, Constants.EMPTY_BYTE_ARRAY)
                : process(query, new InputStreamReader(result, Constants.DEFAULT_CHARSET), props);
    }

    protected Result<?> process(String query, Reader result, Properties props) {
        // TODO check data format first, before splitting it into lines
        final String path = Option.RESULT_JSON_PATH.getValue(props);
        final boolean json = !Checker.isNullOrBlank(path);
        final boolean trim = Boolean.parseBoolean(Option.RESULT_STRING_TRIM.getValue(props));

        // TODO customizable order
        // priority: json -> string(escape, replace, split, trim)
        final List<UnaryOperator<String[]>> ops = new ArrayList<>(5);
        String[] content = new String[0];
        if (json) {
            ops.add(arr -> StringOperations.extract(result, path));
        } else {
            ops.add(arr -> StringOperations.read(result));
        }

        if (Boolean.parseBoolean(Option.RESULT_STRING_ESCAPE.getValue(props))) {
            ops.add(arr -> StringOperations.escape(arr, Option.RESULT_STRING_ESCAPE_TARGET.getValue(props).charAt(0),
                    Option.RESULT_STRING_ESCAPE_CHAR.getValue(props).charAt(0)));
        }
        if (Boolean.parseBoolean(Option.RESULT_STRING_REPLACE.getValue(props))) {
            ops.add(arr -> StringOperations.replace(arr, tag, props, false));
        }
        if (Boolean.parseBoolean(Option.RESULT_STRING_SPLIT.getValue(props))) {
            final String delimiter = Option.RESULT_STRING_SPLIT_CHAR.getValue(props);
            if (!json && !trim && ops.size() == 1) { // split only
                return Result.of(result, delimiter);
            }
            ops.add(arr -> StringOperations.split(arr, delimiter, trim, true));
        } else if (trim) {
            ops.add(arr -> StringOperations.trim(arr, true));
        }

        try {
            // TODO reduce loops
            for (UnaryOperator<String[]> o : ops) {
                content = o.apply(content);
            }

            final Result<?> res;
            int len = content.length;
            if (len == 0) {
                res = Result.of(Constants.EMPTY_STRING);
            } else if (len == 1) {
                res = Result.of(content[0]);
            } else {
                List<Row> rows = new ArrayList<>(len);
                for (int i = 0; i < len; i++) {
                    rows.add(Row.of(content[i]));
                }
                res = Result.of(null, rows);
            }
            return res;
        } catch (UncheckedIOException e) {
            return handleError(e.getCause(), query, props, result);
        }
    }

    @Override
    public QueryContext getContext() {
        return context;
    }

    @Override
    public VariableTag getVariableTag() {
        return tag;
    }
}
