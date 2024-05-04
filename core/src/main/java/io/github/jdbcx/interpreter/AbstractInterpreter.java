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
package io.github.jdbcx.interpreter;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Properties;

import io.github.jdbcx.Checker;
import io.github.jdbcx.Constants;
import io.github.jdbcx.ErrorHandler;
import io.github.jdbcx.Interpreter;
import io.github.jdbcx.Option;
import io.github.jdbcx.QueryContext;
import io.github.jdbcx.Result;
import io.github.jdbcx.Row;
import io.github.jdbcx.VariableTag;
import io.github.jdbcx.executor.Stream;

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

    protected Result<?> process(String query, InputStream result, Properties props) {
        // TODO check data format first, before splitting it into lines
        final String path = Option.RESULT_JSON_PATH.getValue(props);
        final boolean json = !Checker.isNullOrBlank(path);
        final boolean trim = Boolean.parseBoolean(Option.RESULT_STRING_TRIM.getValue(props));
        final String delimiter = Boolean.parseBoolean(Option.RESULT_STRING_SPLIT.getValue(props))
                ? Option.RESULT_STRING_SPLIT_CHAR.getValue(props)
                : Constants.EMPTY_STRING;
        if (!json && !trim) {
            return Result.of(result, delimiter.getBytes(Charset.forName(Option.OUTPUT_CHARSET.getValue(props))));
        }

        try {
            final Result<?> res;
            if (json) {
                List<Row> rows = JsonHelper.extract(result, Charset.forName(Option.OUTPUT_CHARSET.getValue(props)),
                        path, delimiter, trim);
                res = rows.size() == 1 ? Result.of(rows.get(0)) : Result.of(null, rows);
            } else if (Checker.isNullOrEmpty(delimiter)) {
                res = trim ? Result.of(Stream.readAllAsString(result).trim())
                        : Result.of(null, result, Constants.EMPTY_BYTE_ARRAY);
            } else {
                res = Result.of(result, delimiter.getBytes(Charset.forName(Option.OUTPUT_CHARSET.getValue(props))));
            }
            return res;
        } catch (IOException e) {
            return handleError(e, query, props, result);
        }
    }

    protected Result<?> process(String query, Reader result, Properties props) {
        final String path = Option.RESULT_JSON_PATH.getValue(props);
        final boolean json = !Checker.isNullOrBlank(path);
        final boolean trim = Boolean.parseBoolean(Option.RESULT_STRING_TRIM.getValue(props));
        final String delimiter = Boolean.parseBoolean(Option.RESULT_STRING_SPLIT.getValue(props))
                ? Option.RESULT_STRING_SPLIT_CHAR.getValue(props)
                : Constants.EMPTY_STRING;
        if (!json && !trim) {
            return Result.of(result, delimiter);
        }

        try {
            final Result<?> res;
            if (json) {
                List<Row> rows = JsonHelper.extract(result, path, delimiter, trim);
                res = rows.size() == 1 ? Result.of(rows.get(0)) : Result.of(null, rows);
            } else if (Checker.isNullOrEmpty(delimiter)) {
                res = trim ? Result.of(Stream.readAllAsString(result).trim())
                        : Result.of(result, delimiter);
            } else {
                res = Result.of(result, delimiter);
            }
            return res;
        } catch (IOException e) {
            return handleError(e, query, props, result);
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
