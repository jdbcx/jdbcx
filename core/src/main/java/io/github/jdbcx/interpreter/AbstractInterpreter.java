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
import java.io.StringReader;
import java.nio.charset.Charset;
import java.util.Properties;

import io.github.jdbcx.Checker;
import io.github.jdbcx.Constants;
import io.github.jdbcx.ErrorHandler;
import io.github.jdbcx.Interpreter;
import io.github.jdbcx.Option;
import io.github.jdbcx.QueryContext;
import io.github.jdbcx.Result;
import io.github.jdbcx.executor.Stream;

abstract class AbstractInterpreter implements Interpreter {
    private final QueryContext context;

    protected AbstractInterpreter(QueryContext context) {
        this.context = context;
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
        final String delimiter = Boolean.parseBoolean(Option.RESULT_STRING_SPLIT.getValue(props))
                ? Option.RESULT_STRING_SPLIT_CHAR.getValue(props)
                : Constants.EMPTY_STRING;
        try {
            String text = Stream.readAllAsString(result);
            boolean trim = Boolean.parseBoolean(Option.RESULT_STRING_TRIM.getValue(props));
            if (!Checker.isNullOrBlank(path)) {
                String extracted = JsonHelper.extract(text, path);
                if (trim) {
                    extracted = extracted.trim();
                }
                return Checker.isNullOrEmpty(delimiter) ? Result.of(extracted)
                        : Result.of(new StringReader(extracted), delimiter);
            } else if (Checker.isNullOrEmpty(delimiter)) {
                return Result.of(trim ? text.trim() : text);
            }
        } catch (IOException e) {
            return handleError(e, query, props, result);
        }

        return Result.of(result, delimiter.getBytes(Charset.forName(Option.OUTPUT_CHARSET.getValue(props))));
    }

    protected Result<?> process(String query, Reader result, Properties props) {
        final String path = Option.RESULT_JSON_PATH.getValue(props);
        final String delimiter = Boolean.parseBoolean(Option.RESULT_STRING_SPLIT.getValue(props))
                ? Option.RESULT_STRING_SPLIT_CHAR.getValue(props)
                : Constants.EMPTY_STRING;
        try {
            String text = Stream.readAllAsString(result);
            boolean trim = Boolean.parseBoolean(Option.RESULT_STRING_TRIM.getValue(props));
            if (Checker.isNullOrBlank(path)) {
                String extracted = JsonHelper.extract(text, path);
                if (trim) {
                    extracted = extracted.trim();
                }
                return Checker.isNullOrEmpty(delimiter) ? Result.of(extracted)
                        : Result.of(new StringReader(extracted), delimiter);
            } else if (Checker.isNullOrEmpty(delimiter)) {
                return Result.of(trim ? text.trim() : text);
            }
        } catch (IOException e) {
            return handleError(e, query, props, result);
        }

        return Result.of(result, delimiter);
    }

    @Override
    public QueryContext getContext() {
        return context;
    }
}
