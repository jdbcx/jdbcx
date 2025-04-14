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
package io.github.jdbcx.extension;

import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.CompletionException;

import io.github.jdbcx.Checker;
import io.github.jdbcx.Constants;
import io.github.jdbcx.Field;
import io.github.jdbcx.JdbcActivityListener;
import io.github.jdbcx.Interpreter;
import io.github.jdbcx.Option;
import io.github.jdbcx.QueryContext;
import io.github.jdbcx.Result;
import io.github.jdbcx.Row;
import io.github.jdbcx.executor.jdbc.SqlExceptionUtils;

abstract class AbstractActivityListener implements JdbcActivityListener {
    protected final Properties config;
    protected final Interpreter interpreter;

    private final String defaultQuery;

    protected AbstractActivityListener(Interpreter interpreter, Properties config) {
        this.interpreter = interpreter;
        this.config = config;

        this.defaultQuery = config.getProperty(Constants.EMPTY_STRING, Constants.EMPTY_STRING).trim();
    }

    protected SQLException onException(Exception e) { // NOSONAR
        return null;
    }

    protected SQLException onCompletionException(CompletionException e) {
        SQLException exp = onException(e);
        if (exp == null) {
            exp = e.getCause() == null ? SqlExceptionUtils.clientWarning(e)
                    : SqlExceptionUtils.clientError(e.getCause());
        }
        return exp;
    }

    protected SQLException onUnexpectedException(Exception e) {
        SQLException exp = onException(e);
        if (exp == null) {
            exp = SqlExceptionUtils.clientError(e);
        }
        return exp;
    }

    @Override
    public Result<?> onQuery(String query) throws SQLException {
        final String resultVar = Option.RESULT_VAR.getValue(config);
        final boolean saveResult = !Checker.isNullOrEmpty(resultVar);
        final String scope = saveResult ? Option.RESULT_SCOPE.getValue(config) : Constants.SCOPE_QUERY;
        try {
            final QueryContext context = interpreter.getContext();

            if (saveResult && Boolean.parseBoolean(Option.RESULT_REUSE.getValue(config))) {
                String value = context.getVariableInScope(scope, resultVar);
                if (value != null) {
                    return Result.of(value);
                }
            }

            final Result<?> result = interpreter.interpret(Checker.isNullOrEmpty(query) ? defaultQuery : query, config);
            if (saveResult) {
                final int len = result.getFieldCount();
                final String value;
                if (len <= 0) {
                    value = Constants.EMPTY_STRING;
                } else if (len == 1) {
                    StringBuilder builder = new StringBuilder();
                    for (Row r : result.rows()) {
                        builder.append(r.value(0).asString()).append(',');
                    }
                    int l = builder.length() - 1;
                    if (l >= 0) {
                        builder.setLength(l);
                    }
                    value = builder.toString();
                } else {
                    String[] vars = new String[len];
                    StringBuilder[] vals = new StringBuilder[len];
                    int i = 0;
                    for (Field f : result.fields()) {
                        vars[i] = new StringBuilder(resultVar).append('.').append(f.name()).toString();
                        vals[i++] = new StringBuilder();
                    }

                    for (Row r : result.rows()) {
                        for (i = 0; i < len; i++) {
                            vals[i].append(r.value(i).asString()).append(',');
                        }
                    }

                    final boolean trim = vals[0].length() > 0;
                    for (i = 0; i < len; i++) {
                        StringBuilder builder = vals[i];
                        if (trim) {
                            builder.setLength(builder.length() - 1);
                        }
                        context.setVariableInScope(scope, vars[i], builder.toString());
                    }
                    value = Constants.EMPTY_STRING;
                }
                context.setVariableInScope(scope, resultVar, value);
                return Result.of(value);
            }
            return result;
        } catch (CompletionException e) {
            throw onCompletionException(e);
        } catch (Exception e) {
            throw onUnexpectedException(e);
        }
    }
}
