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
package io.github.jdbcx.extension;

import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.CompletionException;

import io.github.jdbcx.Checker;
import io.github.jdbcx.JdbcActivityListener;
import io.github.jdbcx.Interpreter;
import io.github.jdbcx.Option;
import io.github.jdbcx.Result;
import io.github.jdbcx.executor.jdbc.SqlExceptionUtils;

abstract class AbstractActivityListener implements JdbcActivityListener {
    protected final Properties config;
    protected final Interpreter interpreter;

    protected AbstractActivityListener(Interpreter interpreter, Properties config) {
        this.interpreter = interpreter;
        this.config = config;
    }

    @Override
    public Result<?> onQuery(String query) throws SQLException {
        try {
            final Result<?> result = interpreter.interpret(query, config);

            final String resultId = Option.RESULT_ID.getValue(config);
            if (!Checker.isNullOrEmpty(resultId)) {
                interpreter.getContext().putResult(resultId, result);
            }
            return result;
        } catch (CompletionException e) {
            if (e.getCause() == null) {
                throw SqlExceptionUtils.clientWarning(e);
            } else {
                throw SqlExceptionUtils.clientError(e.getCause());
            }
        } catch (Exception e) { // unexpected error
            throw SqlExceptionUtils.clientError(e);
        }
    }
}
