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

import java.sql.Connection;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.function.Supplier;

import io.github.jdbcx.Option;
import io.github.jdbcx.QueryContext;
import io.github.jdbcx.Result;
import io.github.jdbcx.executor.QueryExecutor;

public class QueryInterpreter extends AbstractInterpreter {
    public static final List<Option> OPTIONS = Collections
            .unmodifiableList(Arrays.asList(QueryExecutor.OPTION_PATH, Option.INPUT_FILE));

    private final QueryExecutor executor;

    public QueryInterpreter(QueryContext context, Properties config) {
        super(context);

        executor = new QueryExecutor(getVariableTag(), config);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Result<?> interpret(String query, Properties props) {
        try {
            return executor.execute(query, (Supplier<Connection>) getContext().get(QueryContext.KEY_MANAGED_CONNECTION),
                    props);
        } catch (Exception e) {
            return handleError(e, query, props);
        }
    }
}
