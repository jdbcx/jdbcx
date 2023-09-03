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

import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;

import io.github.jdbcx.Checker;
import io.github.jdbcx.Constants;
import io.github.jdbcx.Option;
import io.github.jdbcx.QueryContext;
import io.github.jdbcx.Result;
import io.github.jdbcx.Utils;

public class VarInterpreter extends AbstractInterpreter {
    static final Option OPTION_PREFIX = Option.of(new String[] { "prefix", "Prefix of variable", "" });

    private final String defaultPrefix;

    public VarInterpreter(QueryContext context, Properties config) {
        super(context);

        defaultPrefix = OPTION_PREFIX.getValue(config);
    }

    @Override
    public Result<String> interpret(String query, Properties props) {
        try {
            if (!Checker.isNullOrBlank(query)) {
                final QueryContext context = getContext();
                final String prefix = OPTION_PREFIX.getValue(props, defaultPrefix);
                if (Checker.isNullOrEmpty(prefix)) {
                    for (Entry<String, String> entry : Utils.toKeyValuePairs(query).entrySet()) {
                        context.setVariable(entry.getKey(), entry.getValue());
                    }
                } else {
                    Map<String, String> map = Utils.toKeyValuePairs(query);
                    for (Entry<String, String> entry : map.entrySet()) {
                        context.setVariable(prefix.concat(entry.getKey()), entry.getValue());
                    }
                }
            }
            return Result.of(Constants.EMPTY_STRING);
        } catch (Exception e) {
            return handleError(e, query, props);
        }
    }
}
