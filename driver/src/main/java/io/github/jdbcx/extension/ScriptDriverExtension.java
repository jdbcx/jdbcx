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

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import io.github.jdbcx.Checker;
import io.github.jdbcx.DriverExtension;
import io.github.jdbcx.JdbcActivityListener;
import io.github.jdbcx.Option;
import io.github.jdbcx.QueryContext;
import io.github.jdbcx.executor.ScriptExecutor;
import io.github.jdbcx.interpreter.ScriptInterpreter;

public class ScriptDriverExtension implements DriverExtension {
    static final class ActivityListener extends AbstractActivityListener {
        static final Option OPTION_VAR_CONNECT = Option
                .of(new String[] { "var.connection", "Variable name of current java.sql.Connection instance", "conn" });

        static QueryContext update(QueryContext context, Connection conn, Properties config) {
            String varConn = OPTION_VAR_CONNECT.getValue(config);

            if (!Checker.isNullOrEmpty(varConn)) {
                Map<String, Object> vars = new HashMap<>();
                vars.put(varConn, conn);
                context.put(ScriptInterpreter.KEY_VARS, vars);
            }
            return context;
        }

        ActivityListener(QueryContext context, Connection conn, Properties config, ClassLoader loader) {
            super(new ScriptInterpreter(update(context, conn, config), config, loader), config);
        }
    }

    private static final List<Option> options;

    static {
        List<Option> list = new ArrayList<>(ScriptInterpreter.OPTIONS);
        list.add(ActivityListener.OPTION_VAR_CONNECT);
        options = Collections.unmodifiableList(list);
    }

    private final ClassLoader loader;

    public ScriptDriverExtension() {
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        this.loader = contextClassLoader != null ? contextClassLoader : getClass().getClassLoader();
    }

    @Override
    public List<String> getAliases() {
        return Collections.unmodifiableList(
                Arrays.asList(new ScriptExecutor("", null, false, null, loader).getSupportedLanguages()));
    }

    @Override
    public List<Option> getDefaultOptions() {
        return options;
    }

    @Override
    public JdbcActivityListener createListener(QueryContext context, Connection conn, Properties props) {
        return new ActivityListener(context, conn, getConfig(props), loader);
    }
}
