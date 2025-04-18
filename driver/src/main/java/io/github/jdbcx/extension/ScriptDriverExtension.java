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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import io.github.jdbcx.Checker;
import io.github.jdbcx.ConfigManager;
import io.github.jdbcx.DriverExtension;
import io.github.jdbcx.JdbcActivityListener;
import io.github.jdbcx.Option;
import io.github.jdbcx.QueryContext;
import io.github.jdbcx.Result;
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
        return ScriptExecutor.getAllScriptEngines(loader);
    }

    @Override
    public List<String> getSchemas(ConfigManager manager, String pattern, Properties props) {
        return DriverExtension.getMatched(ScriptExecutor.getAllSupportedLanguages(loader), pattern);
    }

    @Override
    public ResultSet getTables(ConfigManager manager, String schemaPattern, String tableNamePattern, String[] types,
            Properties props) throws SQLException {
        List<String> engineNames = getSchemas(manager, schemaPattern, props);
        if (engineNames == null || engineNames.isEmpty()) {
            return null;
        } else if (types != null) {
            boolean hasTable = false;
            for (int i = 0, len = types.length; i < len; i++) {
                if (DEFAULT_JDBC_TABLE_TYPE.equalsIgnoreCase(types[i])) {
                    hasTable = true;
                    break;
                }
            }
            if (!hasTable) {
                return null;
            }
        }

        String extName = getName();

        List<String[]> list = ScriptExecutor.getMatchedScriptEngineInfo(loader, engineNames);
        List<String[]> results = new LinkedList<>();
        for (int i = 0, len = list.size(); i < len; i++) {
            String[] info = list.get(i);
            results.add(new String[] { extName, info[0], info[1], DEFAULT_JDBC_TABLE_TYPE, info[2], null, null, null,
                    null, null });
        }
        return Result.of(JDBC_TABLE_FIELDS, results.toArray(new String[0][])).get(ResultSet.class);
    }

    @Override
    public List<Option> getDefaultOptions() {
        return options;
    }

    @Override
    public JdbcActivityListener createListener(QueryContext context, Connection conn, Properties props) {
        return new ActivityListener(context, conn,
                getConfig((ConfigManager) context.get(QueryContext.KEY_CONFIG), props), loader);
    }

    @Override
    public String getDescription() {
        return "Extension for all supported JVM scripting languages. Please make sure you've put required libraries in classpath.";
    }

    @Override
    public String getUsage() {
        return "{{ script(language=rhino): 1+1 }}";
    }
}
