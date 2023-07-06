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
package io.github.jdbcx.script;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import io.github.jdbcx.Checker;
import io.github.jdbcx.ConnectionListener;
import io.github.jdbcx.Option;
import io.github.jdbcx.Scripting;
import io.github.jdbcx.SqlExceptionUtils;

final class ScriptConnectionListener implements ConnectionListener {
    static final Option OPTION_VAR_CONNECT = Option
            .of(new String[] { "var.connection", "Variable name of current java.sql.Connection instance", "conn" });
    static final Option OPTION_VAR_HELPER = Option
            .of(new String[] { "var.helper", "Variable name of the helper object", "helper" });

    final Scripting script;
    final List<Option> options;

    ScriptConnectionListener(Connection conn, Properties config) {
        String varConn = OPTION_VAR_CONNECT.getValue(config);
        String varHelper = OPTION_VAR_HELPER.getValue(config);

        Map<String, Object> vars = new HashMap<>();
        if (!Checker.isNullOrEmpty(varConn)) {
            vars.put(varConn, conn);
        }
        if (!Checker.isNullOrEmpty(varHelper)) {
            vars.put(varHelper, ScriptHelper.getInstance());
        }
        script = new Scripting(Scripting.OPTION_LANGUAGE.getValue(config), config, vars);

        List<Option> list = new ArrayList<>();
        list.add(Scripting.OPTION_LANGUAGE.update().choices(script.getSupportedLanguages()).build());
        options = Collections.unmodifiableList(list);
    }

    @Override
    public String onQuery(String query) throws SQLException {
        try {
            Object result = script.execute(query, null);
            if (result instanceof CharSequence) {
                return result.toString();
            } else {
                throw new SQLException("Script must return SQL query for execution, but we got: " + result);
            }
        } catch (Exception e) {
            throw SqlExceptionUtils.clientError(e);
        }
    }
}
