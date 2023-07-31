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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Supplier;

import io.github.jdbcx.Checker;
import io.github.jdbcx.Option;
import io.github.jdbcx.QueryContext;
import io.github.jdbcx.Result;
import io.github.jdbcx.Utils;
import io.github.jdbcx.executor.JdbcExecutor;

public class JdbcInterpreter extends AbstractInterpreter {
    public static final Option OPTION_CONFIG_PATH = Option
            .of(new String[] { "config.path", "Path to configuration file" });
    public static final Option OPTION_PROPERTIES = Option
            .of(new String[] { "properties", "Comma separated connection properties" });

    public static final List<Option> OPTIONS = Collections
            .unmodifiableList(Arrays.asList(Option.EXEC_ERROR,
                    Option.EXEC_TIMEOUT.update().defaultValue("30000").build(), JdbcConnectionManager.OPTION_ID,
                    JdbcConnectionManager.OPTION_URL, OPTION_PROPERTIES));

    private final String defaultConnectionId;
    private final String defaultConnectionUrl;
    private final String defaultConnectionProps;

    private final JdbcExecutor executor;

    @SuppressWarnings("unchecked")
    protected Connection getConnection(Properties props) throws SQLException {
        final JdbcConnectionManager manager = JdbcConnectionManager.getInstance();
        String str = JdbcConnectionManager.OPTION_ID.getValue(props, defaultConnectionId);
        if (!Checker.isNullOrBlank(str)) {
            return manager.getConnection(str);
        }

        str = Utils.applyVariables(JdbcConnectionManager.OPTION_URL.getValue(props, defaultConnectionUrl), props);
        if (Checker.isNullOrBlank(str)) {
            // log.debug("Reuse current connection since ")
            Supplier<Connection> supplier = (Supplier<Connection>) getContext().get(QueryContext.KEY_CONNECTION);
            if (supplier != null) {
                return supplier.get();
            }
        } else {
            Map<String, String> map = Utils.toKeyValuePairs(
                    Utils.applyVariables(OPTION_PROPERTIES.getValue(props, defaultConnectionProps), props));
            Properties newProps = new Properties(props);
            newProps.putAll(map);
            return manager.getConnection(str, newProps);
        }

        throw new SQLException(Utils.format("Please specify either \"%s\" or \"%s\" to establish connection",
                JdbcConnectionManager.OPTION_ID.getName(), JdbcConnectionManager.OPTION_URL.getName()));
    }

    public JdbcInterpreter(QueryContext context, Properties config) {
        super(context);

        this.defaultConnectionId = JdbcConnectionManager.OPTION_ID.getValue(config);
        this.defaultConnectionUrl = JdbcConnectionManager.OPTION_URL.getValue(config);
        this.defaultConnectionProps = OPTION_PROPERTIES.getValue(config);

        this.executor = new JdbcExecutor(config);
    }

    @Override
    public Result<?> interpret(String query, Properties props) {
        Connection conn = null;
        try {
            conn = getConnection(props);
            final Object result = executor.execute(query, conn);
            if (result instanceof ResultSet) {
                return Result.of((ResultSet) result);
            }

            conn.close();
            return Result.of((Long) result);
        } catch (SQLException e) {
            return handleError(e, query, props, conn);
        }
    }
}
