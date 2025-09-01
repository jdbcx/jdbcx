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
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import io.github.jdbcx.ConfigManager;
import io.github.jdbcx.DriverExtension;
import io.github.jdbcx.JdbcActivityListener;
import io.github.jdbcx.Option;
import io.github.jdbcx.QueryContext;
import io.github.jdbcx.Utils;
import io.github.jdbcx.executor.jdbc.CombinedResultSet;
import io.github.jdbcx.executor.jdbc.SqlExceptionUtils;
import io.github.jdbcx.interpreter.JdbcInterpreter;

public class DbDriverExtension implements DriverExtension {
    static final class ActivityListener extends AbstractActivityListener {
        ActivityListener(QueryContext context, Properties config, ClassLoader loader) {
            super(new JdbcInterpreter(context, config, loader), config);
        }
    }

    private final ClassLoader loader;

    public DbDriverExtension() {
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        this.loader = contextClassLoader != null ? contextClassLoader : getClass().getClassLoader();
    }

    @Override
    public List<String> getAliases() {
        return Collections.singletonList("sql");
    }

    @Override
    public Connection getConnection(ConfigManager manager, String url, Properties props) throws SQLException {
        final String category = getName();
        return manager.hasConfig(category, url)
                ? JdbcInterpreter.getConnectionByConfig(manager.getConfig(category, url), loader)
                : JdbcInterpreter.getConnectionByUrl(url, props, loader);
    }

    @Override
    public ResultSet getTables(ConfigManager manager, String schemaPattern, String tableNamePattern, String[] types,
            Properties props) throws SQLException {
        List<String> ids = getSchemas(manager, schemaPattern, props);
        if (ids == null || ids.isEmpty()) {
            return null;
        }

        final String category = getName();
        int len = ids.size();
        ResultSet[] arr = new ResultSet[len];
        SQLWarning w = null;
        for (int i = 0; i < len; i++) {
            String id = ids.get(i);
            try {
                Properties newProps = new Properties(props);
                newProps.putAll(manager.getConfig(category, id));
                Connection conn = JdbcInterpreter.getConnectionByConfig(newProps, loader);
                String catalog = Utils.getCatalogName(conn);
                String schema = Utils.getSchemaName(conn);

                DatabaseMetaData metaData = conn.getMetaData();
                arr[i] = metaData.getTables(catalog, schema, tableNamePattern, types);
            } catch (Exception e) {
                w = SqlExceptionUtils.consolidate(w, e instanceof SQLWarning ? (SQLWarning) e : new SQLWarning(e));
            }
        }
        return new CombinedResultSet(arr, w);
    }

    @Override
    public List<Option> getDefaultOptions() {
        return JdbcInterpreter.OPTIONS;
    }

    @Override
    public JdbcActivityListener createListener(QueryContext context, Connection conn, Properties props) {
        return new ActivityListener(context, getConfig((ConfigManager) context.get(QueryContext.KEY_CONFIG), props),
                loader);
    }

    @Override
    public String getDescription() {
        return "Extension for JDBC connections. Please make sure you've put required drivers in classpath. "
                + "It is recommended to define connections in ~/.jdbcx/connections folder for ease of use and security reason.";
    }

    @Override
    public String getUsage() {
        return "{{ sql(id=my-db1-in-dc1): select 1 }}";
    }
}