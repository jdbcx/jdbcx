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
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import io.github.jdbcx.Checker;
import io.github.jdbcx.DriverExtension;
import io.github.jdbcx.JdbcActivityListener;
import io.github.jdbcx.Option;
import io.github.jdbcx.QueryContext;
import io.github.jdbcx.executor.jdbc.CombinedResultSet;
import io.github.jdbcx.interpreter.JdbcConnectionManager;
import io.github.jdbcx.interpreter.JdbcInterpreter;

public class SqlDriverExtension implements DriverExtension {
    static final class ActivityListener extends AbstractActivityListener {
        ActivityListener(QueryContext context, Properties config, ClassLoader loader) {
            super(new JdbcInterpreter(context, config, loader), config);
        }
    }

    private final ClassLoader loader;

    public SqlDriverExtension() {
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        this.loader = contextClassLoader != null ? contextClassLoader : getClass().getClassLoader();
    }

    @Override
    public List<String> getAliases() {
        return Collections.singletonList("jdbc");
    }

    @Override
    public Connection getConnection(String url, Properties props) throws SQLException {
        if (Checker.isNullOrBlank(url)) {
            url = JdbcConnectionManager.OPTION_ID.getValue(props);
        }
        return Checker.isNullOrBlank(url) ? null : JdbcConnectionManager.getInstance().getConnection(url);
    }

    @Override
    public List<String> getSchemas(String pattern, Properties props) {
        return DriverExtension.getMatched(JdbcConnectionManager.getInstance().getAllConnectionIds(), pattern);
    }

    @Override
    public ResultSet getTables(String schemaPattern, String tableNamePattern, String[] types, Properties props)
            throws SQLException {
        List<String> ids = getSchemas(schemaPattern, props);
        if (ids.isEmpty()) {
            return null;
        }

        int len = ids.size();
        ResultSet[] arr = new ResultSet[len];
        JdbcConnectionManager manager = JdbcConnectionManager.getInstance();
        for (int i = 0; i < len; i++) {
            String id = ids.get(i);
            try {
                Connection conn = manager.getConnection(id);
                String catalog = null;
                String schema = null;
                try { // NOSONAR
                    catalog = conn.getCatalog();
                } catch (Exception e) {
                    // ignore
                }
                try { // NOSONAR
                    schema = conn.getSchema();
                } catch (Exception e) {
                    // ignore
                }

                DatabaseMetaData metaData = manager.getConnection(id).getMetaData();
                arr[i] = metaData.getTables(catalog, schema, tableNamePattern, types);
            } catch (Exception e) {
                // ignore
            }
        }
        return new CombinedResultSet(arr);
    }

    @Override
    public List<Option> getDefaultOptions() {
        return JdbcInterpreter.OPTIONS;
    }

    @Override
    public JdbcActivityListener createListener(QueryContext context, Connection conn, Properties props) {
        return new ActivityListener(context, getConfig(props), loader);
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