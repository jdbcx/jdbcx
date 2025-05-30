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
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.function.Supplier;

import io.github.jdbcx.Checker;
import io.github.jdbcx.ConfigManager;
import io.github.jdbcx.Constants;
import io.github.jdbcx.ExpandedUrlClassLoader;
import io.github.jdbcx.Option;
import io.github.jdbcx.QueryContext;
import io.github.jdbcx.Result;
import io.github.jdbcx.Utils;
import io.github.jdbcx.VariableTag;
import io.github.jdbcx.executor.JdbcExecutor;
import io.github.jdbcx.executor.jdbc.SqlExceptionUtils;

public class JdbcInterpreter extends AbstractInterpreter {
    private static final String JSON_ARRAY_PREFIX = ":[";
    private static final String JSON_PROP_NAME = "\"name\":";
    private static final String JSON_PROP_PRODUCT = "\"product\":";
    private static final String JSON_PROP_TABLES = "\"tables\":";

    public static final String DEFAULT_TABLE_PATTERN = "%";
    public static final String DEFAULT_TABLE_TYPES = "TABLE,VIEW";

    public static final Option OPTION_CONFIG_PATH = Option
            .of(new String[] { "config.path", "Path to configuration file" });
    public static final Option OPTION_PROPERTIES = Option
            .of(new String[] { "properties", "Comma separated connection properties" });

    public static final Option OPTION_URL = Option.of(new String[] { "url", "JDBC connection URL" });
    public static final Option OPTION_DIALECT = Option.of(new String[] { "dialect", "Dialect class name" });
    public static final Option OPTION_DRIVER = Option.of(new String[] { "driver", "JDBC driver class name" });
    public static final List<Option> OPTIONS = Collections
            .unmodifiableList(Arrays.asList(Option.EXEC_ERROR, Option.INPUT_FILE, JdbcExecutor.OPTION_RESULT,
                    ConfigManager.OPTION_MANAGED, Option.EXEC_TIMEOUT));

    private final String defaultConnectionUrl;
    private final String defaultConnectionProps;

    private final ClassLoader loader;
    private final JdbcExecutor executor;

    static final Driver getDriverByClass(String driver, ClassLoader loader) throws SQLException {
        if (loader == null) {
            loader = JdbcInterpreter.class.getClassLoader();
        }

        try {
            return (Driver) loader.loadClass(driver).getConstructor().newInstance();
        } catch (Exception e) {
            throw SqlExceptionUtils
                    .clientError(Utils.format("Failed to load driver [%s] due to: %s", driver, e.getMessage()), e);
        }
    }

    static final String getDatabaseTables(DatabaseMetaData metaData, String catalog, String schema, String pattern,
            String[] types) throws SQLException {
        StringBuilder builder = new StringBuilder(JSON_PROP_TABLES).append('[');
        boolean first = true;
        try (ResultSet rs = metaData.getTables(catalog, schema, pattern, types)) {
            while (rs.next()) {
                if (first) {
                    first = false;
                } else {
                    builder.append(',');
                }
                builder.append(JsonHelper.encode(rs.getString(3)));
            }
        } catch (SQLFeatureNotSupportedException | UnsupportedOperationException e) {
            // ignore
        }
        return builder.append(']').toString();
    }

    static final String getDatabaseSchemas(DatabaseMetaData metaData, String catalog, String tablePattern,
            String[] tableTypes) throws SQLException {
        String term = metaData.getSchemaTerm();
        if (Checker.isNullOrEmpty(term)) {
            return getDatabaseTables(metaData, catalog, null, tablePattern, tableTypes);
        } else {
            term = new StringBuilder(term.toLowerCase(Locale.ROOT)).append('s').toString();
        }

        StringBuilder builder = new StringBuilder();
        boolean first = true;
        try (ResultSet rs = metaData.getSchemas(catalog, DEFAULT_TABLE_PATTERN)) {
            while (rs.next()) {
                if (first) {
                    builder.append(JsonHelper.encode(term));
                    builder.append(JSON_ARRAY_PREFIX);
                    first = false;
                } else {
                    builder.append(',');
                }
                String schema = rs.getString(1);
                builder.append('{');
                builder.append(JSON_PROP_NAME);
                builder.append(JsonHelper.encode(schema));
                builder.append(',');
                builder.append(getDatabaseTables(metaData, catalog, schema, tablePattern, tableTypes));
                builder.append('}');
            }
        } catch (SQLFeatureNotSupportedException | UnsupportedOperationException e) {
            // ignore
        }
        return first ? getDatabaseTables(metaData, catalog, null, tablePattern, tableTypes)
                : builder.append(']').toString();
    }

    static final String getDatabaseCatalogs(DatabaseMetaData metaData, String tablePattern, String[] tableTypes)
            throws SQLException {
        String term = metaData.getCatalogTerm();
        if (Checker.isNullOrEmpty(term)) {
            return getDatabaseSchemas(metaData, null, tablePattern, tableTypes);
        } else {
            term = new StringBuilder(term.toLowerCase(Locale.ROOT)).append('s').toString();
        }

        StringBuilder builder = new StringBuilder();
        boolean first = true;
        try (ResultSet rs = metaData.getCatalogs()) {
            while (rs.next()) {
                if (first) {
                    builder.append(JsonHelper.encode(term));
                    builder.append(JSON_ARRAY_PREFIX);
                    first = false;
                } else {
                    builder.append(',');
                }
                String catalog = rs.getString(1);
                builder.append('{');
                builder.append(JSON_PROP_NAME);
                builder.append(JsonHelper.encode(catalog));
                builder.append(',');
                builder.append(getDatabaseSchemas(metaData, catalog, tablePattern, tableTypes));
                builder.append('}');
            }
        } catch (SQLFeatureNotSupportedException | UnsupportedOperationException e) {
            // ignore
        }

        return first ? getDatabaseSchemas(metaData, null, tablePattern, tableTypes) : builder.append(']').toString();
    }

    public static final String getDatabaseCatalogs(DatabaseMetaData metaData, String id, String tablePattern, // NOSONAR
            String tableTypes) throws SQLException {
        if (Checker.isNullOrEmpty(tablePattern)) {
            tablePattern = DEFAULT_TABLE_PATTERN;
        }
        final String[] types;
        List<String> list = Utils.split(tableTypes, ',', true, true, true);
        if (list.isEmpty()) {
            types = null;
        } else {
            types = list.toArray(Constants.EMPTY_STRING_ARRAY);
        }
        return getDatabaseCatalogs(metaData, tablePattern, types);
    }

    public static final String getDatabaseProduct(DatabaseMetaData metaData) throws SQLException {
        String str = metaData.getDatabaseProductName();
        if (Checker.isNullOrEmpty(str)) {
            return Constants.EMPTY_STRING;
        }
        String ver = metaData.getDatabaseProductVersion();
        if (!Checker.isNullOrEmpty(ver)) {
            str = new StringBuilder(str).append(' ').append(ver).toString();
        }
        return new StringBuilder(JSON_PROP_PRODUCT).append(JsonHelper.encode(str)).toString();
    }

    public static final Driver getDriverByUrl(String url, ClassLoader classLoader) throws SQLException {
        if (classLoader == null) {
            classLoader = JdbcInterpreter.class.getClassLoader();
        }

        Driver d = null;
        for (Iterator<Driver> it = Utils.load(Driver.class, classLoader).iterator(); it.hasNext();) {
            Driver newDriver = null;
            try {
                newDriver = it.next();
            } catch (Throwable t) { // NOSONAR
                // ignore
            }
            if (newDriver != null && newDriver.acceptsURL(url)) {
                d = newDriver;
                break;
            }
        }

        return d != null ? d : DriverManager.getDriver(url);
    }

    public static final Connection getConnectionByUrl(String url, Properties props, ClassLoader classLoader)
            throws SQLException {
        if (Checker.isNullOrBlank(url)) {
            url = OPTION_URL.getValue(props);
        }
        if (Checker.isNullOrBlank(url)) {
            throw SqlExceptionUtils.clientError("No connection URL specified");
        }

        final Properties filtered = new Properties();
        for (Entry<Object, Object> entry : props.entrySet()) {
            String key = (String) entry.getKey();
            if (!key.startsWith(Option.PROPERTY_PREFIX)) {
                filtered.put(key, entry.getValue());
            }
        }
        filtered.remove(OPTION_URL.getName());

        final String classpath = (String) filtered.remove(Option.CLASSPATH.getName());
        if (!Checker.isNullOrBlank(classpath)) {
            classLoader = ExpandedUrlClassLoader.of(JdbcInterpreter.class, classpath.split(","));
        } else if (classLoader == null) {
            classLoader = JdbcInterpreter.class.getClassLoader();
        }

        final String driver = (String) filtered.remove(OPTION_DRIVER.getName());
        try {
            return (Checker.isNullOrBlank(driver) ? getDriverByUrl(url, classLoader)
                    : getDriverByClass(driver, classLoader)).connect(url, filtered);
        } catch (SQLException e) {
            SQLException ex = SqlExceptionUtils
                    .clientError(Utils.format("Failed to connect to [%s] due to: %s", url, e.getMessage()));
            ex.setNextException(e);
            throw ex;
        }
    }

    public static final Connection getConnectionByConfig(Properties config, ClassLoader classLoader)
            throws SQLException {
        final String classpath = Option.CLASSPATH.getJdbcxValue(config);
        final String url = OPTION_URL.getJdbcxValue(config);
        final String driver = OPTION_DRIVER.getJdbcxValue(config);
        final String properties = OPTION_PROPERTIES.getJdbcxValue(config);

        if (Checker.isNullOrEmpty(url)) {
            throw SqlExceptionUtils.clientError("No connection URL defined");
        }

        final Properties filtered = new Properties();
        Set<String> reservedKeys = new HashSet<>();
        for (Option o : OPTIONS) {
            reservedKeys.add(o.getName());
        }
        for (Entry<Object, Object> entry : config.entrySet()) {
            String key = (String) entry.getKey();
            if (!key.startsWith(Option.PROPERTY_PREFIX) && !reservedKeys.contains(key)) {
                filtered.put(key, entry.getValue());
            }
        }
        if (!Checker.isNullOrEmpty(properties)) {
            filtered.putAll(Utils.toKeyValuePairs(properties));
        }

        if (!Checker.isNullOrBlank(classpath)) {
            classLoader = ExpandedUrlClassLoader.of(JdbcInterpreter.class, classpath.split(","));
        } else if (classLoader == null) {
            classLoader = JdbcInterpreter.class.getClassLoader();
        }

        if (Checker.isNullOrEmpty(driver)) {
            Driver d = getDriverByUrl(url, classLoader);
            if (d != null) {
                return d.connect(url, filtered);
            }
            throw SqlExceptionUtils.clientError(
                    Utils.format("No suitable driver found in classpath [%s]. Please specify \"%s\" and try again.",
                            classpath, OPTION_DRIVER.getJdbcxName()));
        } else {
            Driver d = null;
            try {
                d = (Driver) classLoader.loadClass(driver).getConstructor().newInstance();
                return d.connect(url, filtered);
            } catch (Exception e) {
                throw SqlExceptionUtils.clientError(
                        d == null ? Utils.format("Failed to load driver [%s] due to: %s", driver, e.getMessage())
                                : Utils.format("Failed to connect to [%s] due to: %s", url, e.getMessage()),
                        e);
            }
        }
    }

    @SuppressWarnings("unchecked")
    protected Connection getConnection(Properties props) throws SQLException {
        String str = OPTION_URL.getJdbcxValue(props);
        if (!Checker.isNullOrBlank(str)) {
            return getConnectionByConfig(props, loader);
        }

        VariableTag tag = getVariableTag();
        str = Utils.applyVariables(OPTION_URL.getValue(props, defaultConnectionUrl), tag, props);
        if (Checker.isNullOrBlank(str)) {
            // log.debug("Reuse current connection since ")
            Supplier<Connection> supplier = (Supplier<Connection>) getContext().get(QueryContext.KEY_CONNECTION);
            if (supplier != null) {
                return supplier.get();
            }
        } else {
            Map<String, String> map = Utils.toKeyValuePairs(
                    Utils.applyVariables(OPTION_PROPERTIES.getValue(props, defaultConnectionProps), tag, props));
            Properties newProps = new Properties(); // new Properties(props);
            String classpath = Option.CLASSPATH.getValue(props);
            if (!Checker.isNullOrBlank(classpath)) {
                Option.CLASSPATH.setJdbcxValue(newProps, classpath);
            }
            newProps.putAll(map);
            return getConnectionByUrl(str, newProps, loader);
        }

        throw new SQLException(Utils.format("Please specify either \"%s\" or \"%s\" to establish connection",
                Option.ID.getName(), OPTION_URL.getName()));
    }

    public JdbcInterpreter(QueryContext context, Properties config, ClassLoader loader) {
        super(context);

        this.defaultConnectionUrl = OPTION_URL.getValue(config);
        this.defaultConnectionProps = OPTION_PROPERTIES.getValue(config);

        if (Boolean.parseBoolean(ConfigManager.OPTION_MANAGED.getValue(config))) {
            ConfigManager manager = (ConfigManager) context.get(QueryContext.KEY_CONFIG);
            if (manager != null) {
                manager.reload(config);
            }
        }

        this.loader = loader;
        this.executor = new JdbcExecutor(getVariableTag(), config);
    }

    @Override
    public Result<?> interpret(String query, Properties props) {
        Connection conn = null;
        try {
            conn = getConnection(props);
            final Object result = executor.execute(query, conn, props);
            if (result instanceof ResultSet) { // NOSONAR
                return Result.of((ResultSet) result);
            }

            conn.close();
            return Result.of((Long) result);
        } catch (SQLException e) {
            return handleError(e, query, props, conn);
        }
    }
}
