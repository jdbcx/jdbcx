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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import io.github.jdbcx.Checker;
import io.github.jdbcx.ConfigManager;
import io.github.jdbcx.Constants;
import io.github.jdbcx.ExpandedUrlClassLoader;
import io.github.jdbcx.Logger;
import io.github.jdbcx.LoggerFactory;
import io.github.jdbcx.Option;
import io.github.jdbcx.QueryContext;
import io.github.jdbcx.Result;
import io.github.jdbcx.Utils;
import io.github.jdbcx.VariableTag;
import io.github.jdbcx.executor.JdbcExecutor;
import io.github.jdbcx.executor.jdbc.SqlExceptionUtils;

public class JdbcInterpreter extends AbstractInterpreter {
    private static final Logger log = LoggerFactory.getLogger(JdbcInterpreter.class);

    private static final String JSON_ARRAY_PREFIX = ":[";
    private static final String JSON_EMPTY_ARRAY = "[]";
    private static final String JSON_PROP_COLUMNS = "\"columns\":";
    private static final String JSON_PROP_INDEXES = "\"indexes\":";
    private static final String JSON_PROP_NULLABLE = "\"nullable\":";
    private static final String JSON_PROP_PRIMARY_KEY = "\"primaryKey\":";
    private static final String JSON_PROP_PRODUCT = "\"product\":";
    private static final String JSON_PROP_TABLES = "\"tables\":";

    private static final String PREFIX_CURRENT = "current";
    private static final String TERM_CATALOG = "Catalog";
    private static final String TERM_SCHEMA = "Schema";

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

    final static class OrderedColumn {
        final int position;
        final String column;

        OrderedColumn(int position, String column) {
            this.position = position;
            this.column = column;
        }
    }

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
                String schema = rs.getString(1);
                try {
                    String tables = getDatabaseTables(metaData, catalog, schema, tablePattern, tableTypes);
                    if (tables.endsWith(JSON_EMPTY_ARRAY)) { // ignore schema without any table
                        continue;
                    }
                    if (first) {
                        builder.append(JsonHelper.encode(term));
                        builder.append(JSON_ARRAY_PREFIX);
                        first = false;
                    } else {
                        builder.append(',');
                    }
                    builder.append('{').append(Constants.JSON_PROP_NAME).append(JsonHelper.encode(schema)).append(',')
                            .append(tables).append('}');
                } catch (SQLException e) {
                    log.warn("Failed to get database tables(catalog=%s, schema=%s, pattern=%s) due to %s", catalog,
                            schema, tablePattern, e.getMessage());
                }
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
                String catalog = rs.getString(1);
                try {
                    String schemas = getDatabaseSchemas(metaData, catalog, tablePattern, tableTypes);
                    if (first) {
                        builder.append(JsonHelper.encode(term));
                        builder.append(JSON_ARRAY_PREFIX);
                        first = false;
                    } else {
                        builder.append(',');
                    }
                    builder.append('{').append(Constants.JSON_PROP_NAME).append(JsonHelper.encode(catalog)).append(',')
                            .append(schemas).append('}');
                } catch (SQLException e) {
                    log.debug("Failed to get schemas from catalog [%s] due to %s", catalog, e.getMessage());
                }
            }
        } catch (SQLFeatureNotSupportedException | UnsupportedOperationException e) {
            // ignore
        }

        return first ? getDatabaseSchemas(metaData, null, tablePattern, tableTypes) : builder.append(']').toString();
    }

    static final String normalizedName(String name, String quoteString) {
        if (Checker.isNullOrEmpty(name)) {
            return Constants.EMPTY_STRING;
        } else if (quoteString.isEmpty()) {
            return name;
        }

        final int lastIndex = name.length() - 1;
        for (int i = 0, len = quoteString.length(); i < len; i++) {
            char ch = quoteString.charAt(i);
            if (name.charAt(0) == ch && name.charAt(lastIndex) == ch) {
                return name.substring(1, lastIndex);
            }
        }
        return name;
    }

    static final List<String> getFullQualifiedTable(Connection conn, String table) throws SQLException {
        if (Checker.isNullOrEmpty(table)) {
            return Collections.emptyList();
        }

        DatabaseMetaData metaData = conn.getMetaData();
        String quoteString = metaData.getIdentifierQuoteString();
        quoteString = Checker.isNullOrEmpty(quoteString) ? Constants.EMPTY_STRING : quoteString.trim();
        List<String> list = new ArrayList<>(3);
        StringBuilder builder = new StringBuilder();
        for (int i = 0, len = table.length(); i < len; i++) {
            char ch = table.charAt(i);
            if (builder.length() == 0) {
                if (Character.isWhitespace(ch) || ch == '.') {
                    continue;
                } else if (quoteString.indexOf(ch) != -1) {
                    for (int j = i + 1; j < len; j++) {
                        char nextCh = table.charAt(j);
                        if (nextCh == '\\') {
                            if (++j < len) {
                                builder.append(table.charAt(j));
                            } else {
                                throw new IllegalArgumentException("Missing character after '\\'");
                            }
                        } else if (nextCh == ch) {
                            if (j + 1 < len && table.charAt(j + 1) == nextCh) {
                                builder.append(nextCh);
                            } else {
                                i = j;
                                break;
                            }
                        } else {
                            builder.append(nextCh);
                        }
                    }
                    list.add(builder.toString());
                    builder.setLength(0);
                } else if (ch == '\\') {
                    if (++i < len) {
                        builder.append(table.charAt(i));
                    } else {
                        throw new IllegalArgumentException("Missing character after '\\'");
                    }
                } else {
                    builder.append(ch);
                }
            } else if (ch == '\\') {
                if (++i < len) {
                    builder.append(table.charAt(i));
                } else {
                    throw new IllegalArgumentException("Missing character after '\\'");
                }
            } else if (ch == '.') {
                list.add(builder.toString());
                builder.setLength(0);
            } else {
                builder.append(ch);
            }
        }
        if (builder.length() > 0) {
            list.add(builder.toString());
            builder.setLength(0);
        }

        List<String> names = new ArrayList<>(3);
        String[] types = Utils.split(DEFAULT_TABLE_TYPES, ',', true, true, true).toArray(Constants.EMPTY_STRING_ARRAY);
        if (list.isEmpty()) {
            return Collections.emptyList();
        }
        switch (list.size()) {
            case 1: // just table name
                names.add(Utils.getCatalogName(conn));
                names.add(Utils.getSchemaName(conn));
                names.add(list.get(0));
                break;
            case 2: {
                String catalogOrSchema = list.get(0);
                String pattern = list.get(1);
                String normalizedTable = normalizedName(pattern, quoteString);
                String name = Utils.getSchemaName(conn);
                try (ResultSet rs = metaData.getTables(catalogOrSchema, name, pattern, types)) {
                    while (rs.next()) {
                        if (normalizedTable.equals(normalizedName(rs.getString("TABLE_NAME"), quoteString))) {
                            names.add(catalogOrSchema);
                            names.add(name);
                            names.add(pattern);
                            break;
                        }
                    }
                } catch (SQLFeatureNotSupportedException | UnsupportedOperationException e) {
                    // ignore
                }
                if (names.isEmpty()) {
                    name = Utils.getCatalogName(conn);
                    try (ResultSet rs = metaData.getTables(name, catalogOrSchema, pattern, types)) {
                        while (rs.next()) {
                            if (normalizedTable.equals(normalizedName(rs.getString("TABLE_NAME"), quoteString))) {
                                names.add(name);
                                names.add(catalogOrSchema);
                                names.add(pattern);
                                break;
                            }
                        }
                    } catch (SQLFeatureNotSupportedException | UnsupportedOperationException e) {
                        // ignore
                    }
                }
                break;
            }
            case 3:
                names = list;
                break;
            default:
                names = list.subList(0, 3);
                break;
        }
        return names.isEmpty() ? Collections.emptyList() : Collections.unmodifiableList(names);
    }

    static final String getDatabaseTable(DatabaseMetaData metaData, String catalog, String schema, String table)
            throws SQLException {
        StringBuilder builder = new StringBuilder();

        String quoteString = metaData.getIdentifierQuoteString();
        quoteString = Checker.isNullOrEmpty(quoteString) ? Constants.EMPTY_STRING : quoteString.trim();
        final String normalizedTable = normalizedName(table, quoteString);

        if (!Checker.isNullOrEmpty(catalog)) {
            builder.append(JsonHelper.encode(metaData.getCatalogTerm())).append(':').append(JsonHelper.encode(catalog));
        }
        if (!Checker.isNullOrEmpty(schema)) {
            if (builder.length() > 0) {
                builder.append(',');
            }
            builder.append(JsonHelper.encode(metaData.getSchemaTerm())).append(':').append(JsonHelper.encode(schema));
        }
        builder.append(',').append(Constants.JSON_PROP_TABLE).append(JsonHelper.encode(table));

        // columns
        builder.append(',').append(JSON_PROP_COLUMNS).append('[');
        try (ResultSet rs = metaData.getColumns(catalog, schema, table, DEFAULT_TABLE_PATTERN)) {
            boolean first = true;
            while (rs.next()) {
                final String columnTable = normalizedName(rs.getString("TABLE_NAME"), quoteString);
                final String columnName = rs.getString("COLUMN_NAME");
                if (!normalizedTable.equals(columnTable) || Checker.isNullOrEmpty(columnName)) {
                    continue;
                }

                if (first) {
                    first = false;
                } else {
                    builder.append(',');
                }
                builder.append('{').append(Constants.JSON_PROP_NAME).append(JsonHelper.encode(columnName)).append(',')
                        .append(Constants.JSON_PROP_TYPE).append(JsonHelper.encode(rs.getString("TYPE_NAME")));
                builder.append(',').append(JSON_PROP_NULLABLE)
                        .append(!"no".equalsIgnoreCase(rs.getString("IS_NULLABLE")));
                String remarks = rs.getString("REMARKS");
                if (!Checker.isNullOrEmpty(remarks)) {
                    builder.append(',').append(Constants.JSON_PROP_DESC).append(JsonHelper.encode(remarks));
                }
                builder.append('}');
            }
        }
        builder.append(']');

        // constraints
        try (ResultSet rs = metaData.getPrimaryKeys(catalog, schema, table)) {
            Map<String, List<OrderedColumn>> columMappings = new LinkedHashMap<>();
            while (rs.next()) {
                final String columnTable = normalizedName(rs.getString("TABLE_NAME"), quoteString);
                final String columnName = rs.getString("COLUMN_NAME");
                if (!normalizedTable.equals(columnTable) || Checker.isNullOrEmpty(columnName)) {
                    continue;
                }

                final String pkName = normalizedName(rs.getString("PK_NAME"), Constants.EMPTY_STRING);
                List<OrderedColumn> list = columMappings.get(pkName);
                if (list == null) {
                    list = new ArrayList<>();
                    columMappings.put(pkName, list);
                }
                list.add(new OrderedColumn(rs.getShort("KEY_SEQ"), columnName));
            }

            if (!columMappings.isEmpty()) {
                boolean first = true;
                builder.append(',').append(JSON_PROP_PRIMARY_KEY).append('[');
                if (columMappings.size() == 1) {
                    builder.append(String.join(",", columMappings.values().iterator().next().stream()
                            .sorted((x, y) -> x.position - y.position).map(x -> JsonHelper.encode(x.column))
                            .collect(Collectors.toList())));
                } else {
                    for (Entry<String, List<OrderedColumn>> e : columMappings.entrySet()) {
                        if (first) {
                            first = false;
                        } else {
                            builder.append(',');
                        }
                        builder.append('{').append(Constants.JSON_PROP_NAME).append(JsonHelper.encode(e.getKey()))
                                .append(',')
                                .append(JSON_PROP_COLUMNS).append('[').append(String.join(",", e.getValue().stream()
                                        .sorted((x, y) -> x.position - y.position).map(x -> JsonHelper.encode(x.column))
                                        .collect(Collectors.toList())))
                                .append(']').append('}');
                    }
                }
                builder.append(']');
            }
        }

        // indexes
        builder.append(',').append(JSON_PROP_INDEXES).append('[');
        try (ResultSet rs = metaData.getIndexInfo(catalog, schema, table, false, true)) {
            Map<String, List<OrderedColumn>> columMappings = new LinkedHashMap<>();
            while (rs.next()) {
                final String indexTable = normalizedName(rs.getString("TABLE_NAME"), quoteString);
                final String indexName = rs.getString("INDEX_NAME");
                if (!normalizedTable.equals(indexTable) || indexName == null) {
                    continue;
                }

                List<OrderedColumn> list = columMappings.get(indexName);
                if (list == null) {
                    list = new ArrayList<>();
                    columMappings.put(indexName, list);
                }
                list.add(new OrderedColumn(rs.getInt("ORDINAL_POSITION"), rs.getString("COLUMN_NAME")));
            }

            boolean first = true;
            for (Entry<String, List<OrderedColumn>> e : columMappings.entrySet()) {
                if (first) {
                    first = false;
                } else {
                    builder.append(',');
                }
                builder.append('{').append(Constants.JSON_PROP_NAME).append(JsonHelper.encode(e.getKey())).append(',')
                        .append(JSON_PROP_COLUMNS).append('[')
                        .append(String.join(",", e.getValue().stream()
                                .sorted((x, y) -> x.position - y.position).map(x -> JsonHelper.encode(x.column))
                                .collect(Collectors.toList())))
                        .append(']').append('}');
            }
        }
        return builder.append(']').toString();
    }

    public static final String getCurrentDatabaseCatalogAndSchema(Connection conn, DatabaseMetaData metaData) {
        StringBuilder builder = new StringBuilder();
        try {
            final String catalog = Utils.getCatalogName(conn);
            String catalogTerm = null;
            if (!Checker.isNullOrEmpty(catalog)) {
                catalogTerm = metaData.getCatalogTerm();
                builder.append(JsonHelper.encode(
                        PREFIX_CURRENT
                                + (Checker.isNullOrEmpty(catalogTerm) ? TERM_CATALOG : Utils.capitalize(catalogTerm))))
                        .append(':')
                        .append(JsonHelper.encode(catalog));
            }

            final String schema = Utils.getSchemaName(conn);
            if (!Checker.isNullOrEmpty(schema)) {
                String schemaTerm = metaData.getSchemaTerm();
                if (!Objects.equals(schemaTerm, catalogTerm)) {
                    if (builder.length() > 0) {
                        builder.append(',');
                    }
                    builder.append(JsonHelper.encode(
                            PREFIX_CURRENT
                                    + (Checker.isNullOrEmpty(schemaTerm) ? TERM_SCHEMA : Utils.capitalize(schemaTerm))))
                            .append(':')
                            .append(JsonHelper.encode(schema));
                }
            }
        } catch (Exception e) {
            // ignore
        }
        return builder.toString();
    }

    public static final String getDatabaseTable(Connection conn, String table) throws SQLException {
        List<String> names = getFullQualifiedTable(conn, table);
        if (names.isEmpty()) {
            return Constants.EMPTY_STRING;
        }

        return getDatabaseTable(conn.getMetaData(), names.get(0), names.get(1), names.get(2));
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

    private final String defaultConnectionUrl;
    private final String defaultConnectionProps;

    private final ClassLoader loader;
    private final JdbcExecutor executor;

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
