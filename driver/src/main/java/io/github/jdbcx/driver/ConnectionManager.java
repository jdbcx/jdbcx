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
package io.github.jdbcx.driver;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ParameterMetaData;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;
import java.util.concurrent.CompletionException;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import io.github.jdbcx.Cache;
import io.github.jdbcx.Checker;
import io.github.jdbcx.Constants;
import io.github.jdbcx.DriverExtension;
import io.github.jdbcx.Field;
import io.github.jdbcx.JdbcActivityListener;
import io.github.jdbcx.JdbcDialect;
import io.github.jdbcx.Logger;
import io.github.jdbcx.LoggerFactory;
import io.github.jdbcx.Option;
import io.github.jdbcx.QueryContext;
import io.github.jdbcx.Result;
import io.github.jdbcx.Row;
import io.github.jdbcx.Utils;
import io.github.jdbcx.Version;
import io.github.jdbcx.data.StringValue;
import io.github.jdbcx.dialect.DefaultDialect;
import io.github.jdbcx.executor.jdbc.SqlExceptionUtils;

public final class ConnectionManager implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(ConnectionManager.class);

    private static final JdbcDialect defaultDialect = new DefaultDialect();
    private static final Cache<String, JdbcDialect> cache = Cache.create(50, 0, ConnectionManager::getDialect);

    private static final List<Field> detailFields = Collections.unmodifiableList(
            Arrays.asList(Field.of("name"), Field.of("value"), Field.of("default_value"), Field.of("description"),
                    Field.of("choices"), Field.of("system_property"), Field.of("environment_variable")));

    public static final Version DRIVER_VERSION = Version.of(Utils.getVersion());
    public static final String JDBC_PREFIX = "jdbc:";
    public static final String JDBCX_PREFIX = Option.PROPERTY_JDBCX + ":";

    public static Result<?> describe(DriverExtension ext, Properties props) {
        List<Option> options = ext.getDefaultOptions();
        List<Row> rows = new ArrayList<>(options.size());
        for (Option o : options) {
            rows.add(Row.of(detailFields, new StringValue(o.getName()),
                    new StringValue(o.getValue(props)),
                    new StringValue(o.getDefaultValue()),
                    new StringValue(o.getDescription()),
                    new StringValue(String.join(",", o.getChoices())),
                    new StringValue(o.getSystemProperty(Option.PROPERTY_PREFIX)),
                    new StringValue(o.getEnvironmentVariable(Option.PROPERTY_PREFIX))));
        }
        return Result.of(detailFields, rows);
    }

    public static final String normalize(String result, Properties props) {
        if (result == null) {
            return Constants.EMPTY_STRING;
        }

        if (Boolean.parseBoolean(Option.RESULT_STRING_REPLACE.getValue(props))) {
            result = Utils.applyVariables(result, props);
        }
        if (Boolean.parseBoolean(Option.RESULT_STRING_TRIM.getValue(props))) {
            result = result.trim();
        }
        if (Boolean.parseBoolean(Option.RESULT_STRING_ESCAPE.getValue(props))) {
            result = Utils.escape(result, Option.RESULT_STRING_ESCAPE_TARGET.getValue(props).charAt(0),
                    Option.RESULT_STRING_ESCAPE_CHAR.getValue(props).charAt(0));
        }
        return result;
    }

    public static final <T> T convertTo(Result<?> result, Class<T> clazz) throws SQLException {
        try {
            return result.get(clazz);
        } catch (CompletionException e) {
            if (e.getCause() == null) {
                throw SqlExceptionUtils.clientWarning(e);
            } else {
                throw SqlExceptionUtils.clientError(e);
            }
        } catch (Exception e) { // unexpected error
            throw SqlExceptionUtils.clientError(e);
        }
    }

    public static final JdbcDialect getDialect(String product) {
        return defaultDialect;
    }

    public static final String getDatabaseProduct(Connection conn) {
        if (conn == null) {
            return Constants.EMPTY_STRING;
        }
        StringBuilder builder = new StringBuilder();
        try {
            DatabaseMetaData metaData = conn.getMetaData();
            builder.append(metaData.getDatabaseProductName()).append(' ').append(metaData.getDatabaseMajorVersion())
                    .append('.').append(metaData.getDatabaseMinorVersion());
        } catch (SQLException e) {
            log.debug("Failed to retrieve database product information", e);
        }
        return builder.length() > 0 ? builder.toString() : Constants.EMPTY_STRING;
    }

    private final List<DriverExtension> extList;
    private final Map<String, DriverExtension> extMappings;
    private final DriverExtension defaultExtension;
    private final Connection conn;
    private final JdbcDialect dialect;
    private final String url;
    private final Properties props;
    private final Properties normalizedProps;
    private final Properties originalProps;

    ConnectionManager(DriverInfo info) throws SQLException {
        this(info.getExtensions(), info.extension, info.driver.connect(info.actualUrl, info.normalizedInfo),
                info.actualUrl, info.extensionProps, info.normalizedInfo, info.mergedInfo);
    }

    public ConnectionManager(Map<String, DriverExtension> extensions, DriverExtension defaultExtension, Connection conn,
            String url, Properties extensionProps, Properties normalizedProps, Properties originalProps) {
        List<DriverExtension> list = new LinkedList<>();
        for (DriverExtension e : extensions.values()) {
            if (!list.contains(e)) {
                list.add(e);
            }
        }
        this.extList = Collections.unmodifiableList(new ArrayList<>(list));
        this.extMappings = extensions;
        this.defaultExtension = defaultExtension;
        this.conn = conn;
        this.dialect = null; // cache.get(getDatabaseProduct(conn));
        this.url = url;
        this.props = extensionProps;
        this.normalizedProps = normalizedProps;
        this.originalProps = originalProps;
    }

    @Override
    public void close() throws SQLException {
        conn.close();
    }

    public Connection createConnection() {
        try {
            return DriverManager.getConnection(url, normalizedProps);
        } catch (SQLException e) {
            throw new CompletionException(e);
        }
    }

    public QueryContext createContext() {
        final QueryContext context = QueryContext.newContext();
        final Supplier<Connection> supplier = this::createConnection;
        context.put(QueryContext.KEY_CONNECTION, supplier);
        return context;
    }

    public JdbcActivityListener createDefaultListener(QueryContext context) {
        return defaultExtension.createListener(context, conn, props);
    }

    public JdbcActivityListener createListener(QueryContext context) throws SQLException {
        return getExtension(conn.getCatalog()).createListener(context, conn, props);
    }

    public Properties extractProperties(DriverExtension ext) {
        return new Properties(
                ext == defaultExtension ? props : DriverExtension.extractProperties(ext, originalProps));
    }

    public Connection getConnection() {
        return conn;
    }

    public JdbcDialect getDialect() {
        return dialect;
    }

    public DriverExtension getDefaultExtension() {
        return defaultExtension;
    }

    public List<DriverExtension> getExtensions() {
        return extList;
    }

    public List<DriverExtension> getMatchedExtensions(String pattern) throws SQLException {
        final List<DriverExtension> list;
        if (pattern == null) {
            list = extList;
        } else if (Utils.containsJdbcWildcard(pattern)) {
            list = new ArrayList<>(extList.size());
            String re = Utils.jdbcNamePatternToRe(pattern);
            for (Entry<String, DriverExtension> e : extMappings.entrySet()) {
                if (Pattern.matches(re, e.getKey()) && !list.contains(e.getValue())) {
                    list.add(e.getValue());
                }
            }
        } else {
            list = Collections.singletonList(getExtension(pattern));
        }
        return list;
    }

    public DriverExtension getExtension(String name) throws SQLException {
        final DriverExtension ext;
        if (Checker.isNullOrEmpty(name)) {
            ext = defaultExtension;
        } else if (DriverExtension.getName(DefaultDriverExtension.getInstance()).equals(name)) {
            ext = DefaultDriverExtension.getInstance();
        } else {
            ext = extMappings.get(name);
        }

        if (ext == null) {
            throw SqlExceptionUtils
                    .clientError(Utils.format("The specified extension \"%s\" is unknown and not supported. "
                            + "To find a list of supported extensions, please use the {{ help }} query.", name));
        }
        return ext;
    }

    public String getUrl() {
        return url;
    }

    public Properties getExtensionProperties() {
        return props;
    }

    public Properties getOriginalProperties() {
        return originalProps;
    }

    public Properties getNormalizedProperties() {
        return normalizedProps;
    }

    public DatabaseMetaData getMetaData(DatabaseMetaData metaData) throws SQLException {
        try (QueryContext context = createContext()) {
            return createDefaultListener(context).onMetaData(metaData != null ? metaData : conn.getMetaData());
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw SqlExceptionUtils.clientError(e);
        }
    }

    public ResultSetMetaData getMetaData(ResultSetMetaData metaData) throws SQLException {
        try (QueryContext context = createContext()) {
            return createDefaultListener(context).onMetaData(metaData);
        } catch (Exception e) {
            throw SqlExceptionUtils.clientError(e);
        }
    }

    public ParameterMetaData getMetaData(ParameterMetaData metaData) throws SQLException {
        try (QueryContext context = createContext()) {
            return createDefaultListener(context).onMetaData(metaData);
        } catch (Exception e) {
            throw SqlExceptionUtils.clientError(e);
        }
    }
}
