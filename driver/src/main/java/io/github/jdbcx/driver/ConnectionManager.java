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
package io.github.jdbcx.driver;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ParameterMetaData;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import io.github.jdbcx.Cache;
import io.github.jdbcx.Checker;
import io.github.jdbcx.ConfigManager;
import io.github.jdbcx.Constants;
import io.github.jdbcx.DriverExtension;
import io.github.jdbcx.Field;
import io.github.jdbcx.Format;
import io.github.jdbcx.JdbcActivityListener;
import io.github.jdbcx.JdbcDialect;
import io.github.jdbcx.Logger;
import io.github.jdbcx.LoggerFactory;
import io.github.jdbcx.Option;
import io.github.jdbcx.QueryContext;
import io.github.jdbcx.RequestParameter;
import io.github.jdbcx.ResourceManager;
import io.github.jdbcx.Result;
import io.github.jdbcx.Row;
import io.github.jdbcx.Utils;
import io.github.jdbcx.VariableTag;
import io.github.jdbcx.dialect.DefaultDialect;
import io.github.jdbcx.executor.WebExecutor;
import io.github.jdbcx.executor.jdbc.SqlExceptionUtils;

public final class ConnectionManager implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(ConnectionManager.class);

    private static final Cache<String, JdbcDialect> cache = Cache.create(50, 0, ConnectionManager::createDialect);

    private static final List<Field> detailFields = Collections.unmodifiableList(
            Arrays.asList(Field.of("name"), Field.of("value"), Field.of("default_value"), Field.of("description"),
                    Field.of("choices"), Field.of("system_property"), Field.of("environment_variable")));

    public static final String DB_EXTENSION = "db";
    public static final String JDBC_PREFIX = "jdbc:";
    public static final String JDBCX_PREFIX = Option.PROPERTY_JDBCX + ":";

    public static final String translate(String query) {
        return query;
    }

    public static Result<?> describe(DriverExtension ext, Properties props) { // NOSONAR
        List<Option> options = ext.getDefaultOptions();
        List<Row> rows = new ArrayList<>(options.size());
        for (Option o : options) {
            rows.add(Row.of(detailFields,
                    new Object[] { o.getName(), o.getValue(props), o.getDefaultValue(), o.getDescription(),
                            String.join(",", o.getChoices()), o.getSystemProperty(Option.PROPERTY_PREFIX),
                            o.getEnvironmentVariable(Option.PROPERTY_PREFIX) }));
        }
        return Result.of(detailFields, rows);
    }

    public static final String normalize(String result, VariableTag tag, Properties props) {
        if (result == null) {
            return Constants.EMPTY_STRING;
        }

        if (Boolean.parseBoolean(Option.RESULT_STRING_REPLACE.getValue(props))) {
            result = Utils.applyVariables(result, tag, props);
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

    public static final JdbcDialect findDialect(String product) {
        return cache.get(product);
    }

    static final JdbcDialect createDialect(String product) {
        if (product != null) {
            int index = product.indexOf('/');
            if (index != -1) {
                product = product.substring(0, index).concat("Dialect");
            } else {
                index = product.lastIndexOf('.');
                if (index != -1) {
                    product = product.substring(index + 1);
                }
                if (!product.isEmpty()) {
                    product = new StringBuilder(product.length())
                            .append(product.substring(0, 1).toLowerCase(Locale.ROOT)).append(product.substring(1))
                            .toString();
                }
            }
        }
        return Utils.createInstance(JdbcDialect.class, product, DefaultDialect.getInstance());
    }

    private final List<DriverExtension> extList;
    private final Map<String, DriverExtension> extMappings;
    private final DriverExtension defaultExtension;
    private final Connection conn;
    private final String bridgeUrl;
    private final Properties bridgeConfig;
    private final String bridgeToken;
    private final String jdbcUrl;
    private final Properties props;
    private final Properties normalizedProps;
    private final String originalJdbcUrl;
    private final Properties originalProps;

    private final ConfigManager configManager;

    private final VariableTag tag;

    private final ClassLoader classLoader;

    private final AtomicReference<ConnectionMetaData> metaDataRef;
    private final AtomicReference<JdbcDialect> dialectRef;

    // track the new connections created by this manager instance
    private final Set<Connection> additionalConns;

    JdbcActivityListener createListener(DriverExtension ext, QueryContext context, Connection conn, Properties props) {
        if (ext.requiresBridgeContext()) {
            String key = QueryContext.KEY_BRIDGE;
            if (context.get(key) == null) {
                context.put(key, getBridgeContext());
            }
            key = QueryContext.KEY_DIALECT;
            if (context.get(key) == null) {
                context.put(key, getDialect());
            }
        }
        return ext.createListener(context, conn, props);
    }

    ConnectionManager(DriverInfo info) throws SQLException {
        this(info.configManager, info.getExtensions(), info.extension,
                info.driver.connect(info.actualUrl, info.normalizedInfo), info.actualUrl, info.extensionProps,
                info.normalizedInfo, info.normalizedUrl, info.mergedInfo);
    }

    public ConnectionManager(ConfigManager configManager, Map<String, DriverExtension> extensions,
            DriverExtension defaultExtension, Connection conn, String url, Properties extensionProps,
            Properties normalizedProps, String originalUrl, Properties originalProps) {
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

        String bridge = Option.SERVER_URL.getJdbcxValue(originalProps);
        if (Checker.isNullOrEmpty(bridge)) {
            bridge = Utils.format("http://%s:%s%s", Option.SERVER_HOST.getValue(originalProps, Utils.getHost(null)),
                    Option.SERVER_PORT.getValue(originalProps), Option.SERVER_CONTEXT.getValue(originalProps));
        }
        this.bridgeUrl = bridge;
        this.bridgeConfig = new Properties();

        String token = Option.SERVER_TOKEN.getJdbcxValue(originalProps).trim();
        this.bridgeToken = Checker.isNullOrEmpty(token) ? token : Utils.toBase64(token);

        this.jdbcUrl = url;
        this.props = extensionProps;
        this.normalizedProps = normalizedProps;
        this.originalJdbcUrl = originalUrl;
        this.originalProps = originalProps;

        this.configManager = configManager;

        String varTag = originalProps.getProperty(Option.TAG.getJdbcxName());
        this.tag = Checker.isNullOrEmpty(varTag) ? null
                : VariableTag.valueOf(Option.TAG.getJdbcxValue(originalProps));
        this.classLoader = DriverInfo.getCustomClassLoader(
                Utils.normalizePath(originalProps.getProperty(Option.CUSTOM_CLASSPATH.getJdbcxName(),
                        Option.CUSTOM_CLASSPATH.getEffectiveDefaultValue(Option.PROPERTY_PREFIX))));

        this.metaDataRef = new AtomicReference<>();
        this.dialectRef = new AtomicReference<>();

        this.additionalConns = Collections.synchronizedSet(new LinkedHashSet<>());
        this.additionalConns.add(conn); // ensure this connection won't be added twice
    }

    protected Connection enlist(Connection newConn) {
        this.additionalConns.add(newConn);
        return newConn;
    }

    @Override
    public void close() throws SQLException {
        additionalConns.remove(conn); // prefer to close at the end of execution
        Utils.closeQuietly(additionalConns, false);
        conn.close();
    }

    public Connection createWrappedConnection() {
        try {
            final Connection connection = DriverManager.getConnection(originalJdbcUrl, originalProps);
            return enlist(connection instanceof ManagedConnection ? connection : new WrappedConnection(connection));
        } catch (SQLException e) {
            throw new CompletionException(e);
        }
    }

    public Connection createConnection() {
        try {
            if (Utils.startsWith(jdbcUrl, JDBC_PREFIX, true)) {
                final Driver driver = DriverInfo.findSuitableDriver(jdbcUrl, normalizedProps, classLoader);
                log.debug("Connecting through driver [%s]...", driver);
                return enlist(driver.connect(jdbcUrl, normalizedProps));
            } else {
                final boolean wrapped = Utils.startsWith(jdbcUrl, JDBCX_PREFIX, true);
                log.debug("Connecting using DriverManager(wrapped=%s)...", wrapped);
                return enlist(DriverManager.getConnection(jdbcUrl, wrapped ? originalProps : normalizedProps));
            }
        } catch (SQLException e) {
            throw new CompletionException(e);
        }
    }

    public QueryContext createContext() {
        final QueryContext context = QueryContext.newContext();
        final Supplier<Connection> connSupplier = this::createConnection;
        final Supplier<Connection> wrappedConnSupplier = this::createWrappedConnection;
        final Supplier<VariableTag> tagSupplier = this::getVariableTag;
        context.put(QueryContext.KEY_CONFIG, configManager);
        if (conn instanceof ResourceManager) {
            context.put(QueryContext.KEY_RESOURCE, conn);
        }
        context.put(QueryContext.KEY_CONNECTION, connSupplier);
        context.put(QueryContext.KEY_MANAGED_CONNECTION, wrappedConnSupplier);
        context.put(QueryContext.KEY_TAG, tagSupplier);
        return context;
    }

    public JdbcActivityListener createDefaultListener(QueryContext context) {
        return createListener(defaultExtension, context, conn, props);
    }

    public JdbcActivityListener createListener(QueryContext context) throws SQLException {
        return createListener(getExtension(conn.getCatalog()), context, conn, props);
    }

    public Properties extractProperties(DriverExtension ext) {
        return new Properties(
                ext == defaultExtension ? props : DriverExtension.extractProperties(ext, originalProps));
    }

    public ConfigManager getConfigManager() {
        return configManager;
    }

    public Connection getConnection() {
        return conn;
    }

    public List<DriverExtension> getSupportedExtensions() {
        return this.extList;
    }

    public VariableTag getVariableTag() {
        return tag != null ? tag : getDialect().getVariableTag();
    }

    public ConnectionMetaData getMetaData() {
        ConnectionMetaData metaData = metaDataRef.get();
        if (metaData == null) {
            try {
                metaData = new ConnectionMetaData(conn.getMetaData());
            } catch (Exception e) {
                log.warn("Failed to get database meta data from %s, use class name instead", conn, e);
                metaData = new ConnectionMetaData(conn.getClass().getPackage().getName());
            }

            if (!metaDataRef.compareAndSet(null, metaData)) {
                metaData = metaDataRef.get();
            }
        }
        return metaData;
    }

    public JdbcDialect getDialect() {
        JdbcDialect dialect = dialectRef.get();
        if (dialect == null) {
            dialect = cache.get(getMetaData().getProduct());
            if (!dialectRef.compareAndSet(null, dialect)) {
                dialect = dialectRef.get();
            }
        }
        return dialect;
    }

    public boolean isUsingDatabaseExtension() {
        return defaultExtension.getName().equals(DB_EXTENSION) || defaultExtension.getAliases().contains(DB_EXTENSION);
    }

    public boolean isUsingDatabaseOrDefaultExtension() {
        final DriverExtension dbExt = extMappings.get(DB_EXTENSION);
        DriverExtension currentExt = defaultExtension;
        try {
            currentExt = extMappings.get(getConnection().getCatalog());
        } catch (SQLException e) {
            // ignore
        }
        return (defaultExtension == DefaultDriverExtension.getInstance() || defaultExtension == dbExt)
                && (currentExt == DefaultDriverExtension.getInstance() || currentExt == dbExt);
    }

    public DriverExtension getDefaultExtension() {
        return defaultExtension;
    }

    public List<DriverExtension> getExtensions() {
        return extList;
    }

    public List<DriverExtension> getMatchedExtensions(String pattern) throws SQLException {
        final List<DriverExtension> list;
        if (pattern == null || (pattern.length() == 1 && pattern.charAt(0) == '%')) {
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
        } else if (DefaultDriverExtension.getInstance().getName().equals(name)) {
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

    public String getBridgeUrl() {
        return bridgeUrl;
    }

    /**
     * Gets bridge context.
     *
     * @return non-empty {@link Properties} with the bridge server config as parent
     */
    public Properties getBridgeContext() {
        if (bridgeConfig.isEmpty()) {
            final String url = bridgeUrl + "config";
            final String timeout = WebExecutor.OPTION_CONNECT_TIMEOUT.getValue(normalizedProps);
            try {
                Properties config = new Properties();
                WebExecutor web = new WebExecutor(getVariableTag(), config);

                WebExecutor.OPTION_CONNECT_TIMEOUT.setValue(config, timeout);
                WebExecutor.OPTION_SOCKET_TIMEOUT.setValue(config, timeout);
                WebExecutor.OPTION_FOLLOW_REDIRECT.setValue(config, Constants.FALSE_EXPR);
                try (InputStream input = web.get(Utils.toURL(url), config,
                        Collections.singletonMap(RequestParameter.FORMAT.header(), Format.TXT.mimeType()))) {
                    bridgeConfig.load(input);
                }
                bridgeConfig.setProperty(DriverExtension.PROPERTY_BRIDGE_URL, bridgeUrl);
                if (!Checker.isNullOrEmpty(bridgeToken)
                        && Boolean.parseBoolean(Option.SERVER_AUTH.getValue(bridgeConfig))) {
                    bridgeConfig.setProperty(DriverExtension.PROPERTY_BRIDGE_TOKEN, bridgeToken);
                }
                ConnectionMetaData md = getMetaData();
                bridgeConfig.setProperty(DriverExtension.PROPERTY_PRODUCT, md.getProduct());
                if (md.hasUserName()) {
                    bridgeConfig.setProperty(DriverExtension.PROPERTY_USER, md.getUserName());
                }
            } catch (Exception e) {
                log.warn(
                        "Failed to get bridge server config from [%s] (timeout is %s ms), use default configuration instead",
                        url, timeout, e);
            }
        }
        return new Properties(bridgeConfig);
    }

    /**
     * Gets base64 encoded token to access bridge server.
     *
     * @return non-null base64 encoded token
     */
    public String getBridgeToken() {
        return bridgeToken;
    }

    public String getJdbcUrl() {
        return jdbcUrl;
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
            if (metaData != null) {
                metaDataRef.compareAndSet(null, new ConnectionMetaData(metaData));
            } else {
                metaData = conn.getMetaData();
            }
            return createDefaultListener(context).onMetaData(metaData);
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
