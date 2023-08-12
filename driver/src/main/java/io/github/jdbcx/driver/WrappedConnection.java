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

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import io.github.jdbcx.Cache;
import io.github.jdbcx.Checker;
import io.github.jdbcx.JdbcActivityListener;
import io.github.jdbcx.JdbcDialect;
import io.github.jdbcx.Constants;
import io.github.jdbcx.DriverExtension;
import io.github.jdbcx.Field;
import io.github.jdbcx.Logger;
import io.github.jdbcx.LoggerFactory;
import io.github.jdbcx.Option;
import io.github.jdbcx.QueryContext;
import io.github.jdbcx.Result;
import io.github.jdbcx.Row;
import io.github.jdbcx.Utils;
import io.github.jdbcx.data.StringValue;
import io.github.jdbcx.dialect.DefaultDialect;
import io.github.jdbcx.executor.jdbc.SqlExceptionUtils;

/**
 * This class serves as a wrapper for a {@link Connection}.
 */
public class WrappedConnection implements Connection {
    private static final Logger log = LoggerFactory.getLogger(WrappedConnection.class);

    private static final JdbcDialect defaultDialect = new DefaultDialect();
    private static final Cache<String, JdbcDialect> cache = Cache.create(50, 0, WrappedConnection::getDialect);

    private static final Field[] detailFields = new Field[] {
            Field.of("name"), Field.of("value"), Field.of("default_value"), Field.of("description"),
            Field.of("choices"), Field.of("system_property"), Field.of("environment_variable"),
    };

    static Result<?> describe(DriverExtension ext, Properties props) {
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
        return Result.of(rows);
    }

    static final JdbcDialect getDialect(String product) {
        return defaultDialect;
    }

    static final String getDatabaseProduct(Connection conn) {
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

    static final String normalize(String result, Properties props) {
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

    static final <T> T convertTo(Result<?> result, Class<T> clazz) throws SQLException {
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

    protected final Connection conn;
    protected final JdbcDialect dialect;
    protected final String url;
    protected final Properties props;

    protected final DriverExtension defaultExtension;
    protected final Map<String, DriverExtension> extensions;

    private final Properties originalProps;
    private final AtomicReference<SQLWarning> warning;

    final Connection createConnection() {
        try {
            return DriverManager.getConnection(url, originalProps);
        } catch (SQLException e) {
            throw new CompletionException(e);
        }
    }

    @SuppressWarnings("resources")
    protected QueryContext createContext() {
        final QueryContext context = QueryContext.newContext();
        final Supplier<Connection> supplier = this::createConnection;
        context.put(QueryContext.KEY_CONNECTION, supplier);
        return context;
    }

    protected JdbcActivityListener createListener(QueryContext context) {
        return defaultExtension.createListener(context, conn, props);
    }

    protected WrappedConnection(Connection conn) {
        DriverInfo info = HelpDriverExtension.ActivityListener.defaultDriverInfo;
        this.defaultExtension = info.extension;
        this.extensions = info.getExtensions();

        this.conn = conn;
        this.dialect = cache.get(getDatabaseProduct(conn));
        this.url = Constants.EMPTY_STRING;
        this.props = info.extensionProps;

        this.originalProps = info.normalizedInfo;
        this.warning = new AtomicReference<>();
    }

    protected WrappedConnection(DriverInfo info) throws SQLException {
        this.defaultExtension = info.extension;
        this.extensions = info.getExtensions();

        this.conn = info.getActualDriver().connect(info.actualUrl, info.normalizedInfo);
        this.dialect = cache.get(getDatabaseProduct(this.conn));
        this.url = info.actualUrl;
        this.props = info.extensionProps;

        this.originalProps = info.normalizedInfo;
        this.warning = new AtomicReference<>();
    }

    protected DriverExtension getExtension(String name) throws SQLException {
        DriverExtension ext = Checker.isNullOrEmpty(name) ? defaultExtension
                : extensions.get(name);
        if (ext == null) {
            throw SqlExceptionUtils
                    .clientError(Utils.format("The specified extension \"xx\" is unknown and not supported. "
                            + "To find a list of supported extensions, please use the {{ help }} query.", name));
        }
        return ext;
    }

    protected Properties extractProperties(DriverExtension ext) {
        return new Properties(
                ext == defaultExtension ? props : DriverExtension.extractProperties(ext, originalProps));
    }

    protected List<String> handleResults(String query, WrappedStatement stmt) throws SQLException {
        stmt.warning.set(null);

        SQLWarning w = null;
        try (QueryContext context = createContext()) {
            final ParsedQuery pq = QueryParser.parse(query, context.getCustomVariables());
            final QueryBuilder builder = new QueryBuilder(context, pq, stmt);
            List<String> queries = builder.build();
            if (queries.isEmpty()) {
                return Collections.emptyList();
            }

            w = builder.getLastWarning();

            final JdbcActivityListener listener = createListener(context);
            final int size = queries.size();
            if (size == 1) {
                return Collections
                        .singletonList(normalize(convertTo(listener.onQuery(queries.get(0)), String.class), props));
            }

            // not worthy of running in parallel?
            List<String> results = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                String q = queries.get(i);
                results.add(normalize(convertTo(listener.onQuery(q), String.class), props));
            }
            return Collections.unmodifiableList(results);
        } catch (SQLWarning e) {
            stmt.warning.set(SqlExceptionUtils.consolidate(w, e));
            return Collections.singletonList(query);
        } catch (Throwable t) { // NOSONAR
            throw SqlExceptionUtils.clientError(t);
        }
    }

    protected String handleString(String query) throws SQLException {
        return handleString(query, null);
    }

    protected String handleString(String query, AtomicReference<SQLWarning> ref) throws SQLException {
        if (ref == null) {
            ref = warning;
        }
        ref.set(null);

        SQLWarning w = null;
        try (QueryContext context = createContext()) {
            final ParsedQuery pq = QueryParser.parse(query, context.getCustomVariables());
            final String[] parts = pq.getStaticParts().toArray(Constants.EMPTY_STRING_ARRAY);
            for (ExecutableBlock block : pq.getExecutableBlocks()) {
                DriverExtension ext = Checker.isNullOrEmpty(block.getExtensionName()) ? defaultExtension
                        : extensions.get(block.getExtensionName());
                if (ext == null) {
                    ext = defaultExtension;
                    final String msg = "Extension \"%s\" is not supported, using default extension [%s] instead";
                    log.warn(msg, block.getExtensionName(), ext);
                    ref.set(SqlExceptionUtils.consolidate(w,
                            new SQLWarning(Utils.format(msg, block.getExtensionName(), ext))));
                }

                Properties p = new Properties(
                        ext == defaultExtension ? props : DriverExtension.extractProperties(ext, originalProps));
                p.putAll(context.getCustomVariables());
                for (Entry<Object, Object> entry : block.getProperties().entrySet()) {
                    String key = Utils.applyVariables(entry.getKey().toString(), p);
                    String val = Utils.applyVariables(entry.getValue().toString(), p);
                    p.setProperty(key, val);
                }
                JdbcActivityListener cl = ext.createListener(context, conn, p); // NOSONAR
                if (block.hasOutput()) {
                    try {
                        parts[block.getIndex()] = normalize(convertTo(
                                cl.onQuery(Utils.applyVariables(block.getContent(), context.getCustomVariables())),
                                String.class), p);
                    } catch (SQLWarning e) {
                        ref.set(w = SqlExceptionUtils.consolidate(w, e));
                        parts[block.getIndex()] = block.getContent();
                    }
                } else {
                    cl.onQuery(block.getContent());
                }
            }

            final String substitutedQuery = Utils.applyVariables(String.join(Constants.EMPTY_STRING, parts),
                    context.getCustomVariables());
            log.debug("Original Query: [%s]\r\nSubstituted Query: [%s]", query, substitutedQuery);
            return convertTo(createListener(context).onQuery(substitutedQuery), String.class);
        } catch (SQLWarning e) {
            ref.set(SqlExceptionUtils.consolidate(w, e));
            return query;
        } catch (Throwable t) { // NOSONAR
            throw SqlExceptionUtils.clientError(t);
        }
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return conn.getClass() == iface ? iface.cast(conn) : conn.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return conn.getClass() == iface || conn.isWrapperFor(iface);
    }

    @Override
    public WrappedStatement createStatement() throws SQLException {
        return new WrappedStatement(this, conn.createStatement()); // NOSONAR
    }

    @Override
    public WrappedPreparedStatement prepareStatement(String query) throws SQLException {
        return new WrappedPreparedStatement(this, query, conn.prepareStatement(handleString(query))); // NOSONAR
    }

    @Override
    public WrappedCallableStatement prepareCall(String query) throws SQLException {
        return new WrappedCallableStatement(this, query, conn.prepareCall(handleString(query))); // NOSONAR
    }

    @Override
    public String nativeSQL(String query) throws SQLException {
        return conn.nativeSQL(handleString(query));
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        conn.setAutoCommit(autoCommit);
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return conn.getAutoCommit();
    }

    @Override
    public void commit() throws SQLException {
        conn.commit();
    }

    @Override
    public void rollback() throws SQLException {
        conn.rollback();
    }

    @Override
    public void close() throws SQLException {
        warning.set(null);
        conn.close();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return conn.isClosed();
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        try (QueryContext context = createContext()) {
            return createListener(context).onMetaData(conn.getMetaData());
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw SqlExceptionUtils.clientError(e);
        }
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        conn.setReadOnly(readOnly);
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return conn.isReadOnly();
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        conn.setCatalog(catalog);
    }

    @Override
    public String getCatalog() throws SQLException {
        return conn.getCatalog();
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        conn.setTransactionIsolation(level);
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return conn.getTransactionIsolation();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        SQLWarning w = warning.get();
        if (w != null) {
            SQLWarning next = null;
            try {
                next = conn.getWarnings();
            } catch (Exception e) {
                // ignore
            }
            return SqlExceptionUtils.consolidate(w, next);
        }
        return conn.getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
        warning.set(null);
        conn.clearWarnings();
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return new WrappedStatement(this, conn.createStatement(resultSetType, resultSetConcurrency)); // NOSONAR
    }

    @Override
    public PreparedStatement prepareStatement(String query, int resultSetType, int resultSetConcurrency)
            throws SQLException {
        return new WrappedPreparedStatement(this, query,
                conn.prepareStatement(handleString(query), resultSetType, resultSetConcurrency)); // NOSONAR
    }

    @Override
    public CallableStatement prepareCall(String query, int resultSetType, int resultSetConcurrency)
            throws SQLException {
        return new WrappedCallableStatement(this, query,
                conn.prepareCall(handleString(query), resultSetType, resultSetConcurrency)); // NOSONAR
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return conn.getTypeMap();
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        conn.setTypeMap(map);
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        conn.setHoldability(holdability);
    }

    @Override
    public int getHoldability() throws SQLException {
        return conn.getHoldability();
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        return conn.setSavepoint();
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        return conn.setSavepoint(name);
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        conn.rollback(savepoint);
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        conn.releaseSavepoint(savepoint);
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        return new WrappedStatement(this,
                conn.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability)); // NOSONAR
    }

    @Override
    public PreparedStatement prepareStatement(String query, int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException {
        return new WrappedPreparedStatement(this, query,
                conn.prepareStatement(handleString(query), resultSetType, resultSetConcurrency, resultSetHoldability)); // NOSONAR
    }

    @Override
    public CallableStatement prepareCall(String query, int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException {
        return new WrappedCallableStatement(this, query,
                conn.prepareCall(handleString(query), resultSetType, resultSetConcurrency, resultSetHoldability)); // NOSONAR
    }

    @Override
    public PreparedStatement prepareStatement(String query, int autoGeneratedKeys) throws SQLException {
        return new WrappedPreparedStatement(this, query, conn.prepareStatement(handleString(query), autoGeneratedKeys)); // NOSONAR
    }

    @Override
    public PreparedStatement prepareStatement(String query, int[] columnIndexes) throws SQLException {
        return new WrappedPreparedStatement(this, query, conn.prepareStatement(handleString(query), columnIndexes)); // NOSONAR
    }

    @Override
    public PreparedStatement prepareStatement(String query, String[] columnNames) throws SQLException {
        return new WrappedPreparedStatement(this, query, conn.prepareStatement(handleString(query), columnNames)); // NOSONAR
    }

    @Override
    public Clob createClob() throws SQLException {
        return conn.createClob();
    }

    @Override
    public Blob createBlob() throws SQLException {
        return conn.createBlob();
    }

    @Override
    public NClob createNClob() throws SQLException {
        return conn.createNClob();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        return conn.createSQLXML();
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        return conn.isValid(timeout);
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        conn.setClientInfo(name, value);
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        conn.setClientInfo(properties);
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        return conn.getClientInfo(name);
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return conn.getClientInfo();
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return conn.createArrayOf(typeName, elements);
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        return conn.createStruct(typeName, attributes);
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        conn.setSchema(schema);
    }

    @Override
    public String getSchema() throws SQLException {
        return conn.getSchema();
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        conn.abort(executor);
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        conn.setNetworkTimeout(executor, milliseconds);
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        return conn.getNetworkTimeout();
    }
}
