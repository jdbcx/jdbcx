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

import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
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
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;

import io.github.jdbcx.Checker;
import io.github.jdbcx.JdbcActivityListener;
import io.github.jdbcx.Constants;
import io.github.jdbcx.DriverExtension;
import io.github.jdbcx.Logger;
import io.github.jdbcx.LoggerFactory;
import io.github.jdbcx.QueryContext;
import io.github.jdbcx.Utils;
import io.github.jdbcx.VariableTag;
import io.github.jdbcx.executor.jdbc.SqlExceptionUtils;

/**
 * This class serves as a wrapper for a {@link Connection}.
 */
public class WrappedConnection implements ManagedConnection {
    private static final Logger log = LoggerFactory.getLogger(WrappedConnection.class);

    static final void copy(Statement from, Statement to) throws SQLException {
        int value = from.getFetchDirection();
        if (to.getFetchDirection() != value) {
            to.setFetchDirection(value);
        }

        value = from.getFetchSize();
        if (to.getFetchSize() != value) {
            to.setFetchSize(value);
        }

        value = from.getMaxFieldSize();
        if (to.getMaxFieldSize() != value) {
            to.setMaxFieldSize(value);
        }

        try {
            long l = from.getLargeMaxRows();
            if (to.getLargeMaxRows() != l) {
                to.setLargeMaxRows(l);
            }
        } catch (UnsupportedOperationException | SQLException e) {
            value = from.getMaxRows();
            if (to.getMaxRows() != value) {
                to.setMaxRows(value);
            }
        }

        final int originalQueryTimeout = to.getQueryTimeout();
        value = from.getQueryTimeout();
        if (originalQueryTimeout != value) {
            to.setQueryTimeout(value);
            log.debug("Changed query timeout for [%s] from %d to %d seconds", to, originalQueryTimeout, value);
        }
    }

    protected final ConnectionManager manager;

    private final AtomicReference<SQLWarning> warning;

    protected WrappedConnection(Connection conn) throws SQLException {
        this(HelpDriverExtension.ActivityListener.defaultDriverInfo, conn, conn.getMetaData().getURL());
    }

    protected WrappedConnection(DriverInfo info, Connection conn, String url) {
        if (conn instanceof WrappedConnection) {
            WrappedConnection wrappedConn = (WrappedConnection) conn;
            this.manager = wrappedConn.manager;
            this.warning = wrappedConn.warning;
        } else {
            this.manager = new ConnectionManager(info.configManager, info.getExtensions(), info.extension, conn, url,
                    info.extensionProps, info.normalizedInfo, info.mergedInfo);
            this.warning = new AtomicReference<>();
        }
    }

    protected WrappedConnection(DriverInfo info) throws SQLException {
        this.manager = new ConnectionManager(info);
        this.warning = new AtomicReference<>();
    }

    protected List<String> handleResults(String query, WrappedStatement stmt) throws SQLException {
        SQLWarning w = null;
        try (QueryContext context = manager.createContext()) {
            final ParsedQuery pq = QueryParser.parse(query, manager.getVariableTag(), context.getVariables());
            final QueryBuilder builder = new QueryBuilder(context, pq, manager, stmt.queryResult);
            final List<String> queries = builder.build();
            if (queries.isEmpty()) {
                return Collections.emptyList();
            } else if (manager.isUsingDatabaseExtension()) {
                return Collections.unmodifiableList(queries);
            }

            w = builder.getLastWarning();

            final VariableTag tag = manager.getVariableTag();
            final JdbcActivityListener listener = manager.createDefaultListener(context);
            final int size = queries.size();
            Properties props = manager.getExtensionProperties();
            if (size == 1) {
                return Collections
                        .singletonList(ConnectionManager.normalize(
                                ConnectionManager.convertTo(listener.onQuery(queries.get(0)), String.class), tag,
                                props));
            }

            // not worthy of running in parallel?
            List<String> results = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                String q = queries.get(i);
                results.add(ConnectionManager.normalize(ConnectionManager.convertTo(listener.onQuery(q), String.class),
                        tag, props));
            }
            return Collections.unmodifiableList(results);
        } catch (SQLWarning e) {
            stmt.queryResult.setWarnings(SqlExceptionUtils.consolidate(w, e));
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
        final VariableTag tag = manager.getVariableTag();
        try (QueryContext context = manager.createContext()) {
            final ParsedQuery pq = QueryParser.parse(query, tag, context.getVariables());
            final String[] parts = pq.getStaticParts().toArray(Constants.EMPTY_STRING_ARRAY);
            DriverExtension defaultExtension = manager.getDefaultExtension();
            Properties props = manager.getExtensionProperties();
            Properties originalProps = manager.getOriginalProperties();
            for (ExecutableBlock block : pq.getExecutableBlocks()) {
                DriverExtension ext = Checker.isNullOrEmpty(block.getExtensionName()) ? defaultExtension
                        : manager.getExtension(block.getExtensionName());
                if (ext == null) {
                    ext = defaultExtension;
                    final String msg = "Extension \"%s\" is not supported, using default extension [%s] instead";
                    log.warn(msg, block.getExtensionName(), ext);
                    ref.set(SqlExceptionUtils.consolidate(w,
                            new SQLWarning(Utils.format(msg, block.getExtensionName(), ext))));
                }

                Properties p = new Properties(
                        ext == defaultExtension ? props : DriverExtension.extractProperties(ext, originalProps));
                p.putAll(context.getMergedVariables());
                for (Entry<Object, Object> entry : block.getProperties().entrySet()) {
                    String key = Utils.applyVariables(entry.getKey().toString(), tag, p);
                    String val = Utils.applyVariables(entry.getValue().toString(), tag, p);
                    p.setProperty(key, val);
                }
                JdbcActivityListener cl = manager.createListener(ext, context, manager.getConnection(), p); // NOSONAR
                if (block.hasOutput()) {
                    try {
                        parts[block.getIndex()] = ConnectionManager.normalize(ConnectionManager.convertTo(
                                cl.onQuery(Utils.applyVariables(block.getContent(), tag, context.getVariables())),
                                String.class), tag, p);
                    } catch (SQLWarning e) {
                        ref.set(w = SqlExceptionUtils.consolidate(w, e));
                        parts[block.getIndex()] = block.getContent();
                    }
                } else {
                    cl.onQuery(block.getContent());
                }
            }

            final String substitutedQuery = Utils.applyVariables(String.join(Constants.EMPTY_STRING, parts), tag,
                    context.getVariables());
            log.debug("Original Query: [%s]\r\nSubstituted Query: [%s]", query, substitutedQuery);
            return ConnectionManager.convertTo(manager.createDefaultListener(context).onQuery(substitutedQuery),
                    String.class);
        } catch (SQLWarning e) {
            ref.set(SqlExceptionUtils.consolidate(w, e));
            return query;
        } catch (Throwable t) { // NOSONAR
            throw SqlExceptionUtils.clientError(t);
        }
    }

    @Override
    public final ConnectionManager getManager() {
        return manager;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        Connection conn = manager.getConnection();
        return conn.getClass() == iface ? iface.cast(conn) : conn.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        Connection conn = manager.getConnection();
        return conn.getClass() == iface || conn.isWrapperFor(iface);
    }

    @Override
    public WrappedStatement createStatement() throws SQLException {
        return new WrappedStatement(this, manager.getConnection().createStatement(), false); // NOSONAR
    }

    @Override
    public WrappedPreparedStatement prepareStatement(String query) throws SQLException {
        return new WrappedPreparedStatement(this, query, manager.getConnection().prepareStatement(handleString(query)), // NOSONAR
                false);
    }

    @Override
    public WrappedCallableStatement prepareCall(String query) throws SQLException {
        return new WrappedCallableStatement(this, query, manager.getConnection().prepareCall(handleString(query)), // NOSONAR
                false);
    }

    @Override
    public String nativeSQL(String query) throws SQLException {
        return manager.getConnection().nativeSQL(handleString(query));
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        manager.getConnection().setAutoCommit(autoCommit);
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return manager.getConnection().getAutoCommit();
    }

    @Override
    public void commit() throws SQLException {
        manager.getConnection().commit();
    }

    @Override
    public void rollback() throws SQLException {
        manager.getConnection().rollback();
    }

    @Override
    public void close() throws SQLException {
        warning.set(null);
        manager.getConnection().close();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return manager.getConnection().isClosed();
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return manager.getMetaData((DatabaseMetaData) null);
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        manager.getConnection().setReadOnly(readOnly);
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return manager.getConnection().isReadOnly();
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        manager.getConnection().setCatalog(catalog);
    }

    @Override
    public String getCatalog() throws SQLException {
        return manager.getConnection().getCatalog();
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        manager.getConnection().setTransactionIsolation(level);
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return manager.getConnection().getTransactionIsolation();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        SQLWarning w = warning.get();
        if (w != null) {
            SQLWarning next = null;
            try {
                next = manager.getConnection().getWarnings();
            } catch (Exception e) {
                // ignore
            }
            return SqlExceptionUtils.consolidate(w, next);
        }
        return manager.getConnection().getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
        warning.set(null);
        manager.getConnection().clearWarnings();
    }

    @Override
    public WrappedStatement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return new WrappedStatement(this, manager.getConnection().createStatement(resultSetType, resultSetConcurrency), // NOSONAR
                false);
    }

    @Override
    public WrappedPreparedStatement prepareStatement(String query, int resultSetType, int resultSetConcurrency)
            throws SQLException {
        return new WrappedPreparedStatement(this, query,
                manager.getConnection().prepareStatement(handleString(query), resultSetType, resultSetConcurrency), // NOSONAR
                false);
    }

    @Override
    public WrappedCallableStatement prepareCall(String query, int resultSetType, int resultSetConcurrency)
            throws SQLException {
        return new WrappedCallableStatement(this, query,
                manager.getConnection().prepareCall(handleString(query), resultSetType, resultSetConcurrency), false); // NOSONAR
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return manager.getConnection().getTypeMap();
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        manager.getConnection().setTypeMap(map);
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        manager.getConnection().setHoldability(holdability);
    }

    @Override
    public int getHoldability() throws SQLException {
        return manager.getConnection().getHoldability();
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        return manager.getConnection().setSavepoint();
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        return manager.getConnection().setSavepoint(name);
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        manager.getConnection().rollback(savepoint);
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        manager.getConnection().releaseSavepoint(savepoint);
    }

    @Override
    public WrappedStatement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        return new WrappedStatement(this,
                manager.getConnection().createStatement(resultSetType, resultSetConcurrency, resultSetHoldability), // NOSONAR
                false);
    }

    @Override
    public WrappedPreparedStatement prepareStatement(String query, int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException {
        return new WrappedPreparedStatement(this, query,
                manager.getConnection().prepareStatement(handleString(query), resultSetType, resultSetConcurrency, // NOSONAR
                        resultSetHoldability),
                false);
    }

    @Override
    public WrappedCallableStatement prepareCall(String query, int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException {
        return new WrappedCallableStatement(this, query,
                manager.getConnection().prepareCall(handleString(query), resultSetType, resultSetConcurrency, // NOSONAR
                        resultSetHoldability),
                false);
    }

    @Override
    public WrappedPreparedStatement prepareStatement(String query, int autoGeneratedKeys) throws SQLException {
        return new WrappedPreparedStatement(this, query,
                manager.getConnection().prepareStatement(handleString(query), autoGeneratedKeys), false); // NOSONAR
    }

    @Override
    public WrappedPreparedStatement prepareStatement(String query, int[] columnIndexes) throws SQLException {
        return new WrappedPreparedStatement(this, query,
                manager.getConnection().prepareStatement(handleString(query), columnIndexes), false); // NOSONAR
    }

    @Override
    public WrappedPreparedStatement prepareStatement(String query, String[] columnNames) throws SQLException {
        return new WrappedPreparedStatement(this, query,
                manager.getConnection().prepareStatement(handleString(query), columnNames), false); // NOSONAR
    }

    @Override
    public Clob createClob() throws SQLException {
        return manager.getConnection().createClob();
    }

    @Override
    public Blob createBlob() throws SQLException {
        return manager.getConnection().createBlob();
    }

    @Override
    public NClob createNClob() throws SQLException {
        return manager.getConnection().createNClob();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        return manager.getConnection().createSQLXML();
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        return manager.getConnection().isValid(timeout);
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        manager.getConnection().setClientInfo(name, value);
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        manager.getConnection().setClientInfo(properties);
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        return manager.getConnection().getClientInfo(name);
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return manager.getConnection().getClientInfo();
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return manager.getConnection().createArrayOf(typeName, elements);
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        return manager.getConnection().createStruct(typeName, attributes);
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        manager.getConnection().setSchema(schema);
    }

    @Override
    public String getSchema() throws SQLException {
        return manager.getConnection().getSchema();
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        manager.getConnection().abort(executor);
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        manager.getConnection().setNetworkTimeout(executor, milliseconds);
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        return manager.getConnection().getNetworkTimeout();
    }
}
