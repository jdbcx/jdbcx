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
package io.github.jdbcx;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;

import io.github.jdbcx.WrappedDriver.DriverInfo;
import io.github.jdbcx.impl.DefaultConnectionListener;
import io.github.jdbcx.impl.ExecutableBlock;
import io.github.jdbcx.impl.ParsedQuery;
import io.github.jdbcx.impl.QueryParser;

/**
 * This class serves as a wrapper for a {@link Connection}.
 */
public class WrappedConnection implements Connection {
    private static final Logger log = LoggerFactory.getLogger(WrappedConnection.class);

    protected final Connection conn;
    protected final String url;
    protected final Properties props;

    protected final ConnectionListener listener;

    protected final DriverExtension defaultExtension;
    protected final Map<String, DriverExtension> extensions;
    private final AtomicReference<SQLWarning> warning;

    protected WrappedConnection(Connection conn) {
        DriverInfo info = new DriverInfo();
        this.defaultExtension = info.extension;
        this.extensions = info.getExtensions();

        this.conn = conn;
        this.url = Constants.EMPTY_STRING;
        this.props = info.extensionProps;

        this.listener = DefaultConnectionListener.getInstance();

        this.warning = new AtomicReference<>();
    }

    protected WrappedConnection(DriverInfo info) throws SQLException {
        this.defaultExtension = info.extension;
        this.extensions = info.getExtensions();

        this.conn = info.driver.getActualDriver(info).connect(info.actualUrl, info.normalizedInfo);
        this.url = info.actualUrl;
        this.props = info.extensionProps;

        this.listener = info.extension != null ? info.extension.createListener(this.conn, this.url, this.props)
                : DefaultConnectionListener.getInstance();

        this.warning = new AtomicReference<>();
    }

    protected String handle(String query) throws SQLException {
        return handle(query, null);
    }

    protected String handle(String query, AtomicReference<SQLWarning> ref) throws SQLException {
        if (ref == null) {
            ref = warning;
        }
        ref.set(null);

        ParsedQuery pq = QueryParser.parse(query);
        String[] parts = pq.getQueryParts().toArray(Constants.EMPTY_STRING_ARRAY);
        for (ExecutableBlock block : pq.getExecutableBlocks()) {
            DriverExtension ext = Checker.isNullOrEmpty(block.getExtensionName()) ? defaultExtension
                    : extensions.get(block.getExtensionName());
            if (ext == null) {
                ext = defaultExtension;
                if (log.isWarnEnabled() && block.getExtensionName().equals(DriverInfo.getExtensionName(ext))) {
                    log.warn("Extension \"%s\" is not supported, use default extension [%s] instead",
                            block.getExtensionName(), ext);
                }
            }
            Properties p = block.getProperties();
            if (!p.isEmpty()) {
                p = new Properties(props);
                p.putAll(block.getProperties());
            }
            ConnectionListener cl = ext.createListener(conn, url, p);
            if (block.hasOutput()) {
                parts[block.getIndex()] = cl.onQuery(block.getContent());
            } else {
                cl.onQuery(block.getContent());
            }
        }

        final String substitutedQuery = String.join(Constants.EMPTY_STRING, parts);
        log.debug("Original Query: [%s]\r\nSubstituted Query: [%s]", query, substitutedQuery);

        try {
            return listener.onQuery(substitutedQuery);
        } catch (SQLWarning e) {
            ref.set(e);
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
    public Statement createStatement() throws SQLException {
        return new WrappedStatement(this, conn.createStatement()); // NOSONAR
    }

    @Override
    public PreparedStatement prepareStatement(String query) throws SQLException {
        return new WrappedPreparedStatement(this, query, conn.prepareStatement(handle(query))); // NOSONAR
    }

    @Override
    public CallableStatement prepareCall(String query) throws SQLException {
        return new WrappedCallableStatement(this, query, conn.prepareCall(handle(query))); // NOSONAR
    }

    @Override
    public String nativeSQL(String query) throws SQLException {
        return conn.nativeSQL(handle(query));
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
        conn.close();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return conn.isClosed();
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return listener.onMetaData(conn.getMetaData());
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
        return w != null ? w : conn.getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
        this.warning.set(null);
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
                conn.prepareStatement(handle(query), resultSetType, resultSetConcurrency)); // NOSONAR
    }

    @Override
    public CallableStatement prepareCall(String query, int resultSetType, int resultSetConcurrency)
            throws SQLException {
        return new WrappedCallableStatement(this, query,
                conn.prepareCall(handle(query), resultSetType, resultSetConcurrency)); // NOSONAR
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
                conn.prepareStatement(handle(query), resultSetType, resultSetConcurrency, resultSetHoldability)); // NOSONAR
    }

    @Override
    public CallableStatement prepareCall(String query, int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException {
        return new WrappedCallableStatement(this, query,
                conn.prepareCall(handle(query), resultSetType, resultSetConcurrency, resultSetHoldability)); // NOSONAR
    }

    @Override
    public PreparedStatement prepareStatement(String query, int autoGeneratedKeys) throws SQLException {
        return new WrappedPreparedStatement(this, query, conn.prepareStatement(handle(query), autoGeneratedKeys)); // NOSONAR
    }

    @Override
    public PreparedStatement prepareStatement(String query, int[] columnIndexes) throws SQLException {
        return new WrappedPreparedStatement(this, query, conn.prepareStatement(handle(query), columnIndexes)); // NOSONAR
    }

    @Override
    public PreparedStatement prepareStatement(String query, String[] columnNames) throws SQLException {
        return new WrappedPreparedStatement(this, query, conn.prepareStatement(handle(query), columnNames)); // NOSONAR
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
