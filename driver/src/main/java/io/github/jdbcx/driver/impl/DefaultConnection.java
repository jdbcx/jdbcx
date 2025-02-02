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
package io.github.jdbcx.driver.impl;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import io.github.jdbcx.ConfigManager;
import io.github.jdbcx.Constants;
import io.github.jdbcx.DriverExtension;
import io.github.jdbcx.Logger;
import io.github.jdbcx.LoggerFactory;
import io.github.jdbcx.driver.ConnectionManager;
import io.github.jdbcx.driver.DefaultDriverExtension;
import io.github.jdbcx.driver.ManagedConnection;
import io.github.jdbcx.executor.jdbc.SqlExceptionUtils;

public final class DefaultConnection extends DefaultResource implements ManagedConnection {
    private static final Logger log = LoggerFactory.getLogger(DefaultConnection.class);

    static final String ERROR_UNIMPLEMENTED_CREATE_STMT = "Unimplemented method 'createStatement'";
    static final String ERROR_UNIMPLEMENTED_PREPARE_CALL = "Unimplemented method 'prepareCall'";
    static final String ERROR_UNIMPLEMENTED_PREPARE_STMT = "Unimplemented method 'prepareStatement'";

    protected final AtomicBoolean autoCommit;
    protected final AtomicInteger networkTimeout;
    protected final AtomicBoolean readOnly;
    protected final AtomicInteger rsConcurrency;
    protected final AtomicInteger rsHoldability;
    protected final AtomicInteger rsType;
    protected final AtomicInteger txIsoLevel;

    protected final AtomicReference<String> catalog;
    protected final AtomicReference<String> schema;

    protected final ConnectionManager manager;

    final String url;

    @Override
    protected void reset() {
        this.autoCommit.set(true);
        this.rsConcurrency.set(ResultSet.CONCUR_READ_ONLY);
        this.rsHoldability.set(ResultSet.HOLD_CURSORS_OVER_COMMIT);
        this.rsType.set(ResultSet.TYPE_FORWARD_ONLY);
        this.readOnly.set(false);
        this.txIsoLevel.set(TRANSACTION_READ_COMMITTED);

        super.reset();
    }

    public DefaultConnection(ConfigManager configManager, Map<String, DriverExtension> extensions,
            DriverExtension defaultExtension, String url, Properties extensionProps, Properties normalizedProps,
            Properties originalProps) {
        super(null);

        this.autoCommit = new AtomicBoolean(true);
        this.networkTimeout = new AtomicInteger();
        this.rsConcurrency = new AtomicInteger(ResultSet.CONCUR_READ_ONLY);
        this.rsHoldability = new AtomicInteger(ResultSet.HOLD_CURSORS_OVER_COMMIT);
        this.rsType = new AtomicInteger(ResultSet.TYPE_FORWARD_ONLY);
        this.readOnly = new AtomicBoolean();
        this.txIsoLevel = new AtomicInteger(TRANSACTION_READ_COMMITTED);

        this.catalog = new AtomicReference<>(defaultExtension.getName());
        this.schema = new AtomicReference<>();

        this.manager = new ConnectionManager(configManager, extensions, defaultExtension, this, url, extensionProps,
                normalizedProps, url, originalProps);

        this.url = url;
    }

    @Override
    public ConnectionManager getManager() {
        return manager;
    }

    @Override
    public Statement createStatement() throws SQLException {
        ensureOpen();

        return add(new DefaultStatement(this, rsType.get(), rsConcurrency.get(), rsHoldability.get()));
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException(ERROR_UNIMPLEMENTED_PREPARE_STMT);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException(ERROR_UNIMPLEMENTED_PREPARE_CALL);
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'nativeSQL'");
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        ensureOpen();
        this.autoCommit.set(autoCommit);
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        ensureOpen();
        return this.autoCommit.get();
    }

    @Override
    public void commit() throws SQLException {
        ensureOpen();
        if (autoCommit.get()) {
            throw SqlExceptionUtils.clientError("Please disable auto commit before manual commit");
        }
    }

    @Override
    public void rollback() throws SQLException {
        ensureOpen();
        if (autoCommit.get()) {
            throw SqlExceptionUtils.clientError("Please disable auto commit before manual roll back");
        }
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        ensureOpen();

        return manager.getMetaData(new DefaultDatabaseMetaData(this));
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        ensureOpen();
        this.readOnly.set(readOnly);
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        ensureOpen();
        return this.readOnly.get();
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        ensureOpen();

        if (DefaultDriverExtension.getInstance().getName().equals(catalog)
                || manager.getExtension(catalog) != null) {
            this.catalog.set(catalog);
        }
    }

    @Override
    public String getCatalog() throws SQLException {
        ensureOpen();
        return catalog.get();
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        ensureOpen();
        this.txIsoLevel.set(level);
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        ensureOpen();
        return this.txIsoLevel.get();
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException(ERROR_UNIMPLEMENTED_CREATE_STMT);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
            throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException(ERROR_UNIMPLEMENTED_PREPARE_STMT);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException(ERROR_UNIMPLEMENTED_PREPARE_CALL);
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getTypeMap'");
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'setTypeMap'");
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        ensureOpen();
        this.rsHoldability.set(holdability);
    }

    @Override
    public int getHoldability() throws SQLException {
        ensureOpen();
        return this.rsHoldability.get();
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'setSavepoint'");
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'setSavepoint'");
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'rollback'");
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'releaseSavepoint'");
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException(ERROR_UNIMPLEMENTED_CREATE_STMT);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException(ERROR_UNIMPLEMENTED_PREPARE_STMT);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException(ERROR_UNIMPLEMENTED_PREPARE_CALL);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException(ERROR_UNIMPLEMENTED_PREPARE_STMT);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException(ERROR_UNIMPLEMENTED_PREPARE_STMT);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException(ERROR_UNIMPLEMENTED_PREPARE_STMT);
    }

    @Override
    public Clob createClob() throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'createClob'");
    }

    @Override
    public Blob createBlob() throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'createBlob'");
    }

    @Override
    public NClob createNClob() throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'createNClob'");
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'createSQLXML'");
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        if (closed.get()) {
            return false;
        }
        try (Statement stmt = createStatement();
                ResultSet rs = stmt.executeQuery(manager.getVariableTag().function(Constants.EMPTY_STRING))) {
            return rs.next();
        }
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'setClientInfo'");
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'setClientInfo'");
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getClientInfo'");
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getClientInfo'");
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'createArrayOf'");
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'createStruct'");
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        ensureOpen();
        this.schema.set(schema);
    }

    @Override
    public String getSchema() throws SQLException {
        ensureOpen();
        return schema.get();
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        if (executor == null) {
            throw SqlExceptionUtils.clientError("Non-null executor is required");
        }

        try {
            CompletableFuture.runAsync(() -> {
                for (DefaultResource stmt : children) {
                    log.warn("Cancelling statement [%s]", stmt);
                    try {
                        ((Statement) stmt).cancel();
                    } catch (SQLException e) {
                        throw new CompletionException(e);
                    }
                }
            }, executor);
        } finally {
            closed.set(true);
            reset();
        }
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        ensureOpen();
        if (executor == null || milliseconds < 0) {
            throw SqlExceptionUtils.clientError("Non-null executor and non-negative timeout required");
        }
        networkTimeout.set(milliseconds);
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        ensureOpen();
        return networkTimeout.get();
    }
}
