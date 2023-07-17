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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This class serves as a wrapper for a {@link Statement}.
 */
public class WrappedStatement implements Statement {
    protected final WrappedConnection conn;

    private final Statement stmt;

    private final AtomicReference<SQLWarning> warning;

    protected WrappedStatement(WrappedConnection conn, Statement stmt) {
        this.conn = conn;
        this.stmt = stmt;

        this.warning = new AtomicReference<>();
    }

    protected String handle(String query) throws SQLException {
        return conn.handle(query, warning);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return stmt.getClass() == iface ? iface.cast(iface) : stmt.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return stmt.getClass() == iface || stmt.isWrapperFor(iface);
    }

    @Override
    public ResultSet executeQuery(String query) throws SQLException {
        return new WrappedResultSet(this, stmt.executeQuery(handle(query)));
    }

    @Override
    public int executeUpdate(String query) throws SQLException {
        return stmt.executeUpdate(handle(query));
    }

    @Override
    public void close() throws SQLException {
        stmt.close();
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        return stmt.getMaxFieldSize();
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        stmt.setMaxFieldSize(max);
    }

    @Override
    public int getMaxRows() throws SQLException {
        return stmt.getMaxRows();
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        stmt.setMaxRows(max);
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        stmt.setEscapeProcessing(enable);
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return stmt.getQueryTimeout();
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        stmt.setQueryTimeout(seconds);
    }

    @Override
    public void cancel() throws SQLException {
        stmt.cancel();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        SQLWarning w = warning.get();
        return w != null ? w : stmt.getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
        warning.set(null);
        stmt.clearWarnings();
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        stmt.setCursorName(name);
    }

    @Override
    public boolean execute(String query) throws SQLException {
        return stmt.execute(handle(query));
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        final ResultSet rs = stmt.getResultSet();
        return rs != null ? new WrappedResultSet(this, rs) : rs;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return stmt.getUpdateCount();
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return stmt.getMoreResults();
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        stmt.setFetchDirection(direction);
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return stmt.getFetchDirection();
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        stmt.setFetchSize(rows);
    }

    @Override
    public int getFetchSize() throws SQLException {
        return stmt.getFetchSize();
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return stmt.getResultSetConcurrency();
    }

    @Override
    public int getResultSetType() throws SQLException {
        return stmt.getResultSetType();
    }

    @Override
    public void addBatch(String query) throws SQLException {
        stmt.addBatch(handle(query));
    }

    @Override
    public void clearBatch() throws SQLException {
        stmt.clearBatch();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        return stmt.executeBatch();
    }

    @Override
    public Connection getConnection() throws SQLException {
        return conn;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        return stmt.getMoreResults(current);
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        return new WrappedResultSet(this, stmt.getGeneratedKeys());
    }

    @Override
    public int executeUpdate(String query, int autoGeneratedKeys) throws SQLException {
        return stmt.executeUpdate(handle(query), autoGeneratedKeys);
    }

    @Override
    public int executeUpdate(String query, int[] columnIndexes) throws SQLException {
        return stmt.executeUpdate(handle(query), columnIndexes);
    }

    @Override
    public int executeUpdate(String query, String[] columnNames) throws SQLException {
        return stmt.executeUpdate(handle(query), columnNames);
    }

    @Override
    public boolean execute(String query, int autoGeneratedKeys) throws SQLException {
        return stmt.execute(handle(query), autoGeneratedKeys);
    }

    @Override
    public boolean execute(String query, int[] columnIndexes) throws SQLException {
        return stmt.execute(handle(query), columnIndexes);
    }

    @Override
    public boolean execute(String query, String[] columnNames) throws SQLException {
        return stmt.execute(handle(query), columnNames);
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return stmt.getResultSetHoldability();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return stmt.isClosed();
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        stmt.setPoolable(poolable);
    }

    @Override
    public boolean isPoolable() throws SQLException {
        return stmt.isPoolable();
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        stmt.closeOnCompletion();
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return stmt.isCloseOnCompletion();
    }
}
