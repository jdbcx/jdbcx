/*
 * Copyright 2022-2024, Zhichun Wu
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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import io.github.jdbcx.Logger;
import io.github.jdbcx.LoggerFactory;
import io.github.jdbcx.executor.jdbc.CombinedResultSet;
import io.github.jdbcx.executor.jdbc.SqlExceptionUtils;

/**
 * This class serves as a wrapper for a {@link Statement}.
 */
public class WrappedStatement implements Statement {
    private static final Logger log = LoggerFactory.getLogger(WrappedStatement.class);

    protected final WrappedConnection conn;

    private final Statement stmt;

    protected final AtomicReference<SQLWarning> warning;

    protected final QueryResult queryResult;

    protected WrappedStatement(WrappedConnection conn, Statement stmt) {
        this.conn = conn;
        this.stmt = stmt;

        this.warning = new AtomicReference<>();
        this.queryResult = new QueryResult(this.warning);
    }

    protected ResultSet getGeneratedKeysSafely() throws SQLException {
        ResultSet rs = null;
        try {
            rs = stmt.getGeneratedKeys();
        } catch (SQLFeatureNotSupportedException e) {
            log.debug("Failed to get generated keys due to lack of support from the driver");
        }
        return rs;
    }

    protected boolean execute(String query, ExecuteCallback callback) throws SQLException {
        queryResult.reset();

        final List<String> queries = conn.handleResults(query, this);
        if (queries.isEmpty()) {
            return queryResult.hasResultSet();
        }

        boolean result = false;
        int affectedRows = 0;
        ResultSet[] rs = null;
        try {
            int size = queries.size();
            if (size == 1) {
                String newQuery = queries.get(0);
                log.debug("Executing update: [%s]", newQuery);
                result = stmt.execute(newQuery);
            } else {
                ResultSet[] keys = new ResultSet[size];
                ResultSet[] results = new ResultSet[size];
                rs = new ResultSet[] { new CombinedResultSet(keys), new CombinedResultSet(results) }; // NOSONAR
                for (int i = 0; i < size; i++) {
                    String q = queries.get(i);
                    log.debug("Executing %d of %d: [%s]", i + 1, size, q);
                    if (result |= callback.execute(q)) {
                        results[i] = stmt.getResultSet();
                    } else {
                        affectedRows += stmt.getUpdateCount();
                        keys[i] = getGeneratedKeysSafely();
                    }
                }
                queryResult.updateAll(rs[0], rs[1], affectedRows);
            }
            return result;
        } catch (Exception e) {
            log.error("Failed to invoke execute(*): %s", queries);
            if (rs != null) {
                try {
                    new CombinedResultSet(rs).close();
                } catch (Exception ex) {
                    log.debug("Failed to close result set due to %s", ex.getMessage());
                }
            }
            throw SqlExceptionUtils.handle(e);
        }
    }

    protected int executeUpdate(String query, ExecuteCallback callback) throws SQLException {
        queryResult.reset();

        final List<String> queries = conn.handleResults(query, this);
        if (queries.isEmpty()) {
            return 0;
        }

        int affectedRows = 0;
        ResultSet rs = null;
        try {
            int size = queries.size();
            if (size == 1) {
                String newQuery = queries.get(0);
                log.debug("Executing update: [%s]", newQuery);
                affectedRows = callback.executeUpdate(newQuery);
            } else {
                ResultSet[] arr = new ResultSet[size];
                rs = new CombinedResultSet(arr);
                for (int i = 0; i < size; i++) {
                    String q = queries.get(i);
                    log.debug("Executing update %d of %d: [%s]", i + 1, size, q);
                    affectedRows += callback.executeUpdate(q);
                    arr[i] = getGeneratedKeysSafely();
                }
            }
            queryResult.updateKeys(rs, affectedRows);
            return affectedRows;
        } catch (Exception e) {
            log.error("Failed to invoke executeUpdate(*): %s", queries);
            if (rs != null) {
                try {
                    rs.close();
                } catch (Exception ex) {
                    log.debug("Failed to close result set due to %s", ex.getMessage());
                }
            }
            throw SqlExceptionUtils.handle(e);
        }
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
        queryResult.reset();

        final List<String> queries = conn.handleResults(query, this);
        if (queries.isEmpty()) {
            return queryResult.getResultSet();
        }

        final int size = queries.size();
        ResultSet rs = null;
        try {
            if (size == 1) {
                String newQuery = queries.get(0);
                log.debug("Executing query: [%s]", newQuery);
                rs = new WrappedResultSet(this, stmt.executeQuery(newQuery));
            } else {
                ResultSet[] arr = new ResultSet[size];
                rs = new CombinedResultSet(arr);
                for (int i = 0; i < size; i++) {
                    String q = queries.get(i);
                    log.debug("Executing query %d of %d: [%s]", i + 1, size, q);
                    arr[i] = stmt.executeQuery(q);
                }
            }
            queryResult.updateResult(rs);
            return rs;
        } catch (Exception e) {
            log.error("Failed to invoke executeQuery(String query): %s", queries);
            if (rs != null) {
                try {
                    rs.close();
                } catch (Exception ex) {
                    log.debug("Failed to close result set due to %s", ex.getMessage());
                }
            }
            throw SqlExceptionUtils.handle(e);
        }
    }

    @Override
    public int executeUpdate(String query) throws SQLException {
        return executeUpdate(query, new BaseExecuteCallback(stmt));
    }

    @Override
    public void close() throws SQLException {
        queryResult.reset();

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
        SQLWarning w = queryResult.getWarnings();
        if (w != null) {
            SQLWarning next = null;
            try {
                next = stmt.getWarnings();
            } catch (Exception e) {
                // ignore
            }
            return SqlExceptionUtils.consolidate(w, next);
        }
        return stmt.getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
        queryResult.setWarnings(null);
        stmt.clearWarnings();
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        stmt.setCursorName(name);
    }

    @Override
    public boolean execute(String query) throws SQLException {
        return execute(query, new BaseExecuteCallback(stmt));
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        ResultSet rs = queryResult.getResultSet();
        return rs != null ? rs : new WrappedResultSet(this, stmt.getResultSet());
    }

    @Override
    public int getUpdateCount() throws SQLException {
        Integer i = queryResult.getUpdateCount();
        return i != null ? i.intValue() : stmt.getUpdateCount();
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
        final String newQuery = conn.handleString(query, warning);
        try {
            stmt.addBatch(newQuery);
        } catch (Exception e) {
            log.error("Failed to invoke addBatch(String query): [%s]", newQuery);
            throw e;
        }
    }

    @Override
    public void clearBatch() throws SQLException {
        stmt.clearBatch();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        queryResult.reset();

        return stmt.executeBatch();
    }

    @Override
    public WrappedConnection getConnection() throws SQLException {
        return conn;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        return stmt.getMoreResults(current);
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        ResultSet rs = queryResult.getGeneratedKeys();
        return rs != null ? rs : new WrappedResultSet(this, stmt.getGeneratedKeys());
    }

    @Override
    public int executeUpdate(String query, int autoGeneratedKeys) throws SQLException {
        return executeUpdate(query, new ExecuteWithFlag(stmt, autoGeneratedKeys));
    }

    @Override
    public int executeUpdate(String query, int[] columnIndexes) throws SQLException {
        return executeUpdate(query, new ExecuteWithIndex(stmt, columnIndexes));
    }

    @Override
    public int executeUpdate(String query, String[] columnNames) throws SQLException {
        return executeUpdate(query, new ExecuteWithNames(stmt, columnNames));
    }

    @Override
    public boolean execute(String query, int autoGeneratedKeys) throws SQLException {
        return execute(query, new ExecuteWithFlag(stmt, autoGeneratedKeys));
    }

    @Override
    public boolean execute(String query, int[] columnIndexes) throws SQLException {
        return execute(query, new ExecuteWithIndex(stmt, columnIndexes));
    }

    @Override
    public boolean execute(String query, String[] columnNames) throws SQLException {
        return execute(query, new ExecuteWithNames(stmt, columnNames));
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
