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
package io.github.jdbcx.driver.impl;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import io.github.jdbcx.JdbcActivityListener;
import io.github.jdbcx.Logger;
import io.github.jdbcx.LoggerFactory;
import io.github.jdbcx.QueryContext;
import io.github.jdbcx.Result;
import io.github.jdbcx.driver.ConnectionManager;
import io.github.jdbcx.driver.ParsedQuery;
import io.github.jdbcx.driver.QueryBuilder;
import io.github.jdbcx.driver.QueryParser;
import io.github.jdbcx.driver.QueryResult;
import io.github.jdbcx.executor.jdbc.CombinedResultSet;
import io.github.jdbcx.executor.jdbc.SqlExceptionUtils;

/**
 * This class serves as a wrapper for a {@link Statement}.
 */
public class DefaultStatement extends DefaultResource implements Statement {
    private static final Logger log = LoggerFactory.getLogger(DefaultStatement.class);

    protected final AtomicBoolean closeOnCompletion;
    protected final AtomicBoolean escapeScanning;
    protected final AtomicInteger maxFieldSize;
    protected final AtomicInteger maxRows;
    protected final AtomicBoolean poolable;
    protected final AtomicInteger queryTimeout;
    protected final int rsConcurrency;
    protected final int rsHoldability;
    protected final int rsType;
    protected final AtomicInteger rsFetchDirection;

    protected final List<ResultSet> results;

    protected final ConnectionManager manager;

    protected final QueryResult queryResult;

    protected DefaultStatement(DefaultConnection conn, int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) {
        super(conn);

        this.closeOnCompletion = new AtomicBoolean();
        this.escapeScanning = new AtomicBoolean(true);
        this.maxFieldSize = new AtomicInteger();
        this.maxRows = new AtomicInteger();
        this.poolable = new AtomicBoolean();
        this.queryTimeout = new AtomicInteger();

        this.rsConcurrency = resultSetConcurrency;
        this.rsHoldability = resultSetHoldability;
        this.rsType = resultSetType;
        this.rsFetchDirection = new AtomicInteger(ResultSet.FETCH_FORWARD);

        this.results = new LinkedList<>();

        this.manager = conn.manager;
        this.queryResult = new QueryResult(warning);
    }

    protected List<String> handleResults(String query) throws SQLException {
        SQLWarning w = null;
        try (QueryContext context = manager.createContext()) {
            final ParsedQuery pq = QueryParser.parse(query, context.getVariables());
            final QueryBuilder builder = new QueryBuilder(context, pq, manager, queryResult);
            List<String> queries = builder.build();
            if (queries.isEmpty()) {
                return Collections.emptyList();
            }

            w = builder.getLastWarning();

            final JdbcActivityListener listener = manager.createListener(context);
            final int size = queries.size();
            Properties props = manager.getExtensionProperties();
            if (size == 1) {
                return Collections
                        .singletonList(ConnectionManager.normalize(
                                ConnectionManager.convertTo(listener.onQuery(queries.get(0)), String.class), props));
            }

            // not worthy of running in parallel?
            List<String> list = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                String q = queries.get(i);
                list.add(ConnectionManager.normalize(ConnectionManager.convertTo(listener.onQuery(q), String.class),
                        props));
            }
            return Collections.unmodifiableList(list);
        } catch (SQLWarning e) {
            queryResult.setWarnings(SqlExceptionUtils.consolidate(w, e));
            return Collections.singletonList(query);
        } catch (Throwable t) { // NOSONAR
            throw SqlExceptionUtils.clientError(t);
        }
    }

    protected void resetCurrentResults() {
        queryResult.reset();
    }

    @Override
    protected void reset() {
        resetCurrentResults();
        super.reset();
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        return execute(sql) ? getResultSet() : new CombinedResultSet();
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        return execute(sql) ? 0 : getUpdateCount();
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        ensureOpen();
        return maxFieldSize.get();
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        ensureOpen();
        if (max < 0) {
            throw SqlExceptionUtils
                    .clientError("Max field size should be always greater than or equal to zero, but we got " + max);
        }
        maxFieldSize.set(max);
    }

    @Override
    public int getMaxRows() throws SQLException {
        ensureOpen();
        return maxRows.get();
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        ensureOpen();
        if (max < 0) {
            throw SqlExceptionUtils.clientError("Max rows must be greater than or equal to zero, but we got " + max);
        }
        maxRows.set(max);
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        ensureOpen();
        escapeScanning.set(enable);
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        ensureOpen();
        return queryTimeout.get();
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        ensureOpen();
        queryTimeout.set(seconds);
    }

    @Override
    public void cancel() throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'cancel'");
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'setCursorName'");
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        ensureOpen();
        queryResult.reset();

        final List<String> queries = handleResults(sql);
        if (queries.isEmpty()) {
            return true;
        }

        ResultSet rs = ConnectionManager.convertTo(Result.of(Result.DEFAULT_FIELDS, queries), ResultSet.class);
        queryResult.setResultSet(rs);
        return true;
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        ensureOpen();
        return queryResult.getResultSet();
    }

    @Override
    public int getUpdateCount() throws SQLException {
        ensureOpen();

        Integer i = this.queryResult.getUpdateCount();
        return i != null ? i.intValue() : -1;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        ensureOpen();
        return false;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        ensureOpen();
        if (direction != ResultSet.FETCH_FORWARD && direction != ResultSet.FETCH_REVERSE
                && direction != ResultSet.FETCH_UNKNOWN) {
            throw SqlExceptionUtils.clientError("Invalid fetch direction: " + direction);
        }
        rsFetchDirection.set(direction);
    }

    @Override
    public int getFetchDirection() throws SQLException {
        ensureOpen();
        return rsFetchDirection.get();
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'setFetchSize'");
    }

    @Override
    public int getFetchSize() throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getFetchSize'");
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        ensureOpen();
        return rsConcurrency;
    }

    @Override
    public int getResultSetType() throws SQLException {
        ensureOpen();
        return rsType;
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'addBatch'");
    }

    @Override
    public void clearBatch() throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'clearBatch'");
    }

    @Override
    public int[] executeBatch() throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'executeBatch'");
    }

    @Override
    public DefaultConnection getConnection() throws SQLException {
        ensureOpen();
        return (DefaultConnection) parent;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        ensureOpen();
        return false;
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        ensureOpen();
        return queryResult.getGeneratedKeys();
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return executeUpdate(sql);
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        return executeUpdate(sql);
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        return executeUpdate(sql);
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        return execute(sql);
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        return execute(sql);
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        return execute(sql);
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        ensureOpen();
        return rsHoldability;
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        ensureOpen();
        this.poolable.set(poolable);
    }

    @Override
    public boolean isPoolable() throws SQLException {
        ensureOpen();
        return poolable.get();
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        ensureOpen();
        closeOnCompletion.set(true);
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        ensureOpen();
        return closeOnCompletion.get();
    }
}
