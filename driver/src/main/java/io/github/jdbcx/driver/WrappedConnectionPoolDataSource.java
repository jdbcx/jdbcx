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

import java.io.PrintWriter;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;

import javax.sql.ConnectionPoolDataSource;
import javax.sql.PooledConnection;
import javax.sql.XAConnection;

/**
 * This class serves as a wrapper for a {@link ConnectionPoolDataSource}.
 */
public class WrappedConnectionPoolDataSource implements ConnectionPoolDataSource {
    public static WrappedConnectionPoolDataSource of(ConnectionPoolDataSource ds) {
        if (ds == null) {
            throw new NullPointerException("Non-null connection pool datasource is required");
        }

        return ds instanceof WrappedConnectionPoolDataSource ? (WrappedConnectionPoolDataSource) ds
                : new WrappedConnectionPoolDataSource(ds);
    }

    protected final ConnectionPoolDataSource ds;

    protected WrappedConnectionPoolDataSource(ConnectionPoolDataSource ds) {
        this.ds = ds;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return ds.getParentLogger();
    }

    @Override
    public PooledConnection getPooledConnection() throws SQLException {
        PooledConnection conn = ds.getPooledConnection();
        return conn instanceof XAConnection ? new WrappedXAConnection((XAConnection) conn)
                : new WrappedPooledConnection(conn);
    }

    @Override
    public PooledConnection getPooledConnection(String user, String password) throws SQLException {
        PooledConnection conn = ds.getPooledConnection(user, password);
        return conn instanceof XAConnection ? new WrappedXAConnection((XAConnection) conn)
                : new WrappedPooledConnection(conn);
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return ds.getLogWriter();
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        ds.setLogWriter(out);
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        ds.setLoginTimeout(seconds);
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return ds.getLoginTimeout();
    }
}
