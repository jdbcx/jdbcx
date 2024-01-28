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

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;

import javax.sql.DataSource;

/**
 * This class serves as a wrapper for a {@link DataSource}.
 */
public class WrappedDataSource implements DataSource {
    public static WrappedDataSource of(DataSource ds) {
        if (ds == null) {
            throw new NullPointerException("Non-null datasource is required");
        }

        return ds instanceof WrappedDataSource ? (WrappedDataSource) ds : new WrappedDataSource(ds);
    }

    protected final DataSource ds;

    protected WrappedDataSource(DataSource ds) {
        this.ds = ds;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return ds.getParentLogger();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return ds.getClass() == iface ? iface.cast(ds) : ds.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return ds.getClass() == iface || ds.isWrapperFor(iface);
    }

    @Override
    public Connection getConnection() throws SQLException {
        return new WrappedConnection(ds.getConnection());
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return new WrappedConnection(ds.getConnection(username, password));
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
