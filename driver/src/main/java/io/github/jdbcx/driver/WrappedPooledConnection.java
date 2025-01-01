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

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.ConnectionEventListener;
import javax.sql.PooledConnection;
import javax.sql.StatementEventListener;

/**
 * This class serves as a wrapper for a {@link PooledConnection}.
 */
public class WrappedPooledConnection implements PooledConnection {
    private final PooledConnection conn;

    protected WrappedPooledConnection(PooledConnection conn) {
        this.conn = conn;
    }

    @Override
    public Connection getConnection() throws SQLException {
        return new WrappedConnection(conn.getConnection());
    }

    @Override
    public void close() throws SQLException {
        conn.close();
    }

    @Override
    public void addConnectionEventListener(ConnectionEventListener listener) {
        conn.addConnectionEventListener(listener);
    }

    @Override
    public void removeConnectionEventListener(ConnectionEventListener listener) {
        conn.removeConnectionEventListener(listener);
    }

    @Override
    public void addStatementEventListener(StatementEventListener listener) {
        conn.addStatementEventListener(listener);
    }

    @Override
    public void removeStatementEventListener(StatementEventListener listener) {
        conn.removeStatementEventListener(listener);
    }
}
