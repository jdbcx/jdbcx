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

import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import io.github.jdbcx.Utils;
import io.github.jdbcx.executor.jdbc.SqlExceptionUtils;

abstract class DefaultResource extends DefaultWrapper implements AutoCloseable {
    protected final AtomicBoolean closed;

    protected final AtomicReference<SQLWarning> warning;

    protected final DefaultResource parent;
    protected final Set<AutoCloseable> children;

    protected DefaultResource(DefaultResource parent) {
        this.closed = new AtomicBoolean();
        this.warning = new AtomicReference<>();

        if ((this.parent = parent) != null) {
            parent.add(this);
        }
        this.children = Collections.synchronizedSet(new LinkedHashSet<>());
    }

    protected void ensureOpen() throws SQLException {
        if (closed.get()) {
            throw SqlExceptionUtils.clientError("Cannot operate on a closed ".concat(getClass().getSimpleName()));
        }
    }

    protected boolean remove(AutoCloseable resource) {
        return children.remove(resource);
    }

    protected void reset() {
        this.warning.set(null);
        Utils.closeQuietly(children, true);
    }

    public <T extends AutoCloseable> T add(T resource) {
        try {
            if (isClosed()) {
                throw new IllegalStateException("Resource has been closed");
            }
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
        children.add(resource);
        return resource;
    }

    public <T extends AutoCloseable> T get(Class<T> clazz) {
        return get(clazz, null);
    }

    public <T extends AutoCloseable> T get(Class<T> clazz, Predicate<T> filter) {
        if (clazz != null) {
            for (AutoCloseable resource : children) {
                if (clazz.isInstance(resource)) {
                    T obj = clazz.cast(resource);
                    if (filter == null || filter.test(obj)) {
                        return obj;
                    }
                }
            }
        }
        return null;
    }

    public SQLWarning getWarnings() throws SQLException {
        ensureOpen();
        return warning.get();
    }

    public void clearWarnings() throws SQLException {
        ensureOpen();
        warning.set(null);
    }

    public boolean isClosed() throws SQLException {
        return closed.get();
    }

    @Override
    public void close() throws SQLException {
        reset();
        if (parent != null) {
            parent.remove(this);
        }

        closed.set(true);
    }
}
