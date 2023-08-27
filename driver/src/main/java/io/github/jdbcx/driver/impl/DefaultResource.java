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
package io.github.jdbcx.driver.impl;

import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import io.github.jdbcx.executor.jdbc.SqlExceptionUtils;

abstract class DefaultResource extends DefaultWrapper implements AutoCloseable {
    protected final AtomicBoolean closed;

    protected final AtomicReference<SQLWarning> warning;

    protected final DefaultResource parent;
    protected final List<DefaultResource> children;

    protected DefaultResource(DefaultResource parent) {
        this.closed = new AtomicBoolean();
        this.warning = new AtomicReference<>();

        this.parent = parent;
        this.children = Collections.synchronizedList(new LinkedList<>());
    }

    protected void ensureOpen() throws SQLException {
        if (closed.get()) {
            throw SqlExceptionUtils.clientError("Cannot operate on a closed ".concat(getClass().getSimpleName()));
        }
    }

    protected <T extends DefaultResource> T add(T resource) {
        if (!children.contains(resource)) {
            children.add(resource);
        }
        return resource;
    }

    protected void remove(DefaultResource resource) {
        children.remove(resource);
    }

    protected void reset() {
        this.warning.set(null);

        this.children.clear();
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

        for (Iterator<DefaultResource> it = children.iterator(); it.hasNext();) {
            try {
                it.next().close();
            } catch (Exception e) {
                // log.debug("Failed to close statement %s", stmt, e);
            }
        }

        closed.set(true);
    }
}
