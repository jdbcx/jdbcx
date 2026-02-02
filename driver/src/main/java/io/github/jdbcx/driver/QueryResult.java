/*
 * Copyright 2022-2026, Zhichun Wu
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
import java.sql.SQLWarning;
import java.util.concurrent.atomic.AtomicReference;

import io.github.jdbcx.executor.jdbc.SqlExceptionUtils;

public final class QueryResult implements AutoCloseable {
    private final AtomicReference<ResultSet> generatedKeys;
    private final AtomicReference<ResultSet> resultSet;
    private final AtomicReference<Long> updateCount;
    private final AtomicReference<SQLWarning> warnings;

    protected void resetValues() {
        this.generatedKeys.set(null);
        this.resultSet.set(null);
        this.updateCount.set(null);
    }

    public QueryResult(AtomicReference<SQLWarning> warnings) {
        this.generatedKeys = new AtomicReference<>();
        this.resultSet = new AtomicReference<>();
        this.updateCount = new AtomicReference<>();
        this.warnings = warnings != null ? warnings : new AtomicReference<>();
    }

    public void reset() {
        resetValues();

        this.warnings.set(null);
    }

    public void close() {
        reset();
    }

    public ResultSet getGeneratedKeys() {
        return generatedKeys.get();
    }

    public ResultSet getResultSet() {
        return resultSet.get();
    }

    public Long getUpdateCount() {
        return updateCount.get();
    }

    public SQLWarning getWarnings() {
        return warnings.get();
    }

    public boolean hasGeneratedKeys() {
        return generatedKeys.get() != null;
    }

    public boolean hasResultSet() {
        return resultSet.get() != null;
    }

    public boolean hasUpdateCount() {
        return updateCount.get() != null;
    }

    public boolean hasWarnings() {
        return warnings.get() != null;
    }

    public void setGeneratedKeys(ResultSet rs) {
        resetValues();

        this.generatedKeys.set(rs);
    }

    public void setResultSet(ResultSet rs) {
        resetValues();

        this.resultSet.set(rs);
    }

    public void setUpdateCount(long updateCount) {
        resetValues();

        this.updateCount.set(updateCount);
    }

    public void setWarnings(SQLWarning warnings) {
        this.warnings.set(warnings);
    }

    public void setNextWarning(SQLWarning next) {
        if (next == null) {
            return;
        }

        SQLWarning current = warnings.get();
        if (current == null) {
            warnings.set(next);
        } else {
            current.setNextWarning(next);
        }
    }

    public void updateAll(ResultSet keys, ResultSet rs, long affectedRows) throws SQLException {
        if (!generatedKeys.compareAndSet(null, keys)) {
            throw SqlExceptionUtils.clientError("Conflict when setting generated keys");
        }
        if (!resultSet.compareAndSet(null, rs)) {
            throw SqlExceptionUtils.clientError("Conflict when setting result sets");
        }
        if (!updateCount.compareAndSet(null, affectedRows)) {
            throw SqlExceptionUtils.clientError("Conflict when setting update count");
        }
    }

    public void updateKeys(ResultSet keys, long affectedRows) throws SQLException {
        if (!generatedKeys.compareAndSet(null, keys)) {
            throw SqlExceptionUtils.clientError("Conflict when setting generated keys");
        }
        if (!updateCount.compareAndSet(null, affectedRows)) {
            throw SqlExceptionUtils.clientError("Conflict when setting update count");
        }
    }

    public void updateResult(ResultSet rs) throws SQLException {
        if (!resultSet.compareAndSet(null, rs)) {
            throw SqlExceptionUtils.clientError("Conflict when setting result sets");
        }
    }
}
