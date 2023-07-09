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

import java.net.ConnectException;
import java.sql.BatchUpdateException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;

/**
 * Helper class for building {@link SQLException}.
 */
public final class SqlExceptionUtils {
    public static final String SQL_STATE_CLIENT_ERROR = "HY000";
    public static final String SQL_STATE_OPERATION_CANCELLED = "HY008";
    public static final String SQL_STATE_CONNECTION_EXCEPTION = "08000";
    public static final String SQL_STATE_SQL_ERROR = "07000";
    public static final String SQL_STATE_NO_DATA = "02000";
    public static final String SQL_STATE_INVALID_SCHEMA = "3F000";
    public static final String SQL_STATE_INVALID_TX_STATE = "25000";
    public static final String SQL_STATE_DATA_EXCEPTION = "22000";
    public static final String SQL_STATE_FEATURE_NOT_SUPPORTED = "0A000";

    private SqlExceptionUtils() {
    }

    private static SQLException create(Throwable e) {
        if (e == null) {
            return unknownError();
        } else if (e instanceof SQLException) {
            return (SQLException) e;
        }

        Throwable cause = e.getCause();
        if (cause instanceof SQLException) {
            return (SQLException) cause;
        } else if (cause == null) {
            cause = e;
        }

        return new SQLException(cause);
    }

    // https://en.wikipedia.org/wiki/SQLSTATE
    private static String toSqlState(Exception e) {
        final String sqlState;

        Throwable cause = e.getCause();
        if (e instanceof ConnectException || cause instanceof ConnectException) {
            sqlState = SQL_STATE_CONNECTION_EXCEPTION;
        } else {
            sqlState = SQL_STATE_SQL_ERROR;
        }
        return sqlState;
    }

    public static SQLException clientError(String message) {
        return new SQLException(message, SQL_STATE_CLIENT_ERROR, null);
    }

    public static SQLWarning clientWarning(String message) {
        return new SQLWarning(message, SQL_STATE_CLIENT_ERROR, null);
    }

    public static SQLException clientError(Throwable e) {
        return e != null ? new SQLException(e.getMessage(), SQL_STATE_CLIENT_ERROR, e) : unknownError();
    }

    public static SQLWarning clientWarning(Throwable e) {
        return e != null ? new SQLWarning(e.getMessage(), SQL_STATE_CLIENT_ERROR, e) : unknownWarning();
    }

    public static SQLException clientError(String message, Throwable e) {
        return new SQLException(message, SQL_STATE_CLIENT_ERROR, e);
    }

    public static SQLWarning clientWarning(String message, Throwable e) {
        return new SQLWarning(message, SQL_STATE_CLIENT_ERROR, e);
    }

    public static SQLException handle(Exception e) {
        return e != null ? new SQLException(e.getMessage(), toSqlState(e), 0, e.getCause())
                : unknownError();
    }

    public static SQLException handle(Throwable e, Throwable... more) {
        SQLException rootEx = create(e);
        if (more != null) {
            for (Throwable t : more) {
                rootEx.setNextException(create(t));
            }
        }
        return rootEx;
    }

    public static BatchUpdateException batchUpdateError(Throwable e, long[] updateCounts) {
        if (e == null) {
            return new BatchUpdateException("Something went wrong when performing batch update", SQL_STATE_CLIENT_ERROR,
                    0, updateCounts, null);
        } else if (e instanceof BatchUpdateException) {
            return (BatchUpdateException) e;
        } else if (e instanceof SQLException) {
            SQLException sqlExp = (SQLException) e;
            return new BatchUpdateException(sqlExp.getMessage(), sqlExp.getSQLState(), sqlExp.getErrorCode(),
                    updateCounts, null);
        }

        Throwable cause = e.getCause();
        if (e instanceof BatchUpdateException) {
            return (BatchUpdateException) e;
        } else if (cause instanceof SQLException) {
            SQLException sqlExp = (SQLException) cause;
            return new BatchUpdateException(sqlExp.getMessage(), sqlExp.getSQLState(), sqlExp.getErrorCode(),
                    updateCounts, null);
        } else if (cause == null) {
            cause = e;
        }

        return new BatchUpdateException("Unexpected error", SQL_STATE_SQL_ERROR, 0, updateCounts, cause);
    }

    public static BatchUpdateException queryInBatchError(int[] updateCounts) {
        return new BatchUpdateException("Query is not allowed in batch update", SQL_STATE_CLIENT_ERROR, updateCounts);
    }

    public static BatchUpdateException queryInBatchError(long[] updateCounts) {
        return new BatchUpdateException("Query is not allowed in batch update", SQL_STATE_CLIENT_ERROR, 0, updateCounts,
                null);
    }

    public static SQLException undeterminedExecutionError() {
        return clientError("Please either call clearBatch() to clean up context first, or use executeBatch() instead");
    }

    public static SQLException forCancellation(Exception e) {
        Throwable cause = e.getCause();
        if (cause == null) {
            cause = e;
        }

        // operation canceled
        return new SQLException(e.getMessage(), SQL_STATE_OPERATION_CANCELLED, 0, cause);
    }

    public static SQLFeatureNotSupportedException unsupportedError(String message) {
        return new SQLFeatureNotSupportedException(message, SQL_STATE_FEATURE_NOT_SUPPORTED);
    }

    public static SQLException unknownError() {
        return new SQLException("Unknown error", SQL_STATE_CLIENT_ERROR);
    }

    public static SQLWarning unknownWarning() {
        return new SQLWarning("Unknown error", SQL_STATE_CLIENT_ERROR);
    }
}
