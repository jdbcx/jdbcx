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

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import io.github.jdbcx.data.IterableArray;
import io.github.jdbcx.data.IterableInputStream;
import io.github.jdbcx.data.IterableReader;
import io.github.jdbcx.data.IterableResultSet;
import io.github.jdbcx.data.IterableWrapper;
import io.github.jdbcx.executor.Stream;

public final class Result<T> {
    private static final ErrorHandler defaultErrorHandler = new ErrorHandler(Constants.EMPTY_STRING);

    public static Result<Object[]> of(Object[] arr) {
        return of(arr, () -> new IterableArray(arr));
    }

    public static Result<Iterable<?>> of(Iterable<?> it) {
        return of(it, () -> new IterableWrapper(it));
    }

    public static Result<ResultSet> of(ResultSet rs) {
        return of(rs, () -> new IterableResultSet(rs));
    }

    public static Result<String> of(String str) {
        return of(str, () -> Collections.singletonList(Row.of(str)));
    }

    public static Result<Long> of(Long l) {
        return of(l, () -> Collections.singletonList(Row.of(l)));
    }

    public static Result<InputStream> of(InputStream input) {
        return of(input, () -> new IterableInputStream(input));
    }

    public static Result<InputStream> of(InputStream input, byte[] delimiter) {
        return of(input, () -> new IterableInputStream(input, delimiter));
    }

    public static Result<Reader> of(Reader reader) {
        return of(reader, () -> new IterableReader(reader));
    }

    public static Result<Reader> of(Reader reader, String delimiter) {
        return of(reader, () -> new IterableReader(reader, delimiter));
    }

    public static <T> Result<T> of(T rawValue, Supplier<Iterable<Row>> supplier) {
        return of(rawValue, supplier, defaultErrorHandler);
    }

    public static <T> Result<T> of(T rawValue, Supplier<Iterable<Row>> supplier, ErrorHandler errorHandler) {
        if (rawValue == null || supplier == null) {
            throw new IllegalArgumentException("Non-null raw value and supplier are required");
        }

        return new Result<>(rawValue, DeferredValue.of(supplier), errorHandler);
    }

    public static <T> Result<T> of(T rawValue, Iterable<Row> rows) {
        return of(rawValue, rows, defaultErrorHandler);
    }

    public static <T> Result<T> of(T rawValue, Iterable<Row> rows, ErrorHandler errorHandler) {
        if (rawValue == null || rows == null) {
            throw new IllegalArgumentException("Non-null raw value and rows are required");
        }

        return new Result<>(rawValue, DeferredValue.of(Optional.of(rows)), errorHandler);
    }

    private final T rawValue;
    private final Class<?> valueType;

    private final AtomicReference<ErrorHandler> handler;
    private final DeferredValue<Iterable<Row>> rows;

    public Result<T> update(ErrorHandler errorHandler) {
        if (!handler.compareAndSet(defaultErrorHandler, errorHandler)) {
            throw new IllegalArgumentException(
                    Utils.format("Cannot update error handler to [%s] as we got [%s] already",
                            errorHandler, handler.get()));
        }
        return this;
    }

    public T get() {
        return rawValue;
    }

    /**
     * Gets value according to the given type.
     *
     * @param <R>   type of the value
     * @param clazz non-null class of the value
     * @return non-null value
     * @throws UncheckedIOException when failed to read data from
     *                              {@link InputStream} or {@link Reader}
     */
    @SuppressWarnings("unchecked")
    public <R> R get(Class<R> clazz) {
        if (clazz == null) {
            throw new IllegalArgumentException("Non-null class is required");
        } else if (clazz.isAssignableFrom(valueType)) {
            return clazz.cast(rawValue);
        } else if (String.class.equals(clazz)) {
            String str;
            if (InputStream.class.isAssignableFrom(valueType)) {
                try {
                    str = Stream.readAllAsString((InputStream) rawValue);
                } catch (IOException e) {
                    str = handler.get().handle(e);
                }
            } else if (Reader.class.isAssignableFrom(valueType)) {
                try {
                    str = Stream.readAllAsString((Reader) rawValue);
                } catch (IOException e) {
                    str = handler.get().handle(e);
                }
            } else if (ResultSet.class.isAssignableFrom(valueType)) {
                try (ResultSet rs = (ResultSet) rawValue) {
                    StringBuilder builder = new StringBuilder();
                    while (rs.next()) {
                        builder.append(rs.getString(1)).append('\n');
                    }
                    str = builder.toString();
                } catch (SQLException e) {
                    str = handler.get().handle(e);
                }
            } else {
                str = String.valueOf(rawValue);
            }
            return (R) str;
        }
        throw new UnsupportedOperationException(
                Utils.format("Cannot convert value from [%s] to [%s]", valueType, clazz));
    }

    public Class<?> type() {
        return valueType;
    }

    public Iterable<Row> rows() {
        return rows.get();
    }

    private Result(T rawValue, DeferredValue<Iterable<Row>> rows, ErrorHandler errorHandler) {
        this.rawValue = rawValue;
        this.valueType = rawValue.getClass();

        this.handler = new AtomicReference<>(errorHandler != null ? errorHandler : defaultErrorHandler);
        this.rows = rows;
    }
}
