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
package io.github.jdbcx;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;

import io.github.jdbcx.data.IterableArray;
import io.github.jdbcx.data.IterableInputStream;
import io.github.jdbcx.data.IterableReader;
import io.github.jdbcx.data.IterableResultSet;
import io.github.jdbcx.data.IterableWrapper;
import io.github.jdbcx.executor.Stream;
import io.github.jdbcx.executor.jdbc.ReadOnlyResultSet;
import io.github.jdbcx.format.ArrowSerde;
import io.github.jdbcx.format.AvroSerde;
import io.github.jdbcx.format.BsonSerde;
import io.github.jdbcx.format.CsvSerde;
import io.github.jdbcx.format.JsonlSerde;
import io.github.jdbcx.format.JsonSeqSerde;
import io.github.jdbcx.format.ParquetSerde;
import io.github.jdbcx.format.TsvSerde;
import io.github.jdbcx.format.ValuesSerde;

public final class Result<T> implements AutoCloseable {
    public static final class Builder {
        static final DeferredValue<Iterable<Row>> NO_ROW = DeferredValue.of(Optional.of(Collections.emptyList()));

        private final List<Field> fields;

        private Object rawValue;
        private Class<?> valueType;
        private DeferredValue<Iterable<Row>> rows;

        private Builder() {
            this.fields = new ArrayList<>();
            this.fields.add(Field.DEFAULT);

            this.rawValue = null;
            this.valueType = null;
            this.rows = null;
        }

        private Builder(Result<?> template) {
            this.fields = new ArrayList<>(template.fields());

            this.rawValue = template.rawValue;
            this.valueType = template.valueType;
            this.rows = template.rows;
        }

        public Builder fields(Field... fields) {
            this.fields.clear();
            if (fields != null && fields.length > 0) {
                this.fields.addAll(Arrays.asList(fields));
            }
            return this;
        }

        public Builder fields(Collection<Field> fields) {
            this.fields.clear();
            if (fields != null) {
                this.fields.addAll(fields);
            }
            return this;
        }

        public Builder rows(Value[]... rows) {
            this.rawValue = rows;
            if (rows == null || rows.length == 0) {
                this.valueType = Value[].class;
                this.rows = NO_ROW;
            } else {
                this.valueType = rows.getClass();
                this.rows = DeferredValue.of(Optional.of(new IterableArray(fields, rows)));
            }
            return this;
        }

        public Builder rows(Object[]... rows) {
            this.rawValue = rows;
            if (rows == null || rows.length == 0) {
                this.valueType = Object[].class;
                this.rows = NO_ROW;
            } else {
                this.valueType = rows.getClass();
                this.rows = DeferredValue.of(Optional.of(new IterableArray(fields, rows)));
            }
            return this;
        }

        public Builder value(String str) {
            this.rawValue = str;
            if (str == null) {
                this.valueType = String.class;
                this.rows = NO_ROW;
            } else {
                this.valueType = str.getClass();
                this.rows = DeferredValue.of(Optional.of(Collections.singletonList(Row.of(fields, str))));
            }
            return this;
        }

        public Builder value(InputStream input, byte[] delimiter) {
            this.rawValue = input;
            if (input == null) {
                this.valueType = InputStream.class;
                this.rows = NO_ROW;
            } else {
                this.valueType = input.getClass();
                this.rows = DeferredValue.of(Optional.of(new IterableInputStream(DEFAULT_FIELDS, input, delimiter)));
            }
            return this;
        }

        public Builder value(Reader reader, String delimiter) {
            this.rawValue = reader;
            if (reader == null) {
                this.valueType = Reader.class;
                this.rows = NO_ROW;
            } else {
                this.valueType = reader.getClass();
                this.rows = DeferredValue.of(Optional.of(new IterableReader(DEFAULT_FIELDS, reader, delimiter)));
            }
            return this;
        }

        public Result<?> build() { // NOSONAR
            if (valueType == null || rows == null) {
                throw new IllegalArgumentException("Non-null value type and rows are required");
            }

            return new Result<>(fields, rawValue, valueType, rows);
        }
    }

    private static final ErrorHandler defaultErrorHandler = new ErrorHandler(Constants.EMPTY_STRING);

    public static final List<Field> DEFAULT_FIELDS = Collections.singletonList(Field.DEFAULT);

    public static final Serialization getSerde(Format format, Properties config) {
        if (format == null) {
            throw new IllegalArgumentException("Non-null format is required");
        }
        final Serialization serde;
        switch (format) {
            case ARROW:
                serde = new ArrowSerde(config);
                break;
            case ARROW_STREAM: {
                Properties props = new Properties(config);
                ArrowSerde.OPTION_STREAM.setValue(props, Constants.TRUE_EXPR);
                serde = new ArrowSerde(props);
                break;
            }
            case AVRO:
                serde = new AvroSerde(config);
                break;
            case AVRO_BINARY: {
                Properties props = new Properties(config);
                AvroSerde.OPTION_ENCODER.setValue(props, AvroSerde.ENCODER_BINARY);
                serde = new AvroSerde(props);
                break;
            }
            case AVRO_JSON: {
                Properties props = new Properties(config);
                AvroSerde.OPTION_ENCODER.setValue(props, AvroSerde.ENCODER_JSON);
                serde = new AvroSerde(props);
                break;
            }
            case BSON:
                serde = new BsonSerde(config);
                break;
            case CSV:
                serde = new CsvSerde(config);
                break;
            case TSV:
                serde = new TsvSerde(config);
                break;
            case JSONL:
            case NDJSON:
                serde = new JsonlSerde(config);
                break;
            case JSON_SEQ:
                serde = new JsonSeqSerde(config);
                break;
            case PARQUET:
                serde = new ParquetSerde(config);
                break;
            case VALUES:
                serde = new ValuesSerde(config);
                break;
            default:
                throw new IllegalArgumentException("Unsupport format: " + format);
        }
        return serde;
    }

    public static final Result<?> readFrom(Format format, Properties config, InputStream in) throws IOException { // NOSONAR
        if (in == null) {
            throw new IllegalArgumentException("Non-null input stream is required");
        }

        return getSerde(format, config).deserialize(in);
    }

    public static final void writeTo(Result<?> result, Format format, Properties config, OutputStream out)
            throws IOException {
        if (result == null) {
            return;
        } else if (out == null) {
            throw new IllegalArgumentException("Non-null output stream is required");
        }

        getSerde(format, config).serialize(result, out);
    }

    public static Result<Object[]> of(Object[] arr, Field... fields) {
        return of(Arrays.asList(fields), arr);
    }

    public static Result<Object[]> of(List<Field> fields, Object[] arr) {
        if (fields == null || fields.isEmpty()) {
            fields = DEFAULT_FIELDS;
        }
        return new Result<>(fields, Checker.nonNull(arr, Object[].class), arr.getClass(),
                new IterableArray(fields, arr));
    }

    public static Result<Iterable<?>> of(List<Field> fields, Iterable<?> it) { // NOSONAR
        if (fields == null || fields.isEmpty()) {
            fields = DEFAULT_FIELDS;
        }
        IterableWrapper wrapper = new IterableWrapper(fields, it); // only works when both arguments are not null
        return new Result<>(fields, it, it.getClass(), wrapper);
    }

    public static Result<?> of(List<Field> fields, Row... rows) { // NOSONAR
        if (fields == null || fields.isEmpty()) {
            fields = DEFAULT_FIELDS;
        }
        return new Result<>(fields, Checker.nonNull(rows, Row[].class), rows.getClass(), Arrays.asList(rows));
    }

    public static Result<?> of(Row row, Row... more) { // NOSONAR
        final int len;
        final List<Field> fields;
        if (row == null || more == null) {
            throw new IllegalArgumentException("Non-null row is required");
        } else if ((fields = row.fields()).isEmpty()) {
            return new Result<>(fields, null, Row.class, Collections.emptyList());
        } else if ((len = more.length) == 0) {
            return new Result<>(fields, row, row.getClass(), Collections.singletonList(row));
        }

        List<Row> rows = new ArrayList<>(len + 1);
        rows.add(row);
        for (int i = 0; i < len; i++) {
            Row r = more[i];
            if (r != null) {
                rows.add(r);
            }
        }
        return new Result<>(fields, rows, rows.getClass(), rows);
    }

    public static Result<ResultSet> of(ResultSet rs) {
        if (rs == null) {
            throw new IllegalArgumentException("Non-null ResultSet is required");
        }

        try {
            ResultSetMetaData metaData = rs.getMetaData();
            int columns = metaData.getColumnCount();
            List<Field> fields = new ArrayList<>(columns);
            for (int index = 1; index <= columns; index++) {
                int precision = metaData.getPrecision(index);
                if (precision <= 0) {
                    precision = metaData.getColumnDisplaySize(index);
                }
                fields.add(Field.of(metaData.getColumnName(index), metaData.getColumnTypeName(index),
                        JDBCType.valueOf(metaData.getColumnType(index)),
                        metaData.isNullable(index) != ResultSetMetaData.columnNoNulls, precision,
                        metaData.getScale(index), metaData.isSigned(index)));
            }
            fields = Collections.unmodifiableList(fields);
            return new Result<>(fields, rs, ResultSet.class, new IterableResultSet(rs, fields));
        } catch (SQLException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public static Result<String> of(String str) {
        return new Result<>(DEFAULT_FIELDS, str, String.class, Collections.singletonList(Row.of(str)));
    }

    public static Result<?> of(Collection<String> list) { // NOSONAR
        return of(DEFAULT_FIELDS, list);
    }

    public static Result<Long> of(Long l) {
        return new Result<>(DEFAULT_FIELDS, l, Long.class, Collections.singletonList(Row.of(l)));
    }

    public static Result<InputStream> of(InputStream input, byte[] delimiter, Field... fields) {
        return of(fields != null && fields.length > 0 ? Arrays.asList(fields) : DEFAULT_FIELDS, input, delimiter);
    }

    public static Result<InputStream> of(List<Field> fields, InputStream input, byte[] delimiter) {
        if (fields == null || fields.isEmpty()) {
            fields = DEFAULT_FIELDS;
        }
        return new Result<>(fields, Checker.nonNull(input, InputStream.class), input.getClass(),
                new IterableInputStream(fields, input, delimiter));
    }

    public static Result<Reader> of(Reader reader, String delimiter, Field... fields) {
        return of(fields != null && fields.length > 0 ? Arrays.asList(fields) : DEFAULT_FIELDS, reader, delimiter);
    }

    public static Result<Reader> of(List<Field> fields, Reader reader, String delimiter) {
        if (fields == null || fields.isEmpty()) {
            fields = DEFAULT_FIELDS;
        }
        return new Result<>(fields, Checker.nonNull(reader, Reader.class), reader.getClass(),
                new IterableReader(fields, reader, delimiter));
    }

    public static Builder builder() {
        return new Builder();
    }

    private final List<Field> fields;
    private final T rawValue;
    private final Class<?> valueType;

    private final DeferredValue<Iterable<Row>> rows;

    private final AtomicBoolean active;

    public Builder update() {
        return new Builder(this);
    }

    /**
     * Checks whether this result object is active for reading, meaning that one of
     * {@code get(*)} and {@link #rows()} has been called at least once.
     *
     * @return true if the the result object is active for reading; false otherwise
     */
    public boolean isActive() {
        return active.get();
    }

    /**
     * Gets raw value.
     *
     * @return raw value, could be {@code null}
     */
    public T get() {
        active.compareAndSet(false, true);
        return rawValue;
    }

    /**
     * Gets value according to the given type. Same as {@code get(clazz, null)}.
     *
     * @param <R>   type of the value
     * @param clazz non-null class of the value
     * @return non-null value
     * @throws CompletionException      when failed to retrieve or convert value
     * @throws IllegalArgumentException when the given {@code clazz} is null
     */
    public <R> R get(Class<R> clazz) throws CompletionException {
        return get(clazz, defaultErrorHandler);
    }

    /**
     * Gets value according to the given type.
     *
     * @param <R>     type of the value
     * @param clazz   non-null class of the value
     * @param handler optional error handler
     * @return non-null value
     * @throws CompletionException      when failed to retrieve or convert value
     * @throws IllegalArgumentException when the given {@code clazz} is null
     */
    public <R> R get(Class<R> clazz, ErrorHandler handler) {
        active.compareAndSet(false, true);
        if (clazz == null) {
            throw new IllegalArgumentException("Non-null class is required");
        } else if (Result.class.equals(clazz) || clazz.isAssignableFrom(valueType)) {
            return clazz.cast(rawValue);
        } else if (ResultSet.class.equals(clazz)) {
            return clazz.cast(new ReadOnlyResultSet(null, this));
        } else if (String.class.equals(clazz)) {
            String str;
            if (InputStream.class.isAssignableFrom(valueType)) {
                try {
                    str = Stream.readAllAsString((InputStream) rawValue);
                } catch (IOException e) {
                    str = (handler != null ? handler : defaultErrorHandler).handle(e);
                }
            } else if (Reader.class.isAssignableFrom(valueType)) {
                try {
                    str = Stream.readAllAsString((Reader) rawValue);
                } catch (IOException e) {
                    str = (handler != null ? handler : defaultErrorHandler).handle(e);
                }
            } else if (ResultSet.class.isAssignableFrom(valueType)) {
                try (ResultSet rs = (ResultSet) rawValue) {
                    StringBuilder builder = new StringBuilder();
                    while (rs.next()) {
                        builder.append(rs.getString(1)).append('\n');
                    }
                    int len = builder.length() - 1;
                    if (len >= 0) {
                        builder.setLength(len);
                    }
                    str = builder.toString();
                } catch (SQLException e) {
                    str = (handler != null ? handler : defaultErrorHandler).handle(e);
                }
            } else if (Iterable.class.isAssignableFrom(valueType) || valueType.isArray()) {
                if (fields.isEmpty()) {
                    str = Constants.EMPTY_STRING;
                } else {
                    StringBuilder builder = new StringBuilder();
                    for (Row r : rows()) {
                        for (Value v : r.values()) {
                            builder.append(v.asString()).append(',');
                        }
                        builder.setLength(builder.length() - 1);
                        builder.append('\n');
                    }
                    int len = builder.length() - 1;
                    if (len < 0) {
                        str = Constants.EMPTY_STRING;
                    } else {
                        builder.setLength(len);
                        str = builder.toString();
                    }
                }
            } else if (Row.class.isAssignableFrom(valueType)) {
                StringBuilder builder = new StringBuilder();
                List<Value> values = ((Row) rawValue).values();
                for (Value v : values) {
                    builder.append(v.asString()).append(',');
                }
                if (!values.isEmpty()) {
                    builder.setLength(builder.length() - 1);
                }
                str = builder.toString();
            } else if (Value.class.isAssignableFrom(valueType)) {
                str = ((Value) rawValue).asString();
            } else {
                str = String.valueOf(rawValue);
            }
            return clazz.cast(str);
        }

        throw new UnsupportedOperationException(
                Utils.format("Cannot convert value from [%s] to [%s]", valueType, clazz));
    }

    public int getFieldCount() {
        return fields.size();
    }

    public Class<?> type() {
        return valueType;
    }

    public List<Field> fields() {
        return Collections.unmodifiableList(fields);
    }

    public Iterable<Row> rows() {
        active.compareAndSet(false, true);
        return rows.get();
    }

    @Override
    public void close() {
        if (AutoCloseable.class.isAssignableFrom(valueType)) {
            try {
                ((AutoCloseable) rawValue).close();
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }
        active.compareAndSet(true, false);
    }

    private Result(List<Field> fields, T rawValue, Class<?> valueType, Iterable<Row> rows) {
        this(fields, rawValue, valueType, DeferredValue.of(Optional.of(rows)));
    }

    private Result(List<Field> fields, T rawValue, Class<?> valueType, DeferredValue<Iterable<Row>> rows) {
        this.fields = fields;
        this.rawValue = rawValue;
        this.valueType = valueType;
        this.rows = rows;

        this.active = new AtomicBoolean(false);
    }
}
