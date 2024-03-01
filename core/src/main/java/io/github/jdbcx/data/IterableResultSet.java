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
package io.github.jdbcx.data;

import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import io.github.jdbcx.Checker;
import io.github.jdbcx.Field;
import io.github.jdbcx.Row;
import io.github.jdbcx.Value;
import io.github.jdbcx.ValueFactory;
import io.github.jdbcx.value.WrappedValue;

public final class IterableResultSet implements Iterable<Row> {
    static final class ResultSetValue extends WrappedValue {
        private final transient ResultSet rs;
        private final int index;

        ResultSetValue(Value v, ResultSet rs, int index) {
            super(v);
            this.rs = rs;
            this.index = index;
        }

        @Override
        protected void ensureUpdated() {
            try {
                value.updateFrom(rs, index);
                setUpdated(true);
            } catch (SQLException e) {
                throw new IllegalArgumentException(e);
            }
        }

        @Override
        public Value updateFrom(ResultSet rs, int index) throws SQLException {
            if (this.rs != rs || this.index != index) {
                value.updateFrom(rs, index);
                setUpdated(true);
            }
            return this;
        }
    }

    static final class ResultSetIterator implements Iterator<Row> {
        private final ResultSet rs;
        private final Row cursor;

        private int state; // 0 - before first; 1 - has next; 2 - eos

        ResultSetIterator(ResultSet rs) {
            this(rs, null);
        }

        ResultSetIterator(ResultSet rs, List<Field> fields) {
            this.rs = rs;

            final ValueFactory factory = ValueFactory.getInstance();
            try {
                final Value[] values;
                if (fields != null) {
                    int size = fields.size();
                    values = new Value[size];
                    for (int i = 0; i < size; i++) {
                        values[i] = new ResultSetValue(factory.newValue(fields.get(i), null), rs, i + 1);
                    }
                } else {
                    ResultSetMetaData metaData = rs.getMetaData();
                    int columns = metaData.getColumnCount();
                    fields = new ArrayList<>(columns);
                    values = new Value[columns];
                    for (int i = 0; i < columns; i++) {
                        int index = i + 1;
                        int precision = metaData.getPrecision(index);
                        if (precision <= 0) {
                            precision = metaData.getColumnDisplaySize(index);
                        }
                        Field f = Field.of(metaData.getColumnName(index), metaData.getColumnTypeName(index),
                                JDBCType.valueOf(metaData.getColumnType(index)),
                                metaData.isNullable(index) != ResultSetMetaData.columnNoNulls,
                                precision, metaData.getScale(index), metaData.isSigned(index));
                        fields.add(f);
                        values[i] = new ResultSetValue(factory.newValue(f, metaData.getColumnTypeName(index)), rs,
                                index);
                    }
                }
                this.cursor = new DefaultRow(fields, values);
            } catch (SQLException e) {
                throw new IllegalArgumentException(e);
            }

            this.state = 0;
        }

        @Override
        public boolean hasNext() {
            try {
                final boolean b = rs.next();
                state = b ? 1 : 2;
                if (!b) {
                    rs.close();
                }
                return b;
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public Row next() {
            if (state == 2 || (state == 0 && !hasNext())) {
                throw new NoSuchElementException();
            }
            return cursor;
        }
    }

    private final List<Field> fields;
    private final ResultSet rs;

    public IterableResultSet(ResultSet rs) {
        this(rs, null);
    }

    public IterableResultSet(ResultSet rs, List<Field> fields) {
        this.rs = Checker.nonNull(rs, ResultSet.class);
        this.fields = fields;
    }

    @Override
    public Iterator<Row> iterator() {
        try {
            if (rs.isClosed()) { // || rs.isAfterLast()
                return Collections.emptyListIterator();
            }
        } catch (SQLException e) {
            // ignore
        }

        // try {
        // if (!rs.isBeforeFirst()) {
        // throw new IllegalStateException(
        // "ResultSet has been read as the cursor position is not before the first");
        // }
        // } catch (SQLException e) {
        // // ignore
        // }

        return new ResultSetIterator(rs, fields);
    }
}
