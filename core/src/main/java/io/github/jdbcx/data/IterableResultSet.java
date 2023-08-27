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
package io.github.jdbcx.data;

import java.nio.charset.Charset;
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

public final class IterableResultSet implements Iterable<Row> {
    static final class ResultSetValue implements Value {
        private final transient ResultSet rs;
        private final int index;

        ResultSetValue(ResultSet rs, int index) {
            this.rs = rs;
            this.index = index;
        }

        @Override
        public boolean isNull() {
            try {
                return rs.getObject(index) == null;
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public String asString(Charset charset) {
            try {
                return rs.getString(index);
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public Object asObject() {
            return rs;
        }
    }

    static final class ResultSetIterator implements Iterator<Row> {
        private final ResultSet rs;
        private final List<Field> fields;
        private final Row cursor;

        private int state; // 0 - before first; 1 - has next; 2 - eos

        ResultSetIterator(ResultSet rs) {
            this(rs, null);
        }

        ResultSetIterator(ResultSet rs, List<Field> fields) {
            this.rs = rs;

            try {
                final Value[] values;
                if (fields != null) {
                    int size = fields.size();
                    values = new Value[size];
                    for (int i = 0; i < size; i++) {
                        values[i] = new ResultSetValue(rs, i + 1);
                    }
                } else {
                    ResultSetMetaData metaData = rs.getMetaData();
                    int columns = metaData.getColumnCount();
                    fields = new ArrayList<>(columns);
                    values = new Value[columns];
                    for (int i = 0; i < columns; i++) {
                        int index = i + 1;
                        fields.add(Field.of(metaData.getColumnName(index), metaData.getColumnType(index)));
                        values[i] = new ResultSetValue(rs, index);
                    }
                }
                this.fields = fields;
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
        this.rs = Checker.nonNull(rs, ResultSet.class.getSimpleName());
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
        //     if (!rs.isBeforeFirst()) {
        //         throw new IllegalStateException(
        //                 "ResultSet has been read as the cursor position is not before the first");
        //     }
        // } catch (SQLException e) {
        //     // ignore
        // }

        return new ResultSetIterator(rs, fields);
    }
}
