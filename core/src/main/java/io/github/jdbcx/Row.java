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
package io.github.jdbcx;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

import io.github.jdbcx.data.DefaultRow;
import io.github.jdbcx.data.LongValue;
import io.github.jdbcx.data.StringValue;

public interface Row extends Serializable {
    static final Row EMPTY = DefaultRow.EMPTY;

    static Row of(List<Field> fields, Object value) {
        final Row row;
        if (value == null) {
            row = of(fields);
        } else if (value instanceof Row) {
            row = (Row) value;
        } else if (value instanceof Iterable) {
            row = of(fields, (Iterable<?>) value);
        } else if (value instanceof Value[]) {
            row = of(fields, (Value[]) value);
        } else if (value instanceof String[]) {
            row = of(fields, (String[]) value);
        } else if (value instanceof Object[]) {
            row = of(fields, (Object[]) value);
        } else if (value instanceof Number) {
            row = of(fields, new LongValue(((Number) value).longValue()));
        } else {
            row = of(fields, new StringValue(value));
        }
        return row;
    }

    static Row of(List<Field> fields, Iterable<?> iterable) {
        if (iterable == null) {
            return new DefaultRow(fields);
        }

        List<Value> list = new LinkedList<>();
        for (Object obj : iterable) {
            list.add(new StringValue(obj));
        }
        return new DefaultRow(fields, list.toArray(new Value[0]));
    }

    static Row of(List<Field> fields, Object[] array) {
        final int len;
        if (array == null || (len = array.length) == 0) {
            return new DefaultRow(fields);
        } else if (len == 1 && array[0] instanceof Row) {
            return (Row) array[0];
        }

        StringValue[] values = new StringValue[len];
        for (int i = 0; i < len; i++) {
            values[i] = new StringValue(array[i]);
        }
        return new DefaultRow(fields, values);
    }

    static Row of(List<Field> fields, byte[][] array) {
        final int len;
        if (array == null || (len = array.length) == 0) {
            return new DefaultRow(fields);
        }

        StringValue[] values = new StringValue[len];
        for (int i = 0; i < len; i++) {
            values[i] = new StringValue(array[i]);
        }
        return new DefaultRow(fields, values);
    }

    static Row of(List<Field> fields, String[] array) {
        final int len;
        if (array == null || (len = array.length) == 0) {
            return new DefaultRow(fields);
        }

        StringValue[] values = new StringValue[len];
        for (int i = 0; i < len; i++) {
            values[i] = new StringValue(array[i]);
        }
        return new DefaultRow(fields, values);
    }

    static Row of(Long value) {
        return new DefaultRow(new LongValue(value));
    }

    static Row of(String value) {
        return new DefaultRow(new StringValue(value));
    }

    static Row of(List<Field> fields, Value value) {
        return new DefaultRow(fields, value);
    }

    static Row of(List<Field> fields, Value... values) {
        return new DefaultRow(fields, values);
    }

    Field field(int index);

    List<Field> fields();

    default int index(String fieldName) {
        int idx = -1;
        for (int i = 0; i < size(); i++) {
            Field f = field(i);
            if (f.name().equalsIgnoreCase(fieldName)) {
                idx = i;
                break;
            }
        }
        return idx;
    }

    Value value(int index);

    default Value value(String fieldName) {
        int idx = index(fieldName);
        if (idx == -1) {
            throw new IllegalArgumentException(Utils.format("No field named \"%s\"", fieldName));
        }
        return value(idx);
    }

    List<Value> values();

    int size();
}
