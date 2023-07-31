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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import io.github.jdbcx.data.DefaultRow;
import io.github.jdbcx.data.LongValue;
import io.github.jdbcx.data.StringValue;

public interface Row extends Serializable {
    static final Row EMPTY = DefaultRow.EMPTY;

    static Row of(Object obj) {
        if (obj instanceof Row) {
            return (Row) obj;
        }
        return of(obj != null ? obj.toString() : Constants.EMPTY_STRING);
    }

    static Row of(byte[] binary) {
        return new DefaultRow(new StringValue(binary));
    }

    static Row of(String value) {
        return new DefaultRow(new StringValue(value));
    }

    static Row of(Long value) {
        return new DefaultRow(new LongValue(value));
    }

    static Row of(String[] values) {
        if (values == null || values.length == 0) {
            return EMPTY;
        }

        int len = values.length;
        Value[] array = new Value[len];
        for (int i = 0; i < len; i++) {
            array[i] = new StringValue(values[i]);
        }
        return new DefaultRow(array);
    }

    static Row of(Value... values) {
        if (values == null || values.length == 0) {
            return EMPTY;
        }

        final int len = values.length;
        List<Value> list = new ArrayList<>(len);
        for (int i = 0; i < len; i++) {
            Value val = values[i];
            if (val != null) {
                list.add(val);
            }
        }
        return new DefaultRow(list.toArray(new Value[0]));
    }

    // static Row merge(Row... rows) {
    // return null;
    // }

    Field field(int index);

    Value value(int index);

    default Value get(String name) {
        for (int i = 0; i < size(); i++) {
            Field f = field(i);
            if (f.name().equalsIgnoreCase(name)) {
                return value(i);
            }
        }

        throw new IllegalArgumentException(Utils.format("No field named \"%s\"", name));
    }

    int size();
}
