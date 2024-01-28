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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import io.github.jdbcx.Field;
import io.github.jdbcx.Row;
import io.github.jdbcx.Value;

public final class DefaultRow implements Row {
    static final List<Field> defaultFields = Collections.singletonList(Field.DEFAULT);

    public static final DefaultRow EMPTY = new DefaultRow(Collections.emptyList());

    private final List<Field> fields;
    private final Value[] values;
    private final int size;

    public DefaultRow(Value... values) {
        int len = 0;
        if (values == null) {
            this.fields = EMPTY.fields;
            this.values = EMPTY.values;
        } else if ((len = values.length) == 1) {
            this.fields = defaultFields;
            this.values = values;
        } else {
            List<Field> list = new ArrayList<>(len);
            StringBuilder builder = new StringBuilder("field");
            int position = builder.length();
            for (int i = 0; i < len; i++) {
                list.add(Field.of(builder.append(i + 1).toString()));
                builder.setLength(position);
            }
            this.fields = Collections.unmodifiableList(list);
            this.values = values;
        }
        this.size = len;
    }

    public DefaultRow(List<Field> fields, Value... values) {
        this.fields = fields != null ? fields : Collections.emptyList();
        this.values = values != null ? values : new Value[0];

        this.size = Math.min(this.fields.size(), this.values.length);
    }

    @Override
    public Field field(int index) {
        return fields.get(index);
    }

    @Override
    public List<Field> fields() {
        return Collections.unmodifiableList(fields);
    }

    @Override
    public Value value(int index) {
        return values[index];
    }

    @Override
    public List<Value> values() {
        return Collections.unmodifiableList(Arrays.asList(values));
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder("Row[");
        for (int i = 0; i < size; i++) {
            builder.append(fields.get(i).name()).append('=').append(values[i].asString()).append(',');
        }
        if (size > 0) {
            builder.setLength(builder.length() - 1);
        }
        return builder.append("]@").append(hashCode()).toString();
    }
}
