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

import io.github.jdbcx.Checker;
import io.github.jdbcx.Field;
import io.github.jdbcx.Row;
import io.github.jdbcx.Value;

public final class DefaultRow implements Row {
    private static final Field[] DEFAULT_FIELDS = new Field[] { Field.of("results") };

    public static final DefaultRow EMPTY = new DefaultRow(new Field[0], new Value[0]);

    private final Field[] fields;
    private final Value[] values;

    public DefaultRow(Value value) {
        this.fields = DEFAULT_FIELDS;
        this.values = new Value[] { Checker.nonNull(value, Value.class) };
    }

    public DefaultRow(Value[] values) {
        int len = Checker.nonNull(values, Value[].class).length;
        if (len == 1) {
            this.fields = DEFAULT_FIELDS;
        } else {
            this.fields = new Field[len];
            StringBuilder builder = new StringBuilder("col");
            int position = builder.length();
            for (int i = 0; i < len; i++) {
                this.fields[i] = Field.of(builder.append(i + 1).toString());
                builder.setLength(position);
            }
        }
        this.values = values;
    }

    DefaultRow(Field field, Value value) {
        this.fields = new Field[] { field };
        this.values = new Value[] { value };
    }

    DefaultRow(Field[] fields, Value[] values) {
        this.fields = fields;
        this.values = values;
    }

    @Override
    public Field field(int index) {
        return fields[index];
    }

    @Override
    public Value value(int index) {
        return values[index];
    }

    @Override
    public int size() {
        return values.length;
    }
}
