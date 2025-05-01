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
package io.github.jdbcx.data;

import java.util.Iterator;
import java.util.List;
import java.util.function.BiFunction;

import io.github.jdbcx.Field;
import io.github.jdbcx.Row;

public final class IterableWrapper<T> implements Iterable<Row> {
    static class WrappedIterator<T> implements Iterator<Row> {
        protected final List<Field> fields;
        protected final Iterator<T> it;

        WrappedIterator(IterableWrapper<T> wrapper) {
            this.fields = wrapper.fields;
            this.it = wrapper.it.iterator();
        }

        @Override
        public boolean hasNext() {
            return it.hasNext();
        }

        @Override
        public Row next() {
            return Row.of(fields, it.next());
        }
    }

    static final class CustomizedIterator<T> extends WrappedIterator<T> {
        private final BiFunction<List<Field>, T, Row> converter;

        CustomizedIterator(IterableWrapper<T> wrapper) {
            super(wrapper);
            this.converter = wrapper.converter;
        }

        @Override
        public Row next() {
            return converter.apply(fields, it.next());
        }
    }

    private final List<Field> fields;
    private final Iterable<T> it;

    private final BiFunction<List<Field>, T, Row> converter;

    public IterableWrapper(List<Field> fields, Iterable<T> it, BiFunction<List<Field>, T, Row> converter) {
        if (fields == null || it == null) {
            throw new IllegalArgumentException("Non-null fields and values are required");
        }

        this.fields = fields;
        this.it = it;
        this.converter = converter;
    }

    public IterableWrapper(List<Field> fields, Iterable<T> it) {
        this(fields, it, null);
    }

    @Override
    public Iterator<Row> iterator() {
        return converter != null ? new CustomizedIterator<>(this) : new WrappedIterator<>(this);
    }
}
