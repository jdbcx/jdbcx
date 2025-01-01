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

import io.github.jdbcx.Field;
import io.github.jdbcx.Row;

public final class IterableWrapper implements Iterable<Row> {
    static final class WrappedIterator implements Iterator<Row> {
        private final List<Field> fields;
        private final Iterator<?> it;

        WrappedIterator(IterableWrapper wrapper) {
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

    private final List<Field> fields;
    private final Iterable<?> it;

    public IterableWrapper(List<Field> fields, Iterable<?> it) {
        if (fields == null || it == null) {
            throw new IllegalArgumentException("Non-null fields and values are required");
        }

        this.fields = fields;
        this.it = it;
    }

    @Override
    public Iterator<Row> iterator() {
        return new WrappedIterator(this);
    }
}
