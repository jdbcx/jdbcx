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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import io.github.jdbcx.DeferredValue;
import io.github.jdbcx.Field;
import io.github.jdbcx.Row;

public final class IterableRow implements Iterable<Row> {
    static final class RowIterator implements Iterator<Row> {
        private final IterableRow ref;

        private int index;
        private Iterator<Row> current;

        RowIterator(IterableRow ref) {
            this.ref = ref;

            this.index = 0;
            this.current = null;
        }

        @Override
        public boolean hasNext() {
            final int size = ref.values.size();
            if (index > size) {
                return false;
            } else if (current == null || !current.hasNext()) {
                while (index < size) {
                    Iterable<Row> it = ref.values.get(index++).get();
                    if (it != null) {
                        current = it.iterator();
                        if (current.hasNext()) {
                            return true;
                        }
                    }
                }
            }
            return current != null && current.hasNext();
        }

        @Override
        public Row next() {
            try {
                return current.next();
            } catch (IndexOutOfBoundsException e) {
                throw new NoSuchElementException(e.getMessage());
            }
        }
    }

    private final List<Field> fields;
    private final List<DeferredValue<Iterable<Row>>> values;

    public IterableRow(List<Field> fields, List<DeferredValue<Iterable<Row>>> values) {
        if (fields == null) {
            throw new IllegalArgumentException("Non-null fields and values are required");
        }
        this.fields = fields;
        this.values = values == null || values.isEmpty() ? Collections.emptyList()
                : Collections.unmodifiableList(new ArrayList<>(values));
    }

    @Override
    public Iterator<Row> iterator() {
        return new RowIterator(this);
    }
}
