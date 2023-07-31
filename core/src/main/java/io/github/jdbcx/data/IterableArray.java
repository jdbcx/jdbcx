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

import java.util.Iterator;
import java.util.NoSuchElementException;

import io.github.jdbcx.Checker;
import io.github.jdbcx.Row;

public final class IterableArray implements Iterable<Row> {
    static final class ArrayIterator implements Iterator<Row> {
        private final Object[] array;
        private int index;

        ArrayIterator(Object[] array) {
            this.array = array;
            this.index = 0;
        }

        @Override
        public boolean hasNext() {
            return index < array.length;
        }

        @Override
        public Row next() {
            try {
                return Row.of(array[index++]);
            } catch (IndexOutOfBoundsException e) {
                throw new NoSuchElementException(e.getMessage());
            }
        }
    }

    private final Object[] array;

    public IterableArray(Object[] array) {
        this.array = Checker.nonNull(array, Object[].class.getSimpleName());
    }

    @Override
    public Iterator<Row> iterator() {
        return new ArrayIterator(array);
    }
}
