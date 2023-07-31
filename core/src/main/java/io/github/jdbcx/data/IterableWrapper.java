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

import io.github.jdbcx.Checker;
import io.github.jdbcx.Row;

public final class IterableWrapper implements Iterable<Row> {
    static final class WrappedIterator implements Iterator<Row> {
        private final Iterator<?> it;

        WrappedIterator(Iterator<?> it) {
            this.it = it;
        }

        @Override
        public boolean hasNext() {
            return it.hasNext();
        }

        @Override
        public Row next() {
            return Row.of(it.next());
        }
    }

    private final Iterable<?> it;

    public IterableWrapper(Iterable<?> it) {
        this.it = Checker.nonNull(it, Iterable.class.getSimpleName());
    }

    @Override
    public Iterator<Row> iterator() {
        return new WrappedIterator(it.iterator());
    }
}
