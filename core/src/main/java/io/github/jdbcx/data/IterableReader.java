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

import java.io.IOException;
import java.io.Reader;
import java.util.Iterator;
import java.util.NoSuchElementException;

import io.github.jdbcx.Checker;
import io.github.jdbcx.Constants;
import io.github.jdbcx.Row;

public final class IterableReader implements Iterable<Row> {
    static final class ReaderIterator implements Iterator<Row> {
        private final Reader reader;
        private final char[] delimiter;
        private final char[] buffer;

        private final StringBuilder builder;

        private int position = 0;
        private int length = 0;

        boolean read() {
            if (position < length) {
                return true;
            } else if (length == -1) {
                return false;
            }

            position = 0;
            try {
                return (length = reader.read(buffer)) >= 0;
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }

        ReaderIterator(Reader reader, String delimiter) {
            this.reader = reader;
            this.delimiter = delimiter.toCharArray();
            this.buffer = new char[Math.max(Constants.DEFAULT_BUFFER_SIZE / 2, this.delimiter.length)];

            this.builder = new StringBuilder();
        }

        @Override
        public boolean hasNext() {
            return read();
        }

        @Override
        public Row next() { // NOSONAR
            final StringBuilder out = builder;
            out.setLength(0);

            final char[] arr = delimiter;
            final int len = arr.length;
            if (len == 0) {
                do {
                    out.append(buffer, position, length);
                    position = length;
                } while (read());
                return Row.of(out.toString());
            }

            int mi = 0;
            outerloop: do { // NOSONAR
                char[] buf = buffer;
                for (int i = position; i < length; i++) {
                    char ch = buf[i];
                    out.append(ch);
                    if (ch == arr[mi]) {
                        if (++mi == len) {
                            out.setLength(out.length() - len);
                            position = i + 1;
                            break outerloop;
                        }
                    } else {
                        mi = ch == arr[0] ? 1 : 0;
                    }
                }
                position = length;
            } while (read());
            if (length == -1 && out.length() == 0) {
                throw new NoSuchElementException();
            }
            return Row.of(out.toString());
        }
    }

    private final Reader reader;
    private final String delimiter;

    public IterableReader(Reader reader) {
        this(reader, null);
    }

    public IterableReader(Reader reader, String delimiter) {
        this.reader = Checker.nonNull(reader, Reader.class.getSimpleName());
        this.delimiter = Checker.isNullOrEmpty(delimiter) ? Constants.EMPTY_STRING : delimiter;
    }

    @Override
    public Iterator<Row> iterator() {
        return new ReaderIterator(reader, delimiter);
    }
}
