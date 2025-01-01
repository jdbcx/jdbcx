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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import io.github.jdbcx.Constants;
import io.github.jdbcx.Field;
import io.github.jdbcx.Row;

public final class IterableInputStream implements Iterable<Row> {
    static final class InputStreamIterator implements Iterator<Row> {
        private final List<Field> fields;
        private final InputStream input;
        private final byte[] delimiter;
        private final byte[] buffer;

        private final ByteArrayOutputStream builder;

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
                if ((length = input.read(buffer)) == -1) {
                    input.close();
                }
                return length >= 0;
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        InputStreamIterator(IterableInputStream i) {
            this.fields = i.fields;
            this.input = i.input;
            this.delimiter = i.delimiter;
            this.buffer = new byte[Math.max(Constants.DEFAULT_BUFFER_SIZE, delimiter.length)];

            this.builder = new ByteArrayOutputStream();
        }

        @Override
        public boolean hasNext() {
            return read();
        }

        @Override
        public Row next() { // NOSONAR
            final ByteArrayOutputStream out = builder;
            out.reset();

            final byte[] arr = delimiter;
            final int len = arr.length;

            if (len == 0) {
                do {
                    out.write(buffer, position, length);
                    position = length;
                } while (read());
                return Row.of(fields, out.toByteArray());
            }

            int mi = 0;
            outerloop: do { // NOSONAR
                final byte[] buf = buffer;
                for (int i = position; i < length; i++) {
                    byte ch = buf[i];
                    if (ch == arr[mi]) {
                        if (++mi == len) {
                            position = i + 1;
                            break outerloop;
                        }
                    } else {
                        out.write(arr, 0, mi);
                        if (ch == arr[0]) {
                            mi = 1;
                        } else {
                            out.write(ch);
                            mi = 0;
                        }
                    }
                }
                position = length;
            } while (read());
            if (length == -1) {
                if (mi > 0) {
                    out.write(arr, 0, mi);
                }
                if (out.size() == 0) {
                    throw new NoSuchElementException();
                }
            }
            return Row.of(fields, out.toByteArray());
        }
    }

    private final List<Field> fields;
    private final InputStream input;
    private final byte[] delimiter;

    public IterableInputStream(List<Field> fields, InputStream input) {
        this(fields, input, null);
    }

    public IterableInputStream(List<Field> fields, InputStream input, byte[] delimiter) {
        if (fields == null || input == null) {
            throw new IllegalArgumentException("Non-null fields and input are required");
        }
        this.fields = fields;
        this.input = input;
        this.delimiter = delimiter != null ? delimiter : Constants.EMPTY_BYTE_ARRAY;
    }

    @Override
    public Iterator<Row> iterator() {
        return new InputStreamIterator(this);
    }
}
