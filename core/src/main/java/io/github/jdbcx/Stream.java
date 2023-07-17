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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Writer;
import java.nio.Buffer;
import java.nio.CharBuffer;

public final class Stream {
    public static long pipe(InputStream from, OutputStream to) throws IOException {
        return pipe(from, to, null, false);
    }

    public static long pipe(InputStream from, OutputStream to, boolean closeOutput) throws IOException {
        return pipe(from, to, null, closeOutput);
    }

    public static long pipe(InputStream from, OutputStream to, byte[] buffer, boolean closeOutput) throws IOException {
        if (from == null) {
            return 0L;
        }

        long count = 0L;
        if (buffer == null || buffer.length == 0) {
            buffer = new byte[Constants.DEFAULT_BUFFER_SIZE];
        }
        try (InputStream in = from) {
            int len = 0;
            while ((len = in.read(buffer)) != -1) {
                to.write(buffer, 0, len);
                count += len;
            }
            to.flush();
        } finally {
            if (closeOutput) {
                try {
                    to.close();
                } catch (Exception e) {
                    // ignore
                }
            }
        }
        return count;
    }

    public static long pipe(Readable from, Writer to) throws IOException {
        return pipe(from, to, null, false);
    }

    public static long pipe(Readable from, Writer to, boolean closeOutput) throws IOException {
        return pipe(from, to, null, closeOutput);
    }

    public static long pipe(Readable from, Writer to, char[] buffer, boolean closeOutput) throws IOException {
        if (from == null) {
            return 0L;
        }

        long count = 0L;
        if (buffer == null || buffer.length == 0) {
            buffer = new char[Constants.DEFAULT_BUFFER_SIZE / 2];
        }
        final CharBuffer buf = CharBuffer.wrap(buffer);

        try {
            int len = 0;
            while ((len = from.read(buf)) != -1) {
                to.write(buffer, 0, len);
                ((Buffer) buf).rewind();
                count += len;
            }
            to.flush();
        } finally {
            if (from instanceof AutoCloseable) {
                try {
                    ((AutoCloseable) from).close();
                } catch (Exception e) {
                    // ignore
                }
            }
            if (closeOutput) {
                try {
                    to.close();
                } catch (Exception e) {
                    // ignore
                }
            }
        }
        return count;
    }

    private Stream() {
    }
}
