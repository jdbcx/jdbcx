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
package io.github.jdbcx.executor;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.Buffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.util.concurrent.CompletionException;

import io.github.jdbcx.Constants;

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

    public static long pipeAsync(InputStream from, OutputStream to) throws CompletionException {
        return pipeAsync(from, to, null, false);
    }

    public static long pipeAsync(InputStream from, OutputStream to, boolean closeOutput) throws CompletionException {
        return pipeAsync(from, to, null, closeOutput);
    }

    public static long pipeAsync(InputStream from, OutputStream to, byte[] buffer, boolean closeOutput)
            throws CompletionException {
        try {
            return pipe(from, to, buffer, closeOutput);
        } catch (IOException e) {
            throw new CompletionException(e);
        }
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

    public static long pipeAsync(Readable from, Writer to) throws CompletionException {
        return pipeAsync(from, to, null, false);
    }

    public static long pipeAsync(Readable from, Writer to, boolean closeOutput) throws CompletionException {
        return pipeAsync(from, to, null, closeOutput);
    }

    public static long pipeAsync(Readable from, Writer to, char[] buffer, boolean closeOutput)
            throws CompletionException {
        try {
            return pipe(from, to, buffer, closeOutput);
        } catch (IOException e) {
            throw new CompletionException(e);
        }
    }

    public static String readAllAsString(InputStream input) throws IOException {
        return readAllAsString(input, null);
    }

    public static String readAllAsString(InputStream input, Charset charset) throws IOException {
        if (input == null) {
            return Constants.EMPTY_STRING;
        }

        try (InputStream in = input;
                ByteArrayOutputStream out = new ByteArrayOutputStream(Constants.DEFAULT_BUFFER_SIZE)) {
            Stream.pipe(in, out);
            return new String(out.toByteArray(), charset != null ? charset : Constants.DEFAULT_CHARSET);
        }
    }

    public static String readAllAsString(Reader input) throws IOException {
        if (input == null) {
            return Constants.EMPTY_STRING;
        }

        try (Reader reader = input; StringWriter writer = new StringWriter(Constants.DEFAULT_BUFFER_SIZE / 2)) {
            Stream.pipe(reader, writer);
            return writer.toString();
        }
    }

    public static void writeAll(OutputStream output, Object input) throws IOException {
        if (input instanceof InputStream) {
            Stream.pipe((InputStream) input, output);
        } else if (input instanceof File) {
            Stream.pipe(new FileInputStream((File) input), output);
        } else if (input instanceof Readable) {
            Stream.pipe((Readable) input, new OutputStreamWriter(output, Constants.DEFAULT_CHARSET));
        } else if (input != null) {
            Stream.pipe(new ByteArrayInputStream(input.toString().getBytes(Constants.DEFAULT_CHARSET)), output);
        }
    }

    private Stream() {
    }
}
