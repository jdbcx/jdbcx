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
package io.github.jdbcx.format;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import io.github.jdbcx.Checker;
import io.github.jdbcx.Constants;
import io.github.jdbcx.Logger;
import io.github.jdbcx.LoggerFactory;
import io.github.jdbcx.Option;
import io.github.jdbcx.Result;
import io.github.jdbcx.Serialization;
import io.github.jdbcx.Stream;

public final class BinarySerde implements Serialization {
    private static final Logger log = LoggerFactory.getLogger(BinarySerde.class);

    public static final Option OPTION_CHARSET = Option.of(new String[] { "charset", "Character set" });

    final Charset charset;

    public BinarySerde(Properties config) {
        String value = OPTION_CHARSET.getValue(config);
        charset = Checker.isNullOrEmpty(value) ? Constants.DEFAULT_CHARSET : Charset.forName(value);
    }

    @Override
    public Result<?> deserialize(InputStream in) throws IOException {
        throw new UnsupportedOperationException("Unimplemented method 'deserialize'");
    }

    @Override
    public void serialize(Result<?> result, OutputStream out) throws IOException {
        final Object obj;
        if (result.fields().isEmpty()) {
            obj = null;
        } else {
            Object o = result.get();
            if (o instanceof ResultSet) {
                try (ResultSet rs = (ResultSet) o) {
                    o = null;
                    if (rs.next()) {
                        o = rs.getObject(1);
                    }
                } catch (SQLException e) {
                    throw new IOException(e);
                }
            }
            obj = o;
        }

        if (obj == null) {
            log.warn("Nothing to write due to no field or no value defined in the given result");
            return;
        }

        final long written;
        if (obj instanceof InputStream) {
            written = Stream.pipe((InputStream) obj, out);
        } else if (obj instanceof byte[]) {
            byte[] bytes = ((byte[]) obj);
            out.write(bytes);
            written = bytes.length;
        } else if (obj instanceof ByteBuffer) {
            ByteBuffer buf = (ByteBuffer) obj;
            int length = buf.remaining();
            if (length > 0) {
                byte[] bytes = new byte[length];
                buf.get(bytes);
                out.write(bytes);
            }
            written = length;
        } else if (obj instanceof CharSequence) {
            String str = ((CharSequence) obj).toString();
            int length = 0;
            if (!str.isEmpty()) {
                byte[] bytes = str.getBytes(charset);
                length = bytes.length;
                out.write(bytes);
            }
            written = length;
        } else if (obj instanceof Readable) {
            int length = 0;
            try (StringWriter writer = new StringWriter()) {
                Stream.pipe((Readable) obj, writer);
                String str = writer.toString();
                byte[] bytes = str.getBytes(charset);
                length = bytes.length;
                out.write(bytes);
            }
            written = length;
        } else if (obj instanceof char[]) {
            char[] chars = (char[]) obj;
            int length = chars.length;
            if (length > 0) {
                byte[] bytes = new String(chars).getBytes(charset);
                length = bytes.length;
                out.write(bytes);
            }
            written = length;
        } else if (obj instanceof CharBuffer) {
            CharBuffer buf = (CharBuffer) obj;
            int length = buf.remaining();
            if (length > 0) {
                char[] chars = new char[length];
                buf.get(chars);
                byte[] bytes = new String(chars).getBytes(charset);
                length = bytes.length;
                out.write(bytes);
            }
            written = length;
        } else {
            throw new UnsupportedOperationException("Unsupported binary source: " + obj);
        }
        log.debug("Written %d bytes from result", written);
    }
}
