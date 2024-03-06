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
package io.github.jdbcx.format;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.Charset;
import java.util.Properties;

import io.github.jdbcx.Checker;
import io.github.jdbcx.Constants;
import io.github.jdbcx.Logger;
import io.github.jdbcx.LoggerFactory;
import io.github.jdbcx.Option;
import io.github.jdbcx.Result;
import io.github.jdbcx.Serialization;

public abstract class TextSerde implements Serialization {
    private static final Logger log = LoggerFactory.getLogger(TextSerde.class);

    public static final Option OPTION_CHARSET = Option.of(new String[] { "charset", "Character set" });
    public static final Option OPTION_HEADER = Option.of(new String[] { "header", "Whether contains header or not" });
    public static final Option OPTION_NULL_VALUE = Option
            .of(new String[] { "null.value", "String literal representing null value, defaults to empty string" });

    protected final int buffer;
    protected final Charset charset;
    protected final boolean header;
    protected final String nullValue;

    protected TextSerde(Properties config) {
        String value = OPTION_BUFFER.getValue(config);
        buffer = Checker.isNullOrEmpty(value) ? 2048 : Integer.parseInt(value);
        value = OPTION_CHARSET.getValue(config);
        charset = Checker.isNullOrEmpty(value) ? Constants.DEFAULT_CHARSET : Charset.forName(value);
        value = OPTION_HEADER.getValue(config);
        header = Checker.isNullOrEmpty(value) || Boolean.parseBoolean(value);
        value = OPTION_NULL_VALUE.getValue(config);
        nullValue = Checker.isNullOrEmpty(value) ? Constants.EMPTY_STRING : value;
    }

    protected abstract Result<?> deserialize(Reader reader) throws IOException; // NOSONAR

    protected abstract void serialize(Result<?> result, Writer writer) throws IOException;

    @Override
    public Result<?> deserialize(InputStream in) throws IOException {
        return buffer > 0 ? deserialize(new BufferedReader(new InputStreamReader(in, charset), buffer))
                : deserialize(new InputStreamReader(in, charset));
    }

    @Override
    public void serialize(Result<?> result, OutputStream out) throws IOException {
        if (result.fields().isEmpty()) {
            log.warn("Nothing to write due to no field defined in the given result");
            return;
        }

        final Writer writer = buffer > 0 ? new BufferedWriter(new OutputStreamWriter(out, charset), buffer)
                : new OutputStreamWriter(out, charset);
        serialize(result, writer);
        writer.flush();
    }
}
