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
import java.io.Reader;
import java.io.Writer;
import java.util.List;
import java.util.Properties;

import io.github.jdbcx.Field;
import io.github.jdbcx.Result;
import io.github.jdbcx.Row;
import io.github.jdbcx.Value;

public class TsvSerde extends TextSerde {
    public TsvSerde(Properties config) {
        super(config);
    }

    protected String encode(String str) {
        if (str == null) {
            return nullValue;
        } else {
            int len = str.length();
            StringBuilder builder = new StringBuilder(len);
            for (int i = 0; i < len; i++) {
                char ch = str.charAt(i);
                if (ch == '\t') {
                    builder.append('\\').append('t');
                } else if (ch == '\r') {
                    builder.append('\\').append('r');
                } else if (ch == '\n') {
                    builder.append('\\').append('n');
                } else if (ch == '\\') {
                    builder.append('\\').append(ch);
                } else {
                    builder.append(ch);
                }
            }
            return builder.toString();
        }
    }

    protected void writeHeader(List<Field> fields, int size, Writer writer) throws IOException {
        writer.write(encode(fields.get(0).name()));
        for (int i = 1; i < size; i++) {
            Field f = fields.get(i);
            writer.write('\t');
            writer.write(encode(f.name()));
        }
    }

    protected void writeRow(Row r, int size, Writer writer) throws IOException {
        Value v = r.value(0);
        writer.write(encode(v.isNull() ? null : v.asString(charset)));
        for (int i = 1; i < size; i++) {
            v = r.value(i);
            writer.write('\t');
            writer.write(encode(v.isNull() ? null : v.asString(charset)));
        }
    }

    @Override
    public Result<?> deserialize(Reader reader) throws IOException {
        throw new UnsupportedOperationException("Unimplemented method 'deserialize'");
    }

    @Override
    public void serialize(Result<?> result, Writer writer) throws IOException {
        final List<Field> fields = result.fields();
        final int size = fields.size();

        if (header) {
            writeHeader(fields, size, writer);
            for (Row r : result.rows()) {
                writer.write('\n');
                writeRow(r, size, writer);
            }
        } else {
            boolean notFirst = false;
            for (Row r : result.rows()) {
                if (notFirst) {
                    writer.write('\n');
                } else {
                    notFirst = true;
                }
                writeRow(r, size, writer);
            }
        }
    }
}
