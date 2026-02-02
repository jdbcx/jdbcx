/*
 * Copyright 2022-2026, Zhichun Wu
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

import io.github.jdbcx.Checker;
import io.github.jdbcx.Constants;
import io.github.jdbcx.Field;
import io.github.jdbcx.Result;
import io.github.jdbcx.Row;
import io.github.jdbcx.Value;

public class MarkdownSerde extends TextSerde {
    static final String MARKDOWN_RESERVED_CHARS = "!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~";

    static final String NEW_LINE = "<br/>";
    static final String COLUMN_BEGIN = "| ";
    static final String COLUMN_END = " |";
    static final String COLUMN_SEP = " | ";
    static final String COLUMN_NEW = "\n|";

    static final String ALIGNMENT_LEFT = ":-|";
    static final String ALIGHMENT_RIGHT = "-:|";

    static final Properties update(Properties config) {
        Properties newConf = new Properties(config);
        if (Checker.isNullOrEmpty(OPTION_HEADER.getValue(newConf))) {
            OPTION_HEADER.setValue(newConf, Constants.TRUE_EXPR);
        }
        return newConf;
    }

    public MarkdownSerde(Properties config) {
        super(update(config));
    }

    protected String encode(String str) {
        if (str == null) {
            return nullValue;
        } else {
            int len = str.length();
            StringBuilder builder = new StringBuilder(len);
            for (int i = 0; i < len; i++) {
                char ch = str.charAt(i);
                if (ch == '\r') {
                    // skip
                } else if (ch == '\n') {
                    builder.append(NEW_LINE);
                } else if (MARKDOWN_RESERVED_CHARS.indexOf(ch) != -1) {
                    builder.append('\\').append(ch);
                } else {
                    builder.append(ch);
                }
            }
            return builder.toString();
        }
    }

    protected void writeHeader(List<Field> fields, int size, Writer writer) throws IOException {
        writer.write(COLUMN_BEGIN);
        StringBuilder builder = new StringBuilder(size * 2 + 2).append(COLUMN_NEW);
        if (size-- > 1) {
            for (int i = 0; i < size; i++) {
                Field f = fields.get(i);
                writer.write(encode(f.name()));
                writer.write(COLUMN_SEP);
                builder.append(f.scale() > 0 ? ALIGHMENT_RIGHT : ALIGNMENT_LEFT);
            }
        }
        Field f = fields.get(size);
        writer.write(encode(f.name()));
        builder.append(f.scale() > 0 ? ALIGHMENT_RIGHT : ALIGNMENT_LEFT);
        writer.write(COLUMN_END);
        writer.write(builder.toString());
    }

    protected void writeRow(Row r, int size, Writer writer) throws IOException {
        writer.write(COLUMN_BEGIN);
        if (size-- > 1) {
            for (int i = 0; i < size; i++) {
                Value v = r.value(i);
                writer.write(encode(v.isNull() ? null : v.asString(charset)));
                writer.write(COLUMN_SEP);
            }
        }
        Value v = r.value(size);
        writer.write(encode(v.isNull() ? null : v.asString(charset)));
        writer.write(COLUMN_END);
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
