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

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.List;
import java.util.Properties;

import io.github.jdbcx.Constants;
import io.github.jdbcx.Field;
import io.github.jdbcx.Option;
import io.github.jdbcx.Result;
import io.github.jdbcx.Row;
import io.github.jdbcx.Value;

public class CsvSerde extends TextSerde {
    public static final Option OPTION_USEQUOTES = Option
            .of(new String[] { "useQuotes", "Quote character", Constants.FALSE_EXPR, Constants.TRUE_EXPR });
    public static final Option OPTION_DELIMITER = Option.of(new String[] { "delimChar", "Delimiter character", "," });
    public static final Option OPTION_LINE = Option.of(new String[] { "lineChar", "New line character", "\n" });
    public static final Option OPTION_QUOTE = Option.of(new String[] { "quoteChar", "Quote character", "\"" });
    public static final Option OPTION_ESCAPE = Option.of(new String[] { "escapeChar", "Escape character", "\"" });

    private final boolean useQuotes;
    private final char delimChar;
    private final char quoteChar;
    private final char escapeChar;
    private final char lineChar;

    public CsvSerde(Properties config) {
        super(config);

        this.useQuotes = Boolean.parseBoolean(OPTION_USEQUOTES.getValue(config));
        this.delimChar = OPTION_DELIMITER.getValue(config).charAt(0);
        this.quoteChar = OPTION_QUOTE.getValue(config).charAt(0);
        this.escapeChar = OPTION_ESCAPE.getValue(config).charAt(0);
        this.lineChar = OPTION_LINE.getValue(config).charAt(0);
    }

    protected String encode(String str) {
        if (str == null) {
            if (useQuotes) {
                StringBuilder builder = new StringBuilder(nullValue.length() + 2);
                return builder.append(quoteChar).append(nullValue).append(quoteChar).toString();
            }
            return nullValue;
        } else {
            int len = str.length();
            boolean quoted = this.useQuotes;
            StringBuilder builder = new StringBuilder(len + (quoted ? 2 : 0));
            if (quoted) {
                builder.append(quoteChar);
            }
            for (int i = 0; i < len; i++) {
                char ch = str.charAt(i);
                if (!quoted && (ch == delimChar || ch == lineChar || ch == '\r')) {
                    builder.insert(0, quoteChar);
                    quoted = true;
                } else if (ch == quoteChar || ch == escapeChar) {
                    if (!quoted) {
                        builder.insert(0, quoteChar);
                        quoted = true;
                    }
                    builder.append(escapeChar);
                }
                builder.append(ch);
            }
            if (quoted) {
                builder.append(quoteChar);
            }
            return builder.toString();
        }
    }

    protected void writeHeader(List<Field> fields, int size, Writer writer) throws IOException {
        writer.write(encode(fields.get(0).name()));
        for (int i = 1; i < size; i++) {
            Field f = fields.get(i);
            writer.write(delimChar);
            writer.write(encode(f.name()));
        }
    }

    protected void writeRow(Row r, int size, Writer writer) throws IOException {
        Value v = r.value(0);
        writer.write(encode(v.isNull() ? null : v.asString(charset)));
        for (int i = 1; i < size; i++) {
            v = r.value(i);
            writer.write(delimChar);
            writer.write(encode(v.isNull() ? null : v.asString(charset)));
        }
    }

    @Override
    protected Result<?> deserialize(Reader reader) throws IOException {
        throw new UnsupportedOperationException("Unimplemented method 'deserialize'");
    }

    @Override
    protected void serialize(Result<?> result, Writer writer) throws IOException {
        final List<Field> fields = result.fields();
        final int size = fields.size();

        if (header) {
            writeHeader(fields, size, writer);
            for (Row r : result.rows()) {
                writer.write(lineChar);
                writeRow(r, size, writer);
            }
        } else {
            boolean notFirst = false;
            for (Row r : result.rows()) {
                if (notFirst) {
                    writer.write(lineChar);
                } else {
                    notFirst = true;
                }
                writeRow(r, size, writer);
            }
        }
    }
}
