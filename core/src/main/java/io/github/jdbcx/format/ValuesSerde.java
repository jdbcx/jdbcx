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
import java.util.Properties;

import io.github.jdbcx.Checker;
import io.github.jdbcx.Field;
import io.github.jdbcx.Option;
import io.github.jdbcx.Result;
import io.github.jdbcx.Row;
import io.github.jdbcx.Utils;

public class ValuesSerde extends TextSerde {
    static final String VALUES_EXPR = ") VALUES\n";
    static final String LINE_SUFFIX = ",\n";

    public static final Option OPTION_QUOTE = Option.of(new String[] { "quoteChar", "Quote character", "\"" });
    public static final Option OPTION_ESCAPE = Option
            .of(new String[] { "escapeChar", "Escape character, same as quoteChar by default" });

    private final char quoteChar;
    private final char escapeChar;

    public ValuesSerde(Properties config) {
        super(config);

        String str = OPTION_QUOTE.getValue(config);
        this.quoteChar = Checker.isNullOrEmpty(str) ? '"' : str.charAt(0);
        str = OPTION_ESCAPE.getValue(config);
        this.escapeChar = Checker.isNullOrEmpty(str) ? this.quoteChar : str.charAt(0);
    }

    @Override
    public Result<?> deserialize(Reader reader) throws IOException {
        throw new UnsupportedOperationException("Unimplemented method 'deserialize'");
    }

    @Override
    public void serialize(Result<?> result, Writer writer) throws IOException {
        final int size = result.getFieldCount();
        if (size == 0) {
            return;
        }

        final StringBuilder builder = new StringBuilder();
        if (header) {
            final char quote = this.quoteChar;
            final char escape = this.escapeChar;
            builder.append('(');
            for (Field f : result.fields()) {
                builder.append(quote).append(Utils.escape(f.name(), quote, escape)).append(quote).append(',');
            }
            builder.setLength(builder.length() - 1);
            writer.write(builder.append(VALUES_EXPR).toString());
            builder.setLength(0);
        }

        int len = 0;
        for (Row r : result.rows()) {
            if (len > 0) {
                builder.setLength(0);
                builder.append(LINE_SUFFIX);
            }
            builder.append('(');
            for (int i = 0; i < size; i++) {
                builder.append(r.value(i).toSqlExpression()).append(',');
            }
            len = builder.length();
            builder.setCharAt(len - 1, ')');
            writer.write(builder.toString());
        }
    }
}
