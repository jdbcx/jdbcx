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
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;

import io.github.jdbcx.Converter;
import io.github.jdbcx.Field;
import io.github.jdbcx.Result;
import io.github.jdbcx.Row;

public class JsonSeqSerde extends TextSerde {
    static final Properties update(Properties config) {
        Properties props = new Properties(config);
        OPTION_CHARSET.setValue(props, StandardCharsets.UTF_8.name());
        return props;
    }

    public JsonSeqSerde(Properties config) {
        super(update(config));
    }

    protected void writeNamesAndValues(String[] names, Row r, int size, Writer writer) throws IOException {
        writer.write(names[0]);
        writer.write(r.value(0).toJsonExpression());
        for (int i = 1; i < size; i++) {
            writer.write(',');
            writer.write(names[i]);
            writer.write(r.value(i).toJsonExpression());
        }
    }

    protected void writeValues(Row r, int size, Writer writer) throws IOException {
        writer.write(r.value(0).toJsonExpression());
        for (int i = 1; i < size; i++) {
            writer.write(',');
            writer.write(r.value(i).toJsonExpression());
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
            final String[] names = new String[size];
            for (int i = 0; i < size; i++) {
                names[i] = Converter.toJsonExpression(fields.get(i).name()) + ":";
            }
            for (Row r : result.rows()) {
                writer.write(0x1E);
                writer.write('{');
                writeNamesAndValues(names, r, size, writer);
                writer.write('}');
                writer.write('\n');
            }
        } else {
            for (Row r : result.rows()) {
                writer.write(0x1E);
                writer.write('[');
                writeValues(r, size, writer);
                writer.write(']');
                writer.write('\n');
            }
        }
    }
}
