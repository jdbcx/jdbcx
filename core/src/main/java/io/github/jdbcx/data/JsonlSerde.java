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
package io.github.jdbcx.data;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.List;
import java.util.Properties;

import io.github.jdbcx.Converter;
import io.github.jdbcx.Field;
import io.github.jdbcx.Result;
import io.github.jdbcx.Row;

public class JsonlSerde extends TextSerde {
    public JsonlSerde(Properties config) {
        super(config);
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
    protected Result<?> deserialize(Reader reader) throws IOException {
        throw new UnsupportedOperationException("Unimplemented method 'deserialize'");
    }

    @Override
    protected void serialize(Result<?> result, Writer writer) throws IOException {
        final List<Field> fields = result.fields();
        final int size = fields.size();

        boolean notFirst = false;
        if (header) {
            final String[] names = new String[size];
            for (int i = 0; i < size; i++) {
                names[i] = Converter.toJsonExpression(fields.get(i).name()) + ":";
            }
            for (Row r : result.rows()) {
                if (notFirst) {
                    writer.write('\n');
                } else {
                    notFirst = true;
                }
                writer.write('{');
                writeNamesAndValues(names, r, size, writer);
                writer.write('}');
            }
        } else {
            for (Row r : result.rows()) {
                if (notFirst) {
                    writer.write('\n');
                } else {
                    notFirst = true;
                }
                writer.write('[');
                writeValues(r, size, writer);
                writer.write(']');
            }
        }
    }
}
