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
import java.util.List;
import java.util.Properties;
import java.util.function.BiConsumer;

import org.bson.BsonBinary;
import org.bson.BsonBinaryWriter;
import org.bson.BsonWriter;
import org.bson.io.BasicOutputBuffer;
import org.bson.io.OutputBuffer;
import org.bson.types.Decimal128;

import io.github.jdbcx.Field;
import io.github.jdbcx.Result;
import io.github.jdbcx.Row;
import io.github.jdbcx.Serialization;
import io.github.jdbcx.Value;

public class BsonSerde implements Serialization {

    static final class ColumnValueWriter {
        final String name;
        final BiConsumer<String, Value> func;

        ColumnValueWriter(String name, BiConsumer<String, Value> func) {
            this.name = name;
            this.func = func;
        }

        void write(Value value) {
            func.accept(name, value);
        }
    }

    static ColumnValueWriter newValueWriter(final BsonWriter writer, Field f) {
        // TODO https://clickhouse.com/docs/en/interfaces/formats#bsoneachrow
        final ColumnValueWriter cw;
        switch (f.type()) {
            case BOOLEAN: {
                cw = new ColumnValueWriter(f.name(), (n, v) -> writer.writeBoolean(n, v.asBoolean()));
                break;
            }
            case BIT:
                if (f.precision() > 1) {
                    cw = new ColumnValueWriter(f.name(),
                            (n, v) -> writer.writeBinaryData(n, new BsonBinary(v.asBinary())));
                } else {
                    cw = new ColumnValueWriter(f.name(), (n, v) -> writer.writeBoolean(n, v.asBoolean()));
                }
                break;
            case BINARY:
            case VARBINARY:
            case LONGVARBINARY:
            case BLOB: {
                cw = new ColumnValueWriter(f.name(), (n, v) -> writer.writeBinaryData(n, new BsonBinary(v.asBinary())));
                break;
            }
            case TINYINT:
            case SMALLINT:
            case INTEGER: {
                cw = new ColumnValueWriter(f.name(), (n, v) -> writer.writeInt32(n, v.asInt()));
                break;
            }
            case BIGINT: {
                cw = new ColumnValueWriter(f.name(), (n, v) -> writer.writeInt64(n, v.asLong()));
                break;
            }
            case REAL:
            case FLOAT:
            case DOUBLE: {
                cw = new ColumnValueWriter(f.name(), (n, v) -> writer.writeDouble(n, v.asDouble()));
                break;
            }
            case NUMERIC:
            case DECIMAL: {
                cw = new ColumnValueWriter(f.name(),
                        (n, v) -> writer.writeDecimal128(n, new Decimal128(v.asBigDecimal())));
                break;
            }
            case DATE: {
                cw = new ColumnValueWriter(f.name(), (n, v) -> writer.writeInt32(n, v.asInt()));
                break;
            }
            case TIME:
            case TIME_WITH_TIMEZONE:
                if (f.scale() > 0) {
                    cw = new ColumnValueWriter(f.name(), (n, v) -> writer.writeInt64(n, v.asInstant().toEpochMilli()));
                } else {
                    cw = new ColumnValueWriter(f.name(), (n, v) -> writer.writeInt32(n, v.asInt()));
                }
                break;
            case TIMESTAMP:
            case TIMESTAMP_WITH_TIMEZONE: {
                cw = new ColumnValueWriter(f.name(), (n, v) -> writer.writeDateTime(n, v.asInstant().toEpochMilli()));
                break;
            }
            case CHAR:
            case NCHAR:
            case VARCHAR:
            case NVARCHAR:
            case LONGVARCHAR:
            case LONGNVARCHAR:
            case CLOB: {
                cw = new ColumnValueWriter(f.name(), (n, v) -> writer.writeString(n, v.asString()));
                break;
            }
            // complex types
            // case ARRAY:
            // type = new ArrowType.List();
            // break;
            // case NULL:
            // type = new ArrowType.Null();
            // break;
            // case STRUCT:
            // type = new ArrowType.Struct();
            // break;
            default:
                // no-op, shouldn't get here
                throw new UnsupportedOperationException("Unmapped JDBC type: " + f.type());
        }
        return cw;
    }

    public BsonSerde(Properties config) {
        super();
    }

    @Override
    public Result<?> deserialize(InputStream in) throws IOException {
        throw new UnsupportedOperationException("Unimplemented method 'deserialize'");
    }

    @Override
    public void serialize(Result<?> result, OutputStream out) throws IOException {
        final List<Field> resultFields = result.fields();
        final int len = resultFields.size();

        try (OutputBuffer o = new BasicOutputBuffer()) {
            final BsonWriter writer = new BsonBinaryWriter(o);
            final ColumnValueWriter[] consumers = new ColumnValueWriter[len];
            for (int i = 0; i < len; i++) {
                consumers[i] = newValueWriter(writer, resultFields.get(i));
            }

            for (Row r : result.rows()) {
                writer.writeStartDocument();
                for (int i = 0; i < len; i++) {
                    consumers[i].write(r.value(i));
                }
                writer.writeEndDocument();
            }
            o.pipe(out);
        }
    }
}
