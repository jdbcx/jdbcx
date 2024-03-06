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
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Properties;

import org.apache.avro.Conversions.DecimalConversion;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.FieldAssembler;
import org.apache.avro.SchemaBuilder.FieldBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;

import io.github.jdbcx.Checker;
import io.github.jdbcx.Constants;
import io.github.jdbcx.Field;
import io.github.jdbcx.Format;
import io.github.jdbcx.Option;
import io.github.jdbcx.Result;
import io.github.jdbcx.Row;
import io.github.jdbcx.Serialization;
import io.github.jdbcx.Utils;
import io.github.jdbcx.Value;

public class AvroSerde implements Serialization {
    public static final String ENCODER_BINARY = "binary";
    public static final String ENCODER_JSON = "json";

    public static final Option OPTION_ENCODER = Option
            .of(new String[] { "encoder",
                    "Preferred encoder(" + ENCODER_BINARY + " or " + ENCODER_JSON
                            + ") to use, empty means no encoder will be used",
                    Constants.EMPTY_STRING, ENCODER_BINARY, ENCODER_JSON });

    static FieldAssembler<Schema> newField(FieldBuilder<Schema> builder, Schema.Type type, boolean nullable) {
        return nullable ? builder.type().unionOf().nullType().and().type(Schema.create(type)).endUnion().noDefault()
                : builder.type(Schema.create(type)).noDefault();
    }

    static FieldAssembler<Schema> newField(FieldBuilder<Schema> builder, Schema schema, boolean nullable) {
        return nullable ? builder.type().unionOf().nullType().and().type(schema).endUnion().noDefault()
                : builder.type(schema).noDefault();
    }

    // TODO 1) cache the schema; and 2) persist result and only refresh when needed
    static Schema buildSchema(Result<?> result) {
        final Class<?> clazz = result.getClass();
        SchemaBuilder.FieldAssembler<Schema> fieldAssembler = SchemaBuilder.builder(clazz.getPackage().getName())
                .record(clazz.getSimpleName()).fields();
        List<Field> fields = result.fields();
        for (int i = 0, size = fields.size(); i < size; i++) {
            Field f = fields.get(i);
            FieldBuilder<Schema> builder = fieldAssembler.name(Format.normalizeAvroField(i + 1, f.name()));
            switch (f.type()) {
                case BOOLEAN:
                    fieldAssembler = newField(builder, Schema.Type.BOOLEAN, f.isNullable());
                    break;
                case BIT:
                case BLOB:
                case BINARY:
                case VARBINARY:
                    fieldAssembler = newField(builder, Schema.Type.BYTES, f.isNullable());
                    break;
                case TINYINT:
                case SMALLINT:
                    fieldAssembler = newField(builder, Schema.Type.INT, f.isNullable());
                    break;
                case INTEGER:
                    if (f.isSigned()) {
                        fieldAssembler = newField(builder, Schema.Type.INT, f.isNullable());
                    } else {
                        fieldAssembler = newField(builder, Schema.Type.LONG, f.isNullable());
                    }
                    break;
                case BIGINT:
                    if (f.isSigned()) {
                        fieldAssembler = newField(builder, Schema.Type.LONG, f.isNullable());
                    } else {
                        fieldAssembler = newField(builder, LogicalTypes.decimal(f.precision(), 0)
                                .addToSchema(Schema.create(Schema.Type.BYTES)), f.isNullable());
                    }
                    break;
                case REAL:
                case FLOAT:
                    fieldAssembler = newField(builder, Schema.Type.FLOAT, f.isNullable());
                    break;
                case DOUBLE:
                    fieldAssembler = newField(builder, Schema.Type.DOUBLE, f.isNullable());
                    break;
                case NUMERIC:
                case DECIMAL: {
                    fieldAssembler = newField(builder, LogicalTypes.decimal(f.precision(), f.scale())
                            .addToSchema(Schema.create(Schema.Type.BYTES)), f.isNullable());
                    break;
                }
                case DATE: {
                    fieldAssembler = newField(builder, LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT)),
                            f.isNullable());
                    break;
                }
                case TIME:
                case TIME_WITH_TIMEZONE: {
                    final Schema fieldSchema;
                    if (f.scale() > 3) {
                        fieldSchema = LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG));
                    } else {
                        fieldSchema = LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT));
                    }
                    fieldAssembler = newField(builder, fieldSchema, f.isNullable());
                    break;
                }
                case TIMESTAMP: {
                    final Schema fieldSchema;
                    if (f.scale() > 3) {
                        fieldSchema = LogicalTypes.localTimestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
                    } else {
                        fieldSchema = LogicalTypes.localTimestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
                    }
                    fieldAssembler = newField(builder, fieldSchema, f.isNullable());
                    break;
                }
                case TIMESTAMP_WITH_TIMEZONE: {
                    final Schema fieldSchema;
                    if (f.scale() > 3) {
                        fieldSchema = LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
                    } else {
                        fieldSchema = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
                    }
                    fieldAssembler = newField(builder, fieldSchema, f.isNullable());
                    break;
                }
                default:
                    fieldAssembler = newField(builder, Schema.Type.STRING, f.isNullable());
                    break;
            }
        }
        return fieldAssembler.endRecord();
    }

    static Object getValue(Schema schema, DecimalConversion decimalConverter, Field f, Value v) {
        final Object value;
        switch (f.type()) {
            case DATE:
                value = v.asInt();
                break;
            case TIME:
            case TIME_WITH_TIMEZONE:
                value = f.scale() > 3 ? v.asTime().toNanoOfDay() / 1_000L
                        : (int) (v.asTime().toNanoOfDay() / 1_000_000L);
                break;
            case TIMESTAMP:
            case TIMESTAMP_WITH_TIMEZONE:
                value = Utils.toEpochNanoSeconds(v.asDateTime(), v.getFactory().getZoneOffset())
                        / (f.scale() > 3 ? 1_000L : 1_000_000L);
                break;
            case BIGINT:
                if (f.isSigned()) {
                    value = v.asLong();
                } else {
                    value = decimalConverter.toBytes(v.asBigDecimal(), schema, LogicalTypes.decimal(f.precision()));
                }
                break;
            case NUMERIC:
            case DECIMAL:
                value = decimalConverter.toBytes(v.asBigDecimal(f.scale()), schema,
                        LogicalTypes.decimal(f.precision(), f.scale()));
                break;
            default:
                value = v.asObject();
                break;
        }
        return value;
    }

    protected final String encoder;

    public AvroSerde(Properties config) {
        encoder = OPTION_ENCODER.getValue(config);
    }

    @Override
    public Result<?> deserialize(InputStream in) throws IOException {
        throw new UnsupportedOperationException("Unimplemented method 'deserialize'");
    }

    @SuppressWarnings("resource")
    @Override
    public void serialize(Result<?> result, OutputStream out) throws IOException {
        final Schema schema = buildSchema(result);
        final DecimalConversion decimalConverter = new DecimalConversion();
        if (Checker.isNullOrEmpty(encoder)) {
            final int len = result.fields().size();
            final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
            try (DataFileWriter<GenericRecord> writer = new DataFileWriter<>(datumWriter).create(schema, out)) {
                final GenericRecord data = new GenericData.Record(schema);
                for (Row r : result.rows()) {
                    for (int i = 0; i < len; i++) {
                        Value v = r.value(i);
                        if (v.isNull()) {
                            data.put(i, null);
                        } else {
                            data.put(i, getValue(schema, decimalConverter, r.field(i), v));
                        }
                    }
                    writer.append(data);
                }
            }
        } else {
            final DatumWriter<GenericRecord> writer = new SpecificDatumWriter<>(schema);
            final Encoder enc = ENCODER_JSON.equals(encoder) ? EncoderFactory.get().jsonEncoder(schema, out)
                    : EncoderFactory.get().binaryEncoder(out, null);
            final int len = result.fields().size();
            try {
                final GenericRecord data = new GenericData.Record(schema);
                for (Row r : result.rows()) {
                    for (int i = 0; i < len; i++) {
                        Value v = r.value(i);
                        if (v.isNull()) {
                            data.put(i, null);
                        } else {
                            data.put(i, getValue(schema, decimalConverter, r.field(i), v));
                        }
                    }
                    writer.write(data, enc);
                }
            } finally {
                try {
                    enc.flush();
                } catch (IOException e) {
                    // ignore
                }
            }
        }
    }
}
