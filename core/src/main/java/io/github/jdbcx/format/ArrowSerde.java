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
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;
import java.util.function.BiConsumer;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.Decimal256Vector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.TimeStampSecVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import io.github.jdbcx.Option;
import io.github.jdbcx.Result;
import io.github.jdbcx.Serialization;
import io.github.jdbcx.Value;

public class ArrowSerde implements Serialization {
    public static final Option OPTION_BATCH = Option
            .of(new String[] { "batch", "Batch size for serialization", "100" });

    static final class VectorConsumer {
        final FieldVector vector;
        final BiConsumer<Integer, Value> consumer;

        VectorConsumer(FieldVector vector, BiConsumer<Integer, Value> consumer) {
            this.vector = vector;
            this.consumer = consumer;
        }
    }

    static List<VectorConsumer> buildArrowSchemaRoot(BufferAllocator allocator, List<io.github.jdbcx.Field> fields) {
        final int len = fields.size();
        List<VectorConsumer> list = new ArrayList<>();
        for (int i = 0; i < len; i++) {
            VectorConsumer vc = getArrowVectorConsumer(allocator, fields.get(i), null);
            list.add(vc);
        }
        return list;
    }

    static VectorConsumer getArrowVectorConsumer(BufferAllocator allocator, io.github.jdbcx.Field f,
            TimeZone timeZone) {
        final VectorConsumer vc;
        switch (f.type()) {
            case BOOLEAN: {
                final BitVector vector = new BitVector(f.name(),
                        new FieldType(f.isNullable(), new ArrowType.Bool(), null), allocator);
                vc = new VectorConsumer(vector, (i, v) -> vector.set(i, v.asInt()));
                break;
            }
            case BIT:
                if (f.precision() > 1) {
                    final VarBinaryVector vector = new VarBinaryVector(f.name(),
                            new FieldType(f.isNullable(), new ArrowType.Binary(), null), allocator);
                    vc = new VectorConsumer(vector, (i, v) -> vector.set(i, v.asBinary()));
                } else {
                    final BitVector vector = new BitVector(f.name(),
                            new FieldType(f.isNullable(), new ArrowType.Bool(), null), allocator);
                    vc = new VectorConsumer(vector, (i, v) -> vector.set(i, v.asInt()));
                }
                break;
            case BINARY:
            case VARBINARY:
            case LONGVARBINARY:
            case BLOB: {
                final VarBinaryVector vector = new VarBinaryVector(f.name(),
                        new FieldType(f.isNullable(), new ArrowType.Binary(), null), allocator);
                vc = new VectorConsumer(vector, (i, v) -> vector.set(i, v.asBinary()));
                break;
            }
            case TINYINT: {
                final IntVector vector = new IntVector(f.name(),
                        new FieldType(f.isNullable(), new ArrowType.Int(8, f.isSigned()), null), allocator);
                vc = new VectorConsumer(vector, (i, v) -> vector.set(i, v.asInt()));
                break;
            }
            case SMALLINT: {
                final IntVector vector = new IntVector(f.name(),
                        new FieldType(f.isNullable(), new ArrowType.Int(16, f.isSigned()), null), allocator);
                vc = new VectorConsumer(vector, (i, v) -> vector.set(i, v.asInt()));
                break;
            }
            case INTEGER: {
                final IntVector vector = new IntVector(f.name(),
                        new FieldType(f.isNullable(), new ArrowType.Int(32, f.isSigned()), null), allocator);
                vc = new VectorConsumer(vector, (i, v) -> vector.set(i, v.asInt()));
                break;
            }
            case BIGINT: {
                final BigIntVector vector = new BigIntVector(f.name(),
                        new FieldType(f.isNullable(), new ArrowType.Int(64, f.isSigned()), null), allocator);
                vc = new VectorConsumer(vector, (i, v) -> vector.set(i, v.asLong()));
                break;
            }
            case REAL:
            case FLOAT: {
                final Float4Vector vector = new Float4Vector(f.name(),
                        new FieldType(f.isNullable(), new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), null),
                        allocator);
                vc = new VectorConsumer(vector, (i, v) -> vector.set(i, v.asFloat()));
                break;
            }
            case DOUBLE: {
                final Float8Vector vector = new Float8Vector(f.name(),
                        new FieldType(f.isNullable(), new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), null),
                        allocator);
                vc = new VectorConsumer(vector, (i, v) -> vector.set(i, v.asDouble()));
                break;
            }
            case NUMERIC:
            case DECIMAL:
                if (f.precision() > 38) {
                    final Decimal256Vector vector = new Decimal256Vector(f.name(),
                            new FieldType(f.isNullable(), new ArrowType.Decimal(f.precision(), f.scale(), 256), null),
                            allocator);
                    vc = new VectorConsumer(vector, (i, v) -> vector.set(i, v.asBigDecimal()));
                    break;
                } else {
                    final DecimalVector vector = new DecimalVector(f.name(),
                            new FieldType(f.isNullable(), new ArrowType.Decimal(f.precision(), f.scale(), 128), null),
                            allocator);
                    vc = new VectorConsumer(vector, (i, v) -> vector.set(i, v.asBigDecimal()));
                    break;
                }
            case DATE: {
                final DateDayVector vector = new DateDayVector(f.name(),
                        new FieldType(f.isNullable(), new ArrowType.Date(DateUnit.DAY), null), allocator);
                vc = new VectorConsumer(vector, (i, v) -> vector.set(i, v.asInt()));
                break;
            }
            case TIME:
            case TIME_WITH_TIMEZONE:
                if (f.scale() > 6) {
                    final TimeNanoVector vector = new TimeNanoVector(f.name(),
                            new FieldType(f.isNullable(), new ArrowType.Time(TimeUnit.NANOSECOND, 64), null),
                            allocator);
                    vc = new VectorConsumer(vector, (i, v) -> vector.set(i, v.asLong()));
                } else if (f.scale() > 3) {
                    final TimeMicroVector vector = new TimeMicroVector(f.name(),
                            new FieldType(f.isNullable(), new ArrowType.Time(TimeUnit.MICROSECOND, 64), null),
                            allocator);
                    vc = new VectorConsumer(vector, (i, v) -> vector.set(i, v.asLong()));
                } else if (f.scale() > 0) {
                    final TimeMilliVector vector = new TimeMilliVector(f.name(),
                            new FieldType(f.isNullable(), new ArrowType.Time(TimeUnit.MILLISECOND, 32), null),
                            allocator);
                    vc = new VectorConsumer(vector, (i, v) -> vector.set(i, v.asInt()));
                } else {
                    final TimeSecVector vector = new TimeSecVector(f.name(),
                            new FieldType(f.isNullable(), new ArrowType.Time(TimeUnit.SECOND, 32), null),
                            allocator);
                    vc = new VectorConsumer(vector, (i, v) -> vector.set(i, v.asInt()));
                }
                break;
            case TIMESTAMP:
            case TIMESTAMP_WITH_TIMEZONE:
                final String tzId = timeZone != null ? timeZone.getID() : null;
                if (f.scale() > 6) {
                    final TimeStampNanoVector vector = new TimeStampNanoVector(f.name(),
                            new FieldType(f.isNullable(), new ArrowType.Timestamp(TimeUnit.NANOSECOND, tzId), null),
                            allocator);
                    vc = new VectorConsumer(vector, (i, v) -> vector.set(i, v.asLong()));
                } else if (f.scale() > 3) {
                    final TimeStampMicroVector vector = new TimeStampMicroVector(f.name(),
                            new FieldType(f.isNullable(), new ArrowType.Timestamp(TimeUnit.MICROSECOND, tzId), null),
                            allocator);
                    vc = new VectorConsumer(vector, (i, v) -> vector.set(i, v.asLong()));
                } else if (f.scale() > 0) {
                    final TimeStampMilliVector vector = new TimeStampMilliVector(f.name(),
                            new FieldType(f.isNullable(), new ArrowType.Timestamp(TimeUnit.MILLISECOND, tzId), null),
                            allocator);
                    vc = new VectorConsumer(vector, (i, v) -> vector.set(i, v.asInt()));
                } else {
                    final TimeStampSecVector vector = new TimeStampSecVector(f.name(),
                            new FieldType(f.isNullable(), new ArrowType.Timestamp(TimeUnit.SECOND, tzId), null),
                            allocator);
                    vc = new VectorConsumer(vector, (i, v) -> vector.set(i, v.asInt()));
                }
                break;
            case CHAR:
            case NCHAR:
            case VARCHAR:
            case NVARCHAR:
            case LONGVARCHAR:
            case LONGNVARCHAR:
            case CLOB: {
                final VarCharVector vector = new VarCharVector(f.name(),
                        new FieldType(f.isNullable(), new ArrowType.Utf8(), null), allocator);
                vc = new VectorConsumer(vector, (i, v) -> vector.set(i, v.asBinary()));
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
        return vc;
    }

    protected final int batchSize;

    public ArrowSerde(Properties config) {
        this.batchSize = Integer.parseInt(OPTION_BATCH.getValue(config));
    }

    @Override
    public Result<?> deserialize(InputStream in) throws IOException {
        throw new UnsupportedOperationException("Unimplemented method 'deserialize'");
    }

    @Override
    public void serialize(Result<?> result, OutputStream out) throws IOException {
        // https://arrow.apache.org/cookbook/java/io.html#write-out-to-buffer
        // https://github.com/apache/arrow/blob/main/java/adapter/jdbc/src/main/java/org/apache/arrow/adapter/jdbc/JdbcToArrow.java
        final List<io.github.jdbcx.Field> resultFields = result.fields();
        final int len = resultFields.size();
        try (BufferAllocator allocator = new RootAllocator()) {
            final List<VectorConsumer> list = buildArrowSchemaRoot(allocator, resultFields);
            final List<Field> fields = new ArrayList<>(len);
            final List<FieldVector> vectors = new ArrayList<>(len);
            final List<BiConsumer<Integer, Value>> consumers = new ArrayList<>(len);
            for (int i = 0; i < len; i++) {
                VectorConsumer vc = list.get(i);
                FieldVector vector = vc.vector;
                fields.add(vector.getField());
                vectors.add(vector);
                consumers.add(vc.consumer);
            }

            final int size = this.batchSize;
            try (VectorSchemaRoot root = new VectorSchemaRoot(fields, vectors, 0);
                    ArrowFileWriter writer = new ArrowFileWriter(root, null, Channels.newChannel(out))) {
                writer.start();

                int index = 0;
                for (io.github.jdbcx.Row r : result.rows()) {
                    if (index == 0) {
                        root.allocateNew();
                    }

                    for (int i = 0; i < len; i++) {
                        BiConsumer<Integer, Value> consumer = consumers.get(i);
                        Value value = r.value(i);
                        if (value.isNull()) {
                            vectors.get(i).setNull(index);
                        } else {
                            consumer.accept(index, value);
                        }
                    }

                    if (++index >= size) {
                        root.setRowCount(index);
                        writer.writeBatch();
                        root.clear();
                        index = 0;
                    }
                }
                if (index > 0) {
                    root.setRowCount(index);
                    writer.writeBatch();
                    root.clear();
                }
                writer.end();
            }
        }
    }
}
