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

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.Conversions.DecimalConversion;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.avro.AvroWriteSupport;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

import io.github.jdbcx.Checker;
import io.github.jdbcx.Option;
import io.github.jdbcx.Result;
import io.github.jdbcx.Row;
import io.github.jdbcx.Serialization;
import io.github.jdbcx.Value;

public class ParquetSerde implements Serialization {
    static final class ParquetBufferedWriter implements OutputFile {
        private final BufferedOutputStream out;

        ParquetBufferedWriter(BufferedOutputStream out) {
            this.out = out;
        }

        private PositionOutputStream createPositionOutputstream() {
            return new PositionOutputStream() {
                private long pos = 0L;

                @Override
                public long getPos() throws IOException {
                    return pos;
                }

                @Override
                public void flush() throws IOException {
                    out.flush();
                }

                @Override
                public void close() throws IOException {
                    out.close();
                }

                @Override
                public void write(int b) throws IOException {
                    out.write(b);
                    pos++;
                }

                @Override
                public void write(byte[] b, int off, int len) throws IOException {
                    out.write(b, off, len);
                    pos += len;
                }
            };
        }

        @Override
        public PositionOutputStream create(long blockSizeHint) throws IOException {
            return createPositionOutputstream();
        }

        @Override
        public PositionOutputStream createOrOverwrite(long blockSizeHint) throws IOException {
            return createPositionOutputstream();
        }

        @Override
        public boolean supportsBlockSize() {
            return false;
        }

        @Override
        public long defaultBlockSize() {
            return 0L;
        }
    }

    public static final Option OPTION_COMPRSESION = Option.of(new String[] { "compression", "Compression codec" });

    protected final CompressionCodecName codec;
    protected final int buffer;

    public ParquetSerde(Properties config) {
        String value = OPTION_COMPRSESION.getValue(config);
        codec = Checker.isNullOrEmpty(value) ? CompressionCodecName.UNCOMPRESSED : CompressionCodecName.fromConf(value);
        value = OPTION_BUFFER.getValue(config);
        buffer = Checker.isNullOrEmpty(value) ? 8192 : Integer.parseInt(value);
    }

    @Override
    public Result<?> deserialize(InputStream in) throws IOException {
        throw new UnsupportedOperationException("Unimplemented method 'deserialize'");
    }

    @Override
    public void serialize(Result<?> result, OutputStream out) throws IOException {
        final Schema schema = AvroSerde.buildSchema(result);
        final DecimalConversion decimalConverter = new DecimalConversion();
        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter
                .<GenericRecord>builder(
                        new ParquetBufferedWriter(out instanceof BufferedOutputStream ? (BufferedOutputStream) out
                                : new BufferedOutputStream(out, buffer)))
                .withSchema(schema).withCompressionCodec(codec)
                .config(AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE, "false").build()) {
            final int len = result.fields().size();
            final GenericRecord data = new GenericData.Record(schema);
            for (Row r : result.rows()) {
                for (int i = 0; i < len; i++) {
                    Value v = r.value(i);
                    if (v.isNull()) {
                        data.put(i, null);
                    } else {
                        data.put(i, AvroSerde.getValue(schema, decimalConverter, r.field(i), v));
                    }
                }
                writer.write(data);
            }
        }
    }
}
