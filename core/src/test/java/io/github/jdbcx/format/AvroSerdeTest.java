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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.JDBCType;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.FieldBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

import io.github.jdbcx.Constants;
import io.github.jdbcx.Field;
import io.github.jdbcx.Result;
import io.github.jdbcx.Row;

public class AvroSerdeTest {
    @Test(groups = { "unit" })
    public void testNewField() {
        final FieldBuilder<Schema> b = SchemaBuilder.record("result").fields().name("field");

        Assert.assertThrows(NullPointerException.class, () -> AvroSerde.newField(null, (Schema.Type) null, false));
        Assert.assertThrows(NullPointerException.class, () -> AvroSerde.newField(b, (Schema.Type) null, false));
        Assert.assertThrows(NullPointerException.class, () -> AvroSerde.newField(null, (Schema) null, false));
        Assert.assertThrows(NullPointerException.class, () -> AvroSerde.newField(b, (Schema) null, false));

        Schema schema = AvroSerde.newField(b, Schema.Type.INT, false).endRecord();
        Assert.assertEquals(schema.toString(),
                "{\"type\":\"record\",\"name\":\"result\",\"fields\":[{\"name\":\"field\",\"type\":\"int\"}]}");

        FieldBuilder<Schema> builder = SchemaBuilder.record("result").fields().name("field");
        schema = AvroSerde.newField(builder, Schema.Type.INT, true).endRecord();
        Assert.assertEquals(schema.toString(),
                "{\"type\":\"record\",\"name\":\"result\",\"fields\":[{\"name\":\"field\",\"type\":[\"null\",\"int\"]}]}");

        Schema decimal183 = LogicalTypes.decimal(18, 3).addToSchema(Schema.create(Schema.Type.BYTES));
        builder = SchemaBuilder.record("result").fields().name("field");
        schema = AvroSerde.newField(builder, decimal183, false).endRecord();
        Assert.assertEquals(schema.toString(),
                "{\"type\":\"record\",\"name\":\"result\",\"fields\":[{\"name\":\"field\",\"type\":{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":18,\"scale\":3}}]}");

        builder = SchemaBuilder.record("result").fields().name("field");
        schema = AvroSerde.newField(builder, decimal183, true).endRecord();
        Assert.assertEquals(schema.toString(),
                "{\"type\":\"record\",\"name\":\"result\",\"fields\":[{\"name\":\"field\",\"type\":[\"null\",{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":18,\"scale\":3}]}]}");
    }

    @Test(groups = { "unit" })
    public void testBuildSchema() {
        Assert.assertThrows(NullPointerException.class, () -> AvroSerde.buildSchema(null));

        Assert.assertEquals(
                AvroSerde.buildSchema(Result.of(Row.of(Collections.emptyList(), Collections.emptyList()))).toString(),
                "{\"type\":\"record\",\"name\":\"Result\",\"namespace\":\"io.github.jdbcx\",\"fields\":[]}");
        Assert.assertEquals(
                AvroSerde
                        .buildSchema(Result.of(Constants.EMPTY_STRING_ARRAY, Field.of("f1", JDBCType.BIGINT, false)))
                        .toString(),
                "{\"type\":\"record\",\"name\":\"Result\",\"namespace\":\"io.github.jdbcx\",\"fields\":[{\"name\":\"f1\",\"type\":\"long\"}]}");

        Assert.assertEquals(
                AvroSerde.buildSchema(Result.of(Constants.EMPTY_STRING_ARRAY, Field.of("", JDBCType.BIGINT, false),
                        Field.of("f2", "", JDBCType.BIGINT, false, 0, 0, false))).toString(),
                "{\"type\":\"record\",\"name\":\"Result\",\"namespace\":\"io.github.jdbcx\",\"fields\":[{\"name\":\"f1\",\"type\":\"long\"},{\"name\":\"f2\",\"type\":{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":19,\"scale\":0}}]}");
    }

    @Test(groups = { "unit" })
    public void testSerialize() throws IOException {
        Properties config = new Properties();
        AvroSerde serde = new AvroSerde(config);
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            serde.serialize(Result.of("123"), out);
            Assert.assertEquals(out.toByteArray().length, 177);
        }
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            serde.serialize(Result.of(Arrays.asList(Field.of(""), Field.of("1", JDBCType.INTEGER)),
                    new Object[][] { { null, 1 }, { "123", null } }), out);
            Assert.assertEquals(out.toByteArray().length, 214);
        }

        AvroSerde.OPTION_ENCODER.setValue(config, AvroSerde.ENCODER_BINARY);
        serde = new AvroSerde(config);
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            serde.serialize(Result.of("123"), out);
            Assert.assertEquals(out.toByteArray(), new byte[] { 2, 6, 49, 50, 51 });
        }
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            serde.serialize(Result.of(Arrays.asList(Field.of(""), Field.of("1", JDBCType.INTEGER)),
                    new Object[][] { { null, 1 }, { "123", null } }), out);
            Assert.assertEquals(out.toByteArray(), new byte[] { 0, 2, 2, 2, 6, 49, 50, 51, 0 });
        }

        AvroSerde.OPTION_ENCODER.setValue(config, AvroSerde.ENCODER_JSON);
        serde = new AvroSerde(config);
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            serde.serialize(Result.of("123"), out);
            Assert.assertEquals(new String(out.toByteArray()), "{\"results\":{\"string\":\"123\"}}");
        }
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            serde.serialize(Result.of(Arrays.asList(Field.of(""), Field.of("1", JDBCType.INTEGER)),
                    new Object[][] { { null, 1 }, { "123", null } }), out);
            Assert.assertEquals(new String(out.toByteArray()),
                    "{\"f1\":null,\"f2_1\":{\"int\":1}}\n{\"f1\":{\"string\":\"123\"},\"f2_1\":null}");
        }
    }
}
