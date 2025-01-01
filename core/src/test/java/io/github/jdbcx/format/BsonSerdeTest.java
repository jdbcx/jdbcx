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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.JDBCType;
import java.util.Arrays;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.github.jdbcx.Field;
import io.github.jdbcx.Result;

public class BsonSerdeTest {
    @Test(groups = { "unit" })
    public void testSerialize() throws IOException {
        Properties config = new Properties();
        BsonSerde serde = new BsonSerde(config);
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            serde.serialize(Result.of("123"), out);
            Assert.assertEquals(out.toByteArray().length, 22);
        }
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            serde.serialize(Result.of(Arrays.asList(Field.of(""), Field.of("1", JDBCType.INTEGER)),
                    new Object[][] { { null, 1 }, { "123", null }, { "456", 2 } }), out);
            Assert.assertEquals(out.toByteArray().length, 63);
        }

        serde = new BsonSerde(config);
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            serde.serialize(Result.of(Arrays.asList(Field.of(""), Field.of("1", JDBCType.INTEGER)),
                    new Object[][] { { null, 1 }, { "123", null }, { "456", 2 } }), out);
            Assert.assertEquals(out.toByteArray().length, 63);
        }
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            serde.serialize(Result.of("123"), out);
            Assert.assertEquals(out.toByteArray().length, 22);
        }
    }
}
