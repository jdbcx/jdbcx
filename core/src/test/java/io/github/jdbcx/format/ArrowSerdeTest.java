/*
 * Copyright 2022-2026, Zhichun Wu
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
import java.util.List;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.github.jdbcx.Checker;
import io.github.jdbcx.Constants;
import io.github.jdbcx.Field;
import io.github.jdbcx.Result;

public class ArrowSerdeTest {
    @Test(groups = { "unit" })
    public void testSerialize() throws IOException {
        Properties config = new Properties();
        ArrowSerde serde = null;
        final List<Integer> expectedBinaryLengths = Collections
                .unmodifiableList(Arrays.asList(730, 1250, 994, 730, 730));
        final List<Integer> expectedStreamBinaryLengths = Collections
                .unmodifiableList(Arrays.asList(472, 944, 712, 472, 472));
        for (String reset : new String[] { "", "true", "false" }) {
            config.clear();
            if (!Checker.isNullOrEmpty(reset)) {
                ArrowSerde.OPTION_RESET.setValue(config, reset);
            }
            for (int i = 0; i < 5; i++) {
                config.remove(ArrowSerde.OPTION_STREAM.getName());
                if (i != 0) {
                    ArrowSerde.OPTION_ROWS.setValue(config, Integer.toString(i));
                }
                serde = new ArrowSerde(config);
                try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
                    serde.serialize(Result.of("123"), out);
                    Assert.assertEquals(out.toByteArray().length, 538, config.toString());
                }
                try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
                    serde.serialize(Result.of(Arrays.asList(Field.of(""), Field.of("1", JDBCType.INTEGER)),
                            new Object[][] { { null, 1 }, { "123", null }, { "456", 2 } }), out);
                    Assert.assertEquals(out.toByteArray().length, expectedBinaryLengths.get(i), config.toString());
                }

                ArrowSerde.OPTION_STREAM.setValue(config, Constants.TRUE_EXPR);
                serde = new ArrowSerde(config);
                try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
                    serde.serialize(Result.of("123"), out);
                    Assert.assertEquals(out.toByteArray().length, 336, config.toString());
                }
                try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
                    serde.serialize(Result.of(Arrays.asList(Field.of(""), Field.of("1", JDBCType.INTEGER)),
                            new Object[][] { { null, 1 }, { "123", null }, { "456", 2 } }), out);
                    Assert.assertEquals(out.toByteArray().length, expectedStreamBinaryLengths.get(i),
                            config.toString());
                }
            }
        }
    }
}
