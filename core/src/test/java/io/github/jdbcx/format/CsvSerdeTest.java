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
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.github.jdbcx.Field;
import io.github.jdbcx.Result;
import io.github.jdbcx.Serialization;

public class CsvSerdeTest {
    @Test(groups = { "unit" })
    public void testSerialize() throws IOException {
        final Result<?> result = Result.of(Arrays.asList(Field.of("a,b"), Field.of("\"c")), new Object[][] {
                { null, null }, { "1,2\n3", "," }
        });
        final String expected = "\"a,b\",\"\"\"c\"\n,\n\"1,2\n3\",\",\"";

        Properties config = new Properties();
        Serialization serde = new CsvSerde(config);
        Charset charset = ((CsvSerde) serde).charset;
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            serde.serialize(result, out);
            Assert.assertEquals(new String(out.toByteArray(), charset), expected);
        }

        TextSerde.OPTION_HEADER.setValue(config, "false");
        serde = new CsvSerde(config);
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            serde.serialize(result, out);
            Assert.assertEquals(new String(out.toByteArray(), charset), ",\n\"1,2\n3\",\",\"");
        }

        config = new Properties();
        TextSerde.OPTION_BUFFER.setValue(config, "-1");
        serde = new CsvSerde(config);
        charset = ((CsvSerde) serde).charset;
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            serde.serialize(result, out);
            Assert.assertEquals(new String(out.toByteArray(), charset), expected);
        }

        CsvSerde.OPTION_USEQUOTES.setValue(config, "true");
        serde = new CsvSerde(config);
        charset = ((CsvSerde) serde).charset;
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            serde.serialize(result, out);
            Assert.assertEquals(new String(out.toByteArray(), charset),
                    "\"a,b\",\"\"\"c\"\n\"\",\"\"\n\"1,2\n3\",\",\"");
        }
    }
}
