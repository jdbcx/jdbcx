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

public class JsonlSerdeTest {
    @Test(groups = { "unit" })
    public void testSerialize() throws IOException {
        final Result<?> result = Result.of(Arrays.asList(Field.of("a\tb"), Field.of("\\c")), new Object[][] {
                { null, null }, { "1\t2\n3", "\t" }
        });
        final String expectedNvs = "{\"a\\tb\":null,\"\\\\c\":null}\n{\"a\\tb\":\"1\\t2\\n3\",\"\\\\c\":\"\\t\"}";
        final String expectedVals = "[null,null]\n[\"1\\t2\\n3\",\"\\t\"]";

        Properties config = new Properties();
        Serialization serde = new JsonlSerde(config);
        Charset charset = ((JsonlSerde) serde).charset;
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            serde.serialize(result, out);
            Assert.assertEquals(new String(out.toByteArray(), charset), expectedNvs);
        }

        JsonlSerde.OPTION_HEADER.setValue(config, "false");
        serde = new JsonlSerde(config);
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            serde.serialize(result, out);
            Assert.assertEquals(new String(out.toByteArray(), charset), expectedVals);
        }

        config = new Properties();
        JsonlSerde.OPTION_NULL_VALUE.setValue(config, "123");
        serde = new JsonlSerde(config);
        charset = ((JsonlSerde) serde).charset;
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            serde.serialize(result, out);
            Assert.assertEquals(new String(out.toByteArray(), charset), expectedNvs);
        }
    }
}
