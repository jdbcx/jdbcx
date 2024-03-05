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
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.github.jdbcx.Result;

public class ArrowSerdeTest {
    @Test(groups = { "unit" })
    public void testSerialize() throws IOException {
        Properties config = new Properties();
        ArrowSerde serde = new ArrowSerde(config);
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            serde.serialize(Result.of("123"), out);
            Assert.assertEquals(out.toByteArray().length, 538);
        }
    }
}
