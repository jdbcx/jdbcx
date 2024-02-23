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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.github.jdbcx.Result;
import io.github.jdbcx.Row;
import io.github.jdbcx.Serialization;
import io.github.jdbcx.executor.Stream;

public class TextSerdeTest {
    static class MyTextSerde extends TextSerde {
        protected MyTextSerde(Properties config) {
            super(config);
        }

        @Override
        public Result<?> deserialize(Reader reader) throws IOException {
            return Result.of(Stream.readAllAsString(reader));
        }

        @Override
        public void serialize(Result<?> result, Writer writer) throws IOException {
            writer.write(result.toString());
        }

    }

    @Test(groups = { "unit" })
    public void testConstructor() {
        MyTextSerde serde = new MyTextSerde(null);
        Assert.assertEquals(serde.buffer, 2048);
        Assert.assertEquals(serde.charset, StandardCharsets.UTF_8);
        Assert.assertEquals(serde.header, true);
        Assert.assertEquals(serde.nullValue, "");

        Properties props = new Properties();
        serde = new MyTextSerde(props);
        Assert.assertEquals(serde.buffer, 2048);
        Assert.assertEquals(serde.charset, StandardCharsets.UTF_8);
        Assert.assertEquals(serde.header, true);
        Assert.assertEquals(serde.nullValue, "");

        TextSerde.OPTION_BUFFER.setValue(props, "-1");
        TextSerde.OPTION_CHARSET.setValue(props, "gb18030");
        TextSerde.OPTION_HEADER.setValue(props, "false");
        TextSerde.OPTION_NULL_VALUE.setValue(props, "\\N");
        serde = new MyTextSerde(props);
        Assert.assertEquals(serde.buffer, -1);
        Assert.assertEquals(serde.charset, Charset.forName("gb18030"));
        Assert.assertEquals(serde.header, false);
        Assert.assertEquals(serde.nullValue, "\\N");
    }

    @Test(groups = { "unit" })
    public void testSerde() throws IOException {
        final Result<?> result = Result.of("123");

        Properties config = new Properties();
        Serialization serde = new MyTextSerde(config);
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            serde.serialize(result, out);
            Assert.assertEquals(new String(out.toByteArray(), ((MyTextSerde) serde).charset), result.toString());
        }

        try (ByteArrayInputStream in = new ByteArrayInputStream(
                result.toString().getBytes(((MyTextSerde) serde).charset))) {
            Result<?> r = serde.deserialize(in);
            Iterator<Row> it = r.rows().iterator();
            Assert.assertEquals(it.hasNext(), true);
            Row row = it.next();
            Assert.assertEquals(row.size(), 1);
            Assert.assertEquals(row.value(0).asString(), result.toString());
            Assert.assertEquals(it.hasNext(), false);
        }
    }
}
