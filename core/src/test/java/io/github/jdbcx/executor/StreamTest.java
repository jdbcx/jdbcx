/*
 * Copyright 2022-2023, Zhichun Wu
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
package io.github.jdbcx.executor;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import org.testng.Assert;
import org.testng.annotations.Test;

public class StreamTest {
    @Test(groups = "unit")
    public void testPipeBinary() throws IOException {
        String str = "test\1\2\3";
        Charset charset = StandardCharsets.US_ASCII;
        try (ByteArrayInputStream in = new ByteArrayInputStream(str.getBytes(charset));
                ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            Assert.assertEquals(Stream.pipe(in, out), 7L);
            Assert.assertEquals(new String(out.toByteArray(), charset), str);
        }

        try (ByteArrayInputStream in = new ByteArrayInputStream(str.getBytes(charset));
                ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[1];
            Assert.assertEquals(Stream.pipe(in, out, buffer, true), 7L);
            Assert.assertEquals(new String(out.toByteArray(), charset), str);
        }

        try (ByteArrayInputStream in = new ByteArrayInputStream(str.getBytes(charset));
                ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[2];
            Assert.assertEquals(Stream.pipe(in, out, buffer, false), 7L);
            Assert.assertEquals(new String(out.toByteArray(), charset), str);
        }
    }

    @Test(groups = "unit")
    public void testPipeText() throws IOException {
        String str = "test\t123\r456\n!";
        Charset charset = StandardCharsets.UTF_8;
        try (Reader reader = new InputStreamReader(new ByteArrayInputStream(str.getBytes(charset)), charset);
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                Writer writer = new OutputStreamWriter(out, charset)) {
            Assert.assertEquals(Stream.pipe(reader, writer), str.toCharArray().length);
            Assert.assertEquals(new String(out.toByteArray(), charset), str);
        }

        try (Reader reader = new InputStreamReader(new ByteArrayInputStream(str.getBytes(charset)), charset);
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                Writer writer = new OutputStreamWriter(out, charset)) {
            char[] buffer = new char[1];
            Assert.assertEquals(Stream.pipe(reader, writer, buffer, true), str.toCharArray().length);
            Assert.assertEquals(new String(out.toByteArray(), charset), str);
        }

        try (Reader reader = new InputStreamReader(new ByteArrayInputStream(str.getBytes(charset)), charset);
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                Writer writer = new OutputStreamWriter(out, charset)) {
            char[] buffer = new char[2];
            Assert.assertEquals(Stream.pipe(reader, writer, buffer, false), str.toCharArray().length);
            Assert.assertEquals(new String(out.toByteArray(), charset), str);
        }
    }
}
