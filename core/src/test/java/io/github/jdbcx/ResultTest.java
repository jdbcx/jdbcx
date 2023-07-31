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
package io.github.jdbcx;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ResultTest {
    @Test(groups = "unit")
    public void testInputStream() {
        Assert.assertThrows(IllegalArgumentException.class, () -> Result.of((InputStream) null));

        Assert.assertEquals(Result.of(new ByteArrayInputStream(new byte[0])).type(), ByteArrayInputStream.class);
        ByteArrayInputStream in = new ByteArrayInputStream(new byte[0]);
        Assert.assertEquals(Result.of(in).get(), in);
        Assert.assertEquals(Result.of(in).get(InputStream.class), in);
        Assert.assertEquals(Result.of(new ByteArrayInputStream(new byte[0])).get(String.class), "");
        for (Row row : Result.of(new ByteArrayInputStream(new byte[0])).rows()) {
            Assert.fail("Should have no row");
        }

        Assert.assertEquals(Result.of(new ByteArrayInputStream("321".getBytes())).type(), ByteArrayInputStream.class);
        in = new ByteArrayInputStream("321".getBytes());
        Assert.assertEquals(Result.of(in).get(), in);
        Assert.assertEquals(Result.of(in).get(InputStream.class), in);
        Assert.assertEquals(Result.of(new ByteArrayInputStream("321".getBytes())).get(String.class), "321");
        int count = 0;
        for (Row row : Result.of(new ByteArrayInputStream("321".getBytes())).rows()) {
            Assert.assertEquals(row.size(), 1);
            Assert.assertEquals(row.value(0).asString(), "321");
            count++;
        }
        Assert.assertEquals(count, 1);

        in = new ByteArrayInputStream("3\n2\n1".getBytes());
        count = 3;
        for (Row row : Result.of(in, "\n".getBytes()).rows()) {
            Assert.assertEquals(row.size(), 1);
            Assert.assertEquals(row.value(0).asString(), String.valueOf(count--));
        }
        Assert.assertEquals(count, 0);
    }

    @Test(groups = "unit")
    public void testReader() {
        Assert.assertThrows(IllegalArgumentException.class, () -> Result.of((Reader) null));

        Assert.assertEquals(Result.of(new StringReader("")).type(), StringReader.class);
        Reader in = new StringReader("");
        Assert.assertEquals(Result.of(in).get(), in);
        Assert.assertEquals(Result.of(in).get(Reader.class), in);
        Assert.assertEquals(Result.of(new StringReader("")).get(String.class), "");
        for (Row row : Result.of(new StringReader("")).rows()) {
            Assert.fail("Should have no row");
        }

        Assert.assertEquals(Result.of(new StringReader("321")).type(), StringReader.class);
        in = new StringReader("321");
        Assert.assertEquals(Result.of(in).get(), in);
        Assert.assertEquals(Result.of(in).get(Reader.class), in);
        Assert.assertEquals(Result.of(new StringReader("321")).get(String.class), "321");
        int count = 0;
        for (Row row : Result.of(new StringReader("321")).rows()) {
            Assert.assertEquals(row.size(), 1);
            Assert.assertEquals(row.value(0).asString(), "321");
            count++;
        }
        Assert.assertEquals(count, 1);

        in = new StringReader("3\n2\n1");
        count = 3;
        for (Row row : Result.of(in, "\n").rows()) {
            Assert.assertEquals(row.size(), 1);
            Assert.assertEquals(row.value(0).asString(), String.valueOf(count--));
        }
        Assert.assertEquals(count, 0);
    }

    @Test(groups = "unit")
    public void testString() {
        Assert.assertThrows(IllegalArgumentException.class, () -> Result.of((String) null));

        Assert.assertEquals(Result.of("").type(), String.class);
        Assert.assertEquals(Result.of("").get(), "");
        Assert.assertEquals(Result.of("").get(String.class), "");
        int count = 0;
        for (Row row : Result.of("").rows()) {
            Assert.assertEquals(row.size(), 1);
            Assert.assertEquals(row.value(0).asString(), "");
            // Assert.assertEquals(row.field(0)., 1);
            count++;
        }
        Assert.assertEquals(count, 1);

        Assert.assertEquals(Result.of("123").type(), String.class);
        Assert.assertEquals(Result.of("123").get(), "123");
        Assert.assertEquals(Result.of("123").get(String.class), "123");
        count = 0;
        for (Row row : Result.of("123").rows()) {
            Assert.assertEquals(row.size(), 1);
            Assert.assertEquals(row.value(0).asString(), "123");
            // Assert.assertEquals(row.field(0)., 1);
            count++;
        }
        Assert.assertEquals(count, 1);
    }
}
