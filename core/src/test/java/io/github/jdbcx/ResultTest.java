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
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Collections;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.github.jdbcx.data.DefaultRow;

public class ResultTest {
    @Test(groups = "unit")
    public void testBuilder() {
        Assert.assertThrows(IllegalArgumentException.class, () -> Result.builder().build());

        Assert.assertEquals(Result.builder().value(null).build().fields(), Result.DEFAULT_FIELDS);
        Assert.assertEquals(Result.builder().value(null).build().get(), null);

        Assert.assertEquals(Result.builder().rows(new Object[] { 1 }).build().fields(), Result.DEFAULT_FIELDS);
        Assert.assertEquals(Result.builder().rows(new Object[] { 1 }).build().get(), new Object[][] { { 1 } });

        Result<?> result = Result.builder().fields(Field.of("f1"), Field.of("f2")).value("fields").build();

        Assert.assertEquals(result.fields(), Arrays.asList(Field.of("f1"), Field.of("f2")));
        Assert.assertEquals(result.type(), String.class);
        Assert.assertEquals(result.get(), "fields");
        int count = 0;
        for (Row row : result.rows()) {
            Assert.assertEquals(row.size(), 1);
            Assert.assertEquals(row.field(0), Field.of("f1"));
            Assert.assertEquals(row.field(1), Field.of("f2"));
            Assert.assertEquals(row.value(0).asString(), "fields");
            Assert.assertEquals(row.value("F1").asString(), "fields");
            count++;
        }
        Assert.assertEquals(count, 1);
    }

    @Test(groups = "unit")
    public void testUpdate() {
        Result<?> result = Result.builder().fields(Field.of("f1"), Field.of("f2")).value("fields").build();

        Assert.assertEquals(result.fields(), Arrays.asList(Field.of("f1"), Field.of("f2")));
        Assert.assertEquals((result = Result.builder().value(null).build()).get(), null);
        Assert.assertEquals(result.fields(), Result.DEFAULT_FIELDS);
    }

    @Test(groups = "unit")
    public void testInputStream() {
        Assert.assertThrows(IllegalArgumentException.class, () -> Result.of((InputStream) null, null));

        Assert.assertEquals(Result.of(new ByteArrayInputStream(new byte[0]), null).type(), ByteArrayInputStream.class);
        ByteArrayInputStream in = new ByteArrayInputStream(new byte[0]);
        Assert.assertEquals(Result.of(in, null).get(), in);
        Assert.assertEquals(Result.of(in, null).get(InputStream.class), in);
        Assert.assertEquals(Result.of(new ByteArrayInputStream(new byte[0]), null).get(String.class), "");
        for (Row row : Result.of(new ByteArrayInputStream(new byte[0]), null).rows()) {
            Assert.fail("Should have no row");
        }

        Assert.assertEquals(Result.of(new ByteArrayInputStream("321".getBytes()), null).type(),
                ByteArrayInputStream.class);
        in = new ByteArrayInputStream("321".getBytes());
        Assert.assertEquals(Result.of(in, null).get(), in);
        Assert.assertEquals(Result.of(in, null).get(InputStream.class), in);
        Assert.assertEquals(Result.of(new ByteArrayInputStream("321".getBytes()), null).get(String.class), "321");
        int count = 0;
        for (Row row : Result.of(new ByteArrayInputStream("321".getBytes()), null).rows()) {
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
        Assert.assertThrows(IllegalArgumentException.class, () -> Result.of((Reader) null, null));

        Assert.assertEquals(Result.of(new StringReader(""), null).type(), StringReader.class);
        Reader in = new StringReader("");
        Assert.assertEquals(Result.of(in, null).get(), in);
        Assert.assertEquals(Result.of(in, null).get(Reader.class), in);
        Assert.assertEquals(Result.of(new StringReader(""), null).get(String.class), "");
        for (Row row : Result.of(new StringReader(""), null).rows()) {
            Assert.fail("Should have no row");
        }

        Assert.assertEquals(Result.of(new StringReader("321"), null).type(), StringReader.class);
        in = new StringReader("321");
        Assert.assertEquals(Result.of(in, null).get(), in);
        Assert.assertEquals(Result.of(in, null).get(Reader.class), in);
        Assert.assertEquals(Result.of(new StringReader("321"), null).get(String.class), "321");
        int count = 0;
        for (Row row : Result.of(new StringReader("321"), null).rows()) {
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
        Assert.assertEquals(Result.of((String) null).type(), String.class);
        Assert.assertEquals(Result.of((String) null).get(), null);

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

    @Test(groups = "unit")
    public void testFields() {
        Assert.assertEquals(Result.of(Constants.EMPTY_STRING).fields(), Result.DEFAULT_FIELDS);

        Assert.assertEquals(Result.of(Arrays.asList(Field.of("a"), Field.of("b")), Collections.emptyList()).fields(),
                Arrays.asList(Field.of("a"), Field.of("b")));
    }

    @Test(groups = "unit")
    public void testRows() throws Exception {
        Result<?> r = Result.of((Row) null);
        Assert.assertEquals(r.fields(), Result.DEFAULT_FIELDS);
        Assert.assertEquals(r.rows(), Collections.emptyList());
        Assert.assertEquals(r.get(), null);
        Assert.assertEquals(r.type(), Row.class);
        Assert.assertEquals(r.get(ResultSet.class).next(), false);

        r = Result.of(Row.of("a"));
        Assert.assertEquals(r.fields(), Result.DEFAULT_FIELDS);
        int count = 0;
        for (Row row : r.rows()) {
            Assert.assertEquals(row.size(), 1);
            Assert.assertEquals(row.value(0).asString(), "a");
            count++;
        }
        Assert.assertEquals(count, 1);
        Assert.assertEquals(r.get(Row.class).size(), 1);
        Assert.assertEquals(r.get(Row.class).value(0).asString(), "a");
        Assert.assertEquals(r.type(), DefaultRow.class);
        Assert.assertEquals(r.get(String.class), "a");
    }
}
