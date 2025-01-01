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
package io.github.jdbcx.interpreter;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.github.jdbcx.BaseIntegrationTest;
import io.github.jdbcx.Field;
import io.github.jdbcx.Result;
import io.github.jdbcx.Row;

public class ScriptHelperTest extends BaseIntegrationTest {
    private final ScriptHelper helper = ScriptHelper.getInstance();

    @Test(groups = { "unit" })
    public void testRead() throws IOException {
        String classFile = "target/classes/" + helper.getClass().getName().replace('.', '/') + ".class";
        Assert.assertEquals(helper.read(null), "");
        Assert.assertThrows(FileNotFoundException.class, () -> helper.read("non-existing-file"));
        Assert.assertTrue(helper.read("").length() > 0); // list files in current directory
        Assert.assertTrue(helper.read(classFile).length() > 0);
        Assert.assertTrue(helper.read("file:" + classFile).length() > 0);
    }

    @Test(groups = { "integration" })
    public void testReadWeb() throws IOException {
        String address = getClickHouseServer();
        Assert.assertEquals(helper.read("http://" + address), "Ok.\n");
        Assert.assertEquals(helper.read("http://default@" + address, "select 1\n"), "1\n");
        Assert.assertEquals(helper.read("http://default:@" + address, "select 2\n"), "2\n");
        Assert.assertEquals(helper.read("http://" + address, "select 3\n",
                Collections.singletonMap("X-ClickHouse-User", "default")), "3\n");
    }

    @Test(groups = { "unit" })
    public void testTable() {
        Result<?> result = helper.table(null);
        Assert.assertEquals(result.fields(), Collections.singletonList(Field.DEFAULT));
        Assert.assertEquals(result.rows(), Collections.emptyList());

        result = helper.table(new Object[] { 1, null, 2 });
        Assert.assertEquals(result.fields(), Arrays.asList(Field.of("1"), Field.of("null_field_1"), Field.of("2")));
        Assert.assertEquals(result.rows(), Collections.emptyList());

        result = helper.table(new Object[] { "a", "b" }, new Object[] { 1, 2 });
        Assert.assertEquals(result.fields(), Arrays.asList(Field.of("a"), Field.of("b")));
        int count = 0;
        for (Row r : result.rows()) {
            Assert.assertEquals(r.size(), result.fields().size());
            Assert.assertEquals(r.value(0).asInt(), 1);
            Assert.assertEquals(r.value(1).asInt(), 2);
            count++;
        }
        Assert.assertEquals(count, 1);

        result = helper.table(new Object[] { "a", "b" }, new Object[][] { { 1, 2 }, { 3, 4 } });
        Assert.assertEquals(result.fields(), Arrays.asList(Field.of("a"), Field.of("b")));
        count = 0;
        for (Row r : result.rows()) {
            Assert.assertEquals(r.size(), result.fields().size());
            Assert.assertEquals(r.value(0).asInt(), count * 2 + 1);
            Assert.assertEquals(r.value(1).asInt(), count * 2 + 2);
            count++;
        }
        Assert.assertEquals(count, 2);

        result = helper.table(new Object[] { "a", "b" }, Arrays.asList(1, 2), Arrays.asList(3, 4));
        Assert.assertEquals(result.fields(), Arrays.asList(Field.of("a"), Field.of("b")));
        count = 0;
        for (Row r : result.rows()) {
            Assert.assertEquals(r.size(), result.fields().size());
            Assert.assertEquals(r.value(0).asInt(), count * 2 + 1);
            Assert.assertEquals(r.value(1).asInt(), count * 2 + 2);
            count++;
        }
        Assert.assertEquals(count, 2);

        result = helper.table(new Object[] { "a", "b" }, Arrays.asList(Arrays.asList(1, 2), Arrays.asList(3, 4)));
        Assert.assertEquals(result.fields(), Arrays.asList(Field.of("a"), Field.of("b")));
        count = 0;
        for (Row r : result.rows()) {
            Assert.assertEquals(r.size(), result.fields().size());
            Assert.assertEquals(r.value(0).asInt(), count * 2 + 1);
            Assert.assertEquals(r.value(1).asInt(), count * 2 + 2);
            count++;
        }
        Assert.assertEquals(count, 2);
    }
}
