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

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.github.jdbcx.BaseIntegrationTest;
import io.github.jdbcx.Constants;
import io.github.jdbcx.Field;
import io.github.jdbcx.QueryContext;
import io.github.jdbcx.Result;
import io.github.jdbcx.Row;
import io.github.jdbcx.executor.Stream;

public class ScriptHelperTest extends BaseIntegrationTest {
    private final ScriptHelper helper = ScriptHelper.getInstance();

    @Test(groups = { "unit" })
    public void testEncode() {
        Object value = null;
        Assert.assertEquals(helper.encode(value, null), "null");
        Assert.assertEquals(helper.encode(value, "unknown"), "null");
        Assert.assertEquals(helper.encode(value, ScriptHelper.ENCODER_BASE64), "");
        Assert.assertEquals(helper.encode(value, ScriptHelper.ENCODER_JSON), "null");
        Assert.assertEquals(helper.encode(value, ScriptHelper.ENCODER_URL), "");
        Assert.assertEquals(helper.encode(value, ScriptHelper.ENCODER_XML), "");

        value = "";
        Assert.assertEquals(helper.encode(value, null), "");
        Assert.assertEquals(helper.encode(value, "unknown"), "");
        Assert.assertEquals(helper.encode(value, ScriptHelper.ENCODER_BASE64), "");
        Assert.assertEquals(helper.encode(value, ScriptHelper.ENCODER_JSON), "\"\"");
        Assert.assertEquals(helper.encode(value, ScriptHelper.ENCODER_URL), "");
        Assert.assertEquals(helper.encode(value, ScriptHelper.ENCODER_XML),
                ScriptHelper.CDATA_START + ScriptHelper.CDATA_END);

        value = 12.3;
        Assert.assertEquals(helper.encode(value, null), String.valueOf(value));
        Assert.assertEquals(helper.encode(value, "unknown"), String.valueOf(value));
        Assert.assertEquals(helper.encode(value, ScriptHelper.ENCODER_BASE64), "MTIuMw==");
        Assert.assertEquals(helper.encode(value, ScriptHelper.ENCODER_JSON), String.valueOf(value));
        Assert.assertEquals(helper.encode(value, ScriptHelper.ENCODER_URL), String.valueOf(value));
        Assert.assertEquals(helper.encode(value, ScriptHelper.ENCODER_XML),
                ScriptHelper.CDATA_START + String.valueOf(value) + ScriptHelper.CDATA_END);

        value = "1 2/3😂四";
        Assert.assertEquals(helper.encode(value, null), value);
        Assert.assertEquals(helper.encode(value, "unknown"), value);
        Assert.assertEquals(helper.encode(value, ScriptHelper.ENCODER_BASE64), "MSAyLzPwn5iC5Zub");
        Assert.assertEquals(helper.encode(value, ScriptHelper.ENCODER_JSON), "\"" + value + "\"");
        Assert.assertEquals(helper.encode(value, ScriptHelper.ENCODER_URL), "1+2%2F3%F0%9F%98%82%E5%9B%9B");
        Assert.assertEquals(helper.encode(value, ScriptHelper.ENCODER_XML),
                ScriptHelper.CDATA_START + value + ScriptHelper.CDATA_END);
    }

    @Test(groups = { "unit" })
    public void testEncodeFile() throws IOException {
        Assert.assertEquals(helper.encodeFile(null, null, false), "");
        Assert.assertEquals(helper.encodeFile(null, null, true), "");
        Assert.assertEquals(helper.encodeFile(null, false), "");
        Assert.assertEquals(helper.encodeFile(null, true), "");

        for (String type : new String[] { "bmp", "gif", "ico", "jpeg", "png", "webp" }) {
            String imageFile = "target/test-classes/images/jdbcx." + type;
            Assert.assertEquals(helper.encodeFile(imageFile, false),
                    Stream.readAllAsBase64(new FileInputStream(imageFile)));

            Assert.assertEquals(helper.encodeFile(imageFile, true),
                    "data:image/" + type + ";base64," + Stream.readAllAsBase64(new FileInputStream(imageFile)));
        }
    }

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

    @Test(groups = { "unit" })
    public void testVariable() {
        try (QueryContext context = QueryContext.newContext(Constants.SCOPE_THREAD, null)) {
            QueryContext.setCurrentContext(context);
            final String key = UUID.randomUUID().toString();
            Assert.assertEquals(helper.var(key), "");
            Assert.assertEquals(helper.var(key, null), null);
            Assert.assertEquals(helper.var(key, 1), "1");
            Assert.assertEquals(helper.var(null, key, null), "");
            Assert.assertEquals(helper.var(Constants.SCOPE_GLOBAL, key, "0"), "0");
            Assert.assertEquals(helper.var(Constants.SCOPE_THREAD, key, "t0"), "t0");
            Assert.assertThrows(IllegalArgumentException.class, () -> helper.var(Constants.SCOPE_QUERY, key, "q0"));
            Assert.assertThrows(IllegalArgumentException.class, () -> helper.var(1, key, "?"));

            Assert.assertNull(helper.setVariable(key, "a"));
            Assert.assertEquals(helper.var(key), "a");
            Assert.assertEquals(helper.var(Constants.SCOPE_THREAD, key, "t0"), "a");
            Assert.assertEquals(helper.var(Constants.SCOPE_GLOBAL, key, "0"), "0");

            Assert.assertEquals(helper.setVariable(Constants.SCOPE_THREAD, key, "b"), "a");
            Assert.assertEquals(helper.var(key), "b");
            Assert.assertEquals(helper.var(Constants.SCOPE_THREAD, key, "t0"), "b");
            Assert.assertEquals(helper.var(Constants.SCOPE_GLOBAL, key, "0"), "0");

            Assert.assertNull(helper.setVariable(Constants.SCOPE_GLOBAL, key, "5"));
            Assert.assertEquals(helper.var(key), "b");
            Assert.assertEquals(helper.var(Constants.SCOPE_THREAD, key, "t0"), "b");
            Assert.assertEquals(helper.var(Constants.SCOPE_GLOBAL, key, "0"), "5");

            Assert.assertEquals(helper.setVariable(Constants.SCOPE_GLOBAL, key, "6"), "5");
            Assert.assertEquals(helper.var(key), "b");
            Assert.assertEquals(helper.var(Constants.SCOPE_THREAD, key, "t0"), "b");
            Assert.assertEquals(helper.var(Constants.SCOPE_GLOBAL, key, "0"), "6");

            context.getParent().removeVariable(key);
        }
    }
}
