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
package io.github.jdbcx.interpreter;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import io.github.jdbcx.Constants;
import io.github.jdbcx.Row;

public class JsonHelperTest {
    private final String json = "{\"choices\":[{\"finish_reason\":\"stop\",\"index\":0,\"message\":{\"content\":\"Hello, how can I assist you today?\",\"role\":\"assistant\"},\"references\":[]}],\"created\":1689916164,\"id\":\"foobarbaz\",\"model\":\"GPT4All Falcon\",\"object\":\"text_completion\",\"usage\":{\"completion_tokens\":9,\"prompt_tokens\":20,\"total_tokens\":29}}";

    @DataProvider(name = "pathAndValue")
    protected Object[][] getPathAndValue() {
        return new Object[][] { { "choices[0].finish_reason", "stop" },
                { "choices[].index", "0" },
                { "choices[0].message.content", "Hello, how can I assist you today?" }, };
    }

    @Test(dataProvider = "pathAndValue", groups = { "unit" })
    public void testExtractFromInputStream(String path, String expectedValue) throws IOException {
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        Assert.assertEquals(
                JsonHelper.extract(new ByteArrayInputStream(bytes), null, path, "", false).get(0).value(0).asString(),
                expectedValue);
        Assert.assertEquals(
                JsonHelper.extract(new ByteArrayInputStream(bytes), StandardCharsets.US_ASCII, path, "", false).get(0)
                        .value(0)
                        .asString(),
                expectedValue);
    }

    @Test(dataProvider = "pathAndValue", groups = { "unit" })
    public void testExtractFromReader(String path, String expectedValue) throws IOException {
        Assert.assertEquals(JsonHelper.extract(new StringReader(json), path, "", false).get(0).value(0).asString(),
                expectedValue);
    }

    @Test(dataProvider = "pathAndValue", groups = { "unit" })
    public void testExtractFromString(String path, String expectedValue) {
        Assert.assertEquals(JsonHelper.extract(json, path), expectedValue);
    }

    @Test(groups = { "unit" })
    public void testExtract() throws IOException {
        Assert.assertThrows(NullPointerException.class,
                () -> JsonHelper.extract(new ByteArrayInputStream(new byte[0]), Constants.DEFAULT_CHARSET, "value", "",
                        false));

        String text = "{}";
        Assert.assertEquals(JsonHelper.extract(new StringReader(text), "value", "", false).get(0).value(0).asString(),
                "");
        text = "{\"value\":[1,null,2,\" 3\",{}, []]}";
        String[] expected = new String[] { "1", "", "2", "3", "{}", "[]" };
        int count = 0;
        for (Row r : JsonHelper.extract(new StringReader(text), "value", ",", true)) {
            Assert.assertEquals(r.size(), 1);
            Assert.assertEquals(r.value(0).asString(), expected[count++]);
        }
        Assert.assertEquals(count, expected.length);
    }
}
