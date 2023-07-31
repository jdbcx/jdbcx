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

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.function.UnaryOperator;

import org.testng.Assert;
import org.testng.annotations.Test;

public class UtilsTest {
    @Test(groups = "unit")
    public void testApplyVariables() throws IOException {
        Assert.assertEquals(Utils.applyVariables(null, (Properties) null), "");
        Assert.assertEquals(Utils.applyVariables(null, (Map<String, String>) null), "");
        Assert.assertEquals(Utils.applyVariables(null, (UnaryOperator<String>) null), "");
        Assert.assertEquals(Utils.applyVariables("", (Properties) null), "");
        Assert.assertEquals(Utils.applyVariables("", (Map<String, String>) null), "");
        Assert.assertEquals(Utils.applyVariables("", (UnaryOperator<String>) null), "");

        Properties props = new Properties();
        Map<String, String> map = new HashMap<>();
        UnaryOperator<String> func = x -> "";
        Assert.assertEquals(Utils.applyVariables(null, props), "");
        Assert.assertEquals(Utils.applyVariables(null, map), "");
        Assert.assertEquals(Utils.applyVariables(null, func), "");
        Assert.assertEquals(Utils.applyVariables("", props), "");
        Assert.assertEquals(Utils.applyVariables("", map), "");
        Assert.assertEquals(Utils.applyVariables("", func), "");

        Assert.assertEquals(Utils.applyVariables("\\${}$\\{}${\\}", func), "\\${}$\\{}${\\}");
        Assert.assertEquals(Utils.applyVariables("${", func), "${");
        Assert.assertEquals(Utils.applyVariables("${}", func), "");
        Assert.assertEquals(Utils.applyVariables("${\\${}}", func), "}");
        Assert.assertEquals(Utils.applyVariables("${\\${\\}}", func), "");
        Assert.assertEquals(Utils.applyVariables("${ }", func), "");
        Assert.assertEquals(Utils.applyVariables("${ no${th\\}ing }", func), "");
        Assert.assertEquals(Utils.applyVariables("${ no${th\\}ing }${ }${}", func), "");

        props.setProperty(" ", "whitespace");
        props.setProperty("a", "A");
        props.setProperty("${inner}", "i");
        Assert.assertEquals(Utils.applyVariables("\\${}$\\{}${\\}", props), "\\${}$\\{}${\\}");
        Assert.assertEquals(Utils.applyVariables("${", props), "${");
        Assert.assertEquals(Utils.applyVariables("${}", props), "${}");
        Assert.assertEquals(Utils.applyVariables("${ }", props), "whitespace");
        Assert.assertEquals(Utils.applyVariables("${a}", props), "A");
        Assert.assertEquals(Utils.applyVariables("${a}${\\${inner\\}}${ }${}", props), "Aiwhitespace${}");

        map.put(" ", "whitespace");
        map.put("a", "A");
        map.put("${in\\ner}", "i");
        Assert.assertEquals(Utils.applyVariables("\\${}$\\{}${\\}", map), "\\${}$\\{}${\\}");
        Assert.assertEquals(Utils.applyVariables("${", map), "${");
        Assert.assertEquals(Utils.applyVariables("${}", map), "${}");
        Assert.assertEquals(Utils.applyVariables("${ }", map), "whitespace");
        Assert.assertEquals(Utils.applyVariables("${a}", map), "A");
        Assert.assertEquals(Utils.applyVariables("${a}${\\$\\{in\\\\ner\\}}${ }${}", map), "Aiwhitespace${}");
    }

    @Test(groups = "unit")
    public void testNormalizePath() throws IOException {
        Assert.assertEquals(Utils.normalizePath(null), "");
        Assert.assertEquals(Utils.normalizePath(""), "");
        Assert.assertEquals(Utils.normalizePath("http://s.com/term?w=1"), "http://s.com/term?w=1");

        Assert.assertEquals(Utils.normalizePath("~/"), Constants.HOME_DIR);
        Assert.assertEquals(Utils.normalizePath("~/a"), Constants.HOME_DIR + File.separatorChar + "a");
        Assert.assertEquals(Utils.normalizePath("~/a b c"), Constants.HOME_DIR + File.separatorChar + "a b c");
        Assert.assertEquals(Utils.normalizePath("~/a:b-c"), Constants.HOME_DIR + File.separatorChar + "a:b-c");
        Assert.assertEquals(Utils.normalizePath("~/test/dir,/a"),
                Constants.HOME_DIR + File.separatorChar + "test/dir,/a");

        if (Constants.IS_WINDOWS) {
            Assert.assertEquals(Utils.normalizePath("~/\\a"), Constants.HOME_DIR + File.separatorChar + "\\a");
            Assert.assertEquals(Utils.normalizePath("~/D:\\a\\b\\c"),
                    Constants.HOME_DIR + File.separatorChar + "D:\\a\\b\\c");
        } else {
            Assert.assertEquals(Utils.normalizePath("~//a"), Constants.HOME_DIR + File.separatorChar + "/a");
        }
    }

    @Test(groups = "unit")
    public void tetToKeyValuePairs() {
        Assert.assertEquals(Utils.toKeyValuePairs(null), Collections.emptyMap());
        Assert.assertEquals(Utils.toKeyValuePairs(""), Collections.emptyMap());
        Assert.assertEquals(Utils.toKeyValuePairs(",,,"), Collections.emptyMap());
        Assert.assertEquals(Utils.toKeyValuePairs(" , , , "), Collections.emptyMap());

        Map<String, String> expected = new HashMap<>();
        expected.put("Content-Type", "application/json");
        Assert.assertEquals(Utils.toKeyValuePairs("Content-Type=application/json"), expected);
        Assert.assertEquals(Utils.toKeyValuePairs("Content-Type = application/json"), expected);
        Assert.assertEquals(Utils.toKeyValuePairs(" Content-Type = application/json ,"), expected);
        // Assert.assertEquals(Utils.toKeyValuePairs(", Content-Type = application/json
        // ,"), expected);

        expected.put("Authorization", "Bear 1234 5");
        Assert.assertEquals(Utils.toKeyValuePairs("Content-Type=application/json,Authorization=Bear 1234 5"), expected);
        Assert.assertEquals(Utils.toKeyValuePairs("Content-Type = application/json , Authorization = Bear 1234 5 "),
                expected);
    }
}
