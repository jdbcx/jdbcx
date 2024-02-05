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
package io.github.jdbcx;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.function.UnaryOperator;

import org.testng.Assert;
import org.testng.annotations.Test;

public class UtilsTest {
    @Test(groups = "unit")
    public void testContainsJdbcWildcard() {
        Assert.assertFalse(Utils.containsJdbcWildcard(null));
        Assert.assertFalse(Utils.containsJdbcWildcard(""));

        Assert.assertFalse(Utils.containsJdbcWildcard("1\\%23"));
        Assert.assertFalse(Utils.containsJdbcWildcard("abc\\_"));

        Assert.assertTrue(Utils.containsJdbcWildcard("1%23"));
        Assert.assertTrue(Utils.containsJdbcWildcard("abc_"));
    }

    @Test(groups = "unit")
    public void testJdbcNamePatternToRe() {
        Assert.assertEquals(Utils.jdbcNamePatternToRe(null), "");
        Assert.assertEquals(Utils.jdbcNamePatternToRe(""), "");

        Assert.assertEquals(Utils.jdbcNamePatternToRe("\\<"), "\\<");
        Assert.assertEquals(Utils.jdbcNamePatternToRe("<"), "\\<");

        Assert.assertEquals(Utils.jdbcNamePatternToRe("_ta\\_ble%"), ".ta_ble.*");
        Assert.assertEquals(Utils.jdbcNamePatternToRe("s\\%ch%m_"), "s%ch.*m.");
    }

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

        props.clear();
        props.setProperty("", "nothing");
        map.put("", "everything");
        Assert.assertEquals(Utils.applyVariables("${}", props), "nothing");
        Assert.assertEquals(Utils.applyVariables("${}", map), "everything");
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

        expected.clear();
        expected.put("a 1", "select 1");
        Assert.assertEquals(Utils.toKeyValuePairs("a%201=select+1", '&', false), expected);

        expected.clear();
        expected.put("a 1", "b ");
        expected.put("c", "3");
        Assert.assertEquals(Utils.toKeyValuePairs("a%201=b%20&c=3", '&', false), expected);

        expected.clear();
        expected.put("a%201", "b%20");
        expected.put("c", "3");
        Assert.assertEquals(Utils.toKeyValuePairs("a%201=b%20&c=3", '&', true), expected);
    }

    @Test(groups = { "unit" })
    public void testIndexOf() {
        String args = "select 1 -- select one\n union all select 2 -- select two--";
        Assert.assertEquals(Utils.indexOf(args, 0, args.length(), 'x'), args.indexOf('x', 0));
        Assert.assertEquals(Utils.indexOf(args, 3, args.length(), 'x'), args.indexOf('x', 3));
        Assert.assertEquals(Utils.indexOf(args, 0, args.length(), 'o', 'n'), args.indexOf("on", 0));
        Assert.assertEquals(Utils.indexOf(args, 12, args.length(), 'o', 'n'), args.indexOf("on", 12));
    }

    @Test(groups = { "unit" })
    public void testSkipSingleLineComment() {
        String args = "select 1 -- select one\n union all select 2 -- select two--";
        Assert.assertEquals(Utils.skipSingleLineComment(args, 11, args.length()),
                args.indexOf('\n') + 1);
        Assert.assertEquals(Utils.skipSingleLineComment(args, args.indexOf("--", 11), args.length()),
                args.length());
    }

    @Test(groups = { "unit" })
    public void testSkipMultipleLineComment() {
        Assert.assertEquals(Utils.skipMultiLineComment("", 0, 0), -1);
        Assert.assertEquals(Utils.skipMultiLineComment("/", 0, 1), -1);
        Assert.assertEquals(Utils.skipMultiLineComment("/*", 0, 2), -1);
        Assert.assertEquals(Utils.skipMultiLineComment("/**", 0, 3), -1);
        Assert.assertEquals(Utils.skipMultiLineComment("/**/", 0, 4), 4);
        Assert.assertEquals(Utils.skipMultiLineComment("/*/*/", 0, 5), -1);
        Assert.assertEquals(Utils.skipMultiLineComment("/*/**/", 0, 6), -1);
        Assert.assertEquals(Utils.skipMultiLineComment("/*/***/", 0, 7), -1);
        Assert.assertEquals(Utils.skipMultiLineComment("/*/**/*/", 0, 8), 8);

        Assert.assertEquals(Utils.skipMultiLineComment("/*/*/**/*/*/", 0, 12), 12);

        String args = "select 1 /* select 1/*one*/ -- a */, 2";
        Assert.assertEquals(Utils.skipMultiLineComment(args, 9, args.length()),
                args.lastIndexOf("*/") + 2);
        Assert.assertEquals(Utils.skipMultiLineComment(args, 20, args.length()),
                args.indexOf("*/", 21) + 2);
        Assert.assertEquals(Utils.skipMultiLineComment(args, args.lastIndexOf("*/") + 1, args.length()), -1);
    }

    @Test(groups = { "unit" })
    public void testSplitByChar() {
        Assert.assertEquals(Utils.split(null, '\0'), Collections.emptyList());
        Assert.assertEquals(Utils.split("", '\0'), Collections.singletonList(""));
        Assert.assertEquals(Utils.split(" ", ' '), Arrays.asList(""));

        Assert.assertEquals(Utils.split(" ", '\0'), Collections.singletonList(" "));
        Assert.assertEquals(Utils.split("a\0\0b", '\0'), Arrays.asList("a", "", "b"));
        Assert.assertEquals(Utils.split(",,,a,c,,b,,,", ','), Arrays.asList("", "", "", "a", "c", "", "b", "", ""));
    }

    @Test(groups = { "unit" })
    public void testSplitByString() {
        Assert.assertEquals(Utils.split(null, null), Collections.emptyList());
        Assert.assertEquals(Utils.split("", null), Collections.singletonList(""));
        Assert.assertEquals(Utils.split(" ", null), Collections.singletonList(" "));
        Assert.assertEquals(Utils.split(null, " "), Collections.emptyList());
        Assert.assertEquals(Utils.split("", " "), Collections.singletonList(""));

        Assert.assertEquals(Utils.split("12", "123"), Collections.singletonList("12"));
        Assert.assertEquals(Utils.split(" ", ""), Collections.singletonList(" "));
        Assert.assertEquals(Utils.split("121", "21"), Collections.singletonList("1"));
        Assert.assertEquals(Utils.split("1,2,1", ","), Arrays.asList("1", "2", "1"));
        Assert.assertEquals(Utils.split(",,1,2,,1,,,", ","), Arrays.asList("", "", "1", "2", "", "1", "", ""));
        Assert.assertEquals(Utils.split(",,1,2,,1,,,", ",,"), Arrays.asList("", "1,2", "1", ","));
    }

    @Test(groups = { "unit" })
    public void testStartsWith() {
        Assert.assertEquals(Utils.startsWith(null, null, true), false);
        Assert.assertEquals(Utils.startsWith(null, null, false), false);
        Assert.assertEquals(Utils.startsWith(null, "", true), false);
        Assert.assertEquals(Utils.startsWith(null, "", false), false);
        Assert.assertEquals(Utils.startsWith("", null, true), false);
        Assert.assertEquals(Utils.startsWith("", null, false), false);

        Assert.assertEquals(Utils.startsWith("", "a", true), false);
        Assert.assertEquals(Utils.startsWith("", "a", false), false);
        Assert.assertEquals(Utils.startsWith("A", "ab", true), false);
        Assert.assertEquals(Utils.startsWith("a", "ab", false), false);

        Assert.assertEquals(Utils.startsWith("", "", true), true);
        Assert.assertEquals(Utils.startsWith("", "", false), true);
        Assert.assertEquals(Utils.startsWith("3", "", true), true);
        Assert.assertEquals(Utils.startsWith("3", "", false), true);

        Assert.assertEquals(Utils.startsWith("JDBcX:db", "jdbcX", true), true);
        Assert.assertEquals(Utils.startsWith("JDBcX:db", "jdbcX", false), false);
    }
}
