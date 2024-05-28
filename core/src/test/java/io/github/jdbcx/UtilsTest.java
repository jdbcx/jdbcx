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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
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
        Assert.assertEquals(Utils.applyVariables(null, null, (Properties) null), "");
        Assert.assertEquals(Utils.applyVariables(null, null, (Map<String, String>) null), "");
        Assert.assertEquals(Utils.applyVariables(null, null, (UnaryOperator<String>) null), "");
        Assert.assertEquals(Utils.applyVariables("", null, (Properties) null), "");
        Assert.assertEquals(Utils.applyVariables("", null, (Map<String, String>) null), "");
        Assert.assertEquals(Utils.applyVariables("", null, (UnaryOperator<String>) null), "");

        Properties props = new Properties();
        Map<String, String> map = new HashMap<>();
        UnaryOperator<String> func = x -> "";
        Assert.assertEquals(Utils.applyVariables(null, null, props), "");
        Assert.assertEquals(Utils.applyVariables(null, null, map), "");
        Assert.assertEquals(Utils.applyVariables(null, null, func), "");
        Assert.assertEquals(Utils.applyVariables("", null, props), "");
        Assert.assertEquals(Utils.applyVariables("", null, map), "");
        Assert.assertEquals(Utils.applyVariables("", null, func), "");

        Assert.assertEquals(Utils.applyVariables("\\${}$\\{}${\\}", null, func), "\\${}$\\{}${\\}");
        Assert.assertEquals(Utils.applyVariables("${", null, func), "${");
        Assert.assertEquals(Utils.applyVariables("${}", null, func), "");
        Assert.assertEquals(Utils.applyVariables("${\\${}}", null, func), "}");
        Assert.assertEquals(Utils.applyVariables("${\\${\\}}", null, func), "");
        Assert.assertEquals(Utils.applyVariables("${ }", null, func), "");
        Assert.assertEquals(Utils.applyVariables("${ no${th\\}ing }", null, func), "");
        Assert.assertEquals(Utils.applyVariables("${ no${th\\}ing }${ }${}", null, func), "");

        props.setProperty(" ", "whitespace");
        props.setProperty("a", "A");
        props.setProperty("${inner}", "i");
        Assert.assertEquals(Utils.applyVariables("\\${}$\\{}${\\}", null, props), "\\${}$\\{}${\\}");
        Assert.assertEquals(Utils.applyVariables("${", null, props), "${");
        Assert.assertEquals(Utils.applyVariables("${}", null, props), "${}");
        Assert.assertEquals(Utils.applyVariables("${ }", null, props), "whitespace");
        Assert.assertEquals(Utils.applyVariables("${a}", null, props), "A");
        Assert.assertEquals(Utils.applyVariables("${a}${\\${inner\\}}${ }${}", null, props), "Aiwhitespace${}");

        map.put(" ", "whitespace");
        map.put("a", "A");
        map.put("${in\\ner}", "i");
        Assert.assertEquals(Utils.applyVariables("\\${}$\\{}${\\}", null, map), "\\${}$\\{}${\\}");
        Assert.assertEquals(Utils.applyVariables("${", null, map), "${");
        Assert.assertEquals(Utils.applyVariables("${}", null, map), "${}");
        Assert.assertEquals(Utils.applyVariables("${ }", null, map), "whitespace");
        Assert.assertEquals(Utils.applyVariables("${a}", null, map), "A");
        Assert.assertEquals(Utils.applyVariables("${a}${\\$\\{in\\\\ner\\}}${ }${}", null, map), "Aiwhitespace${}");

        props.clear();
        props.setProperty("", "nothing");
        map.put("", "everything");
        Assert.assertEquals(Utils.applyVariables("${}", null, props), "nothing");
        Assert.assertEquals(Utils.applyVariables("${}", null, map), "everything");

        // try different tags
        Assert.assertEquals(Utils.applyVariables("$<>", null, map), "$<>");
        Assert.assertEquals(Utils.applyVariables("$<>", VariableTag.ANGLE_BRACKET, map), "everything");
        Assert.assertEquals(Utils.applyVariables("$[]", null, props), "$[]");
        Assert.assertEquals(Utils.applyVariables("$[]", VariableTag.SQUARE_BRACKET, props), "nothing");

        // try default values
        props.setProperty("s", "1");
        Assert.assertEquals(Utils.applyVariables("${:}", null, props), "${:}");
        Assert.assertEquals(Utils.applyVariablesWithDefault("${:}", null, props), "nothing");
        Assert.assertEquals(Utils.applyVariables("${a:}", null, props), "${a:}");
        Assert.assertEquals(Utils.applyVariablesWithDefault("${a:}", null, props), "");
        Assert.assertEquals(Utils.applyVariables("${s:}", null, props), "${s:}");
        Assert.assertEquals(Utils.applyVariablesWithDefault("${s:}", null, props), "1");
        Assert.assertEquals(Utils.applyVariables("${s} ${s:2}", null, props), "1 ${s:2}");
        Assert.assertEquals(Utils.applyVariablesWithDefault("${s} ${s:2}", null, props), "1 1");
        Assert.assertEquals(Utils.applyVariables("${a:a\\:b}", null, props), "${a:a\\:b}");
        Assert.assertEquals(Utils.applyVariablesWithDefault("${a:a\\:b}", null, props), "a:b");
    }

    @Test(groups = "unit")
    public void testCreateInstance() {
        Assert.assertEquals(Utils.createInstance(String.class, Object.class, false).getClass(), Object.class);
        Assert.assertEquals(Utils.createInstance(String.class, Object.class, true).getClass(), String.class);

        Assert.assertThrows(IllegalArgumentException.class,
                () -> Utils.createInstance(null, "LinkedList", new ArrayList<>()));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> Utils.createInstance(List.class, "LinkedList", null));

        Assert.assertEquals(Utils.createInstance(List.class, null, new ArrayList<>()).getClass(),
                ArrayList.class);
        Assert.assertEquals(Utils.createInstance(List.class, "", new ArrayList<>()).getClass(),
                ArrayList.class);
        Assert.assertEquals(Utils.createInstance(List.class, "NonExistList", new ArrayList<>()).getClass(),
                ArrayList.class);
        Assert.assertEquals(Utils.createInstance(List.class, "LinkedList", new ArrayList<>()).getClass(),
                LinkedList.class);
        Assert.assertEquals(Utils.createInstance(List.class, "java.util.LinkedList", new ArrayList<>()).getClass(),
                LinkedList.class);
    }

    @Test(groups = "unit")
    public void testFindFiles() throws IOException {
        Assert.assertEquals(Utils.findFiles(null, null), Collections.emptyList());
        Assert.assertEquals(Utils.findFiles("", ""), Collections.emptyList());
        Assert.assertEquals(Utils.findFiles("nothing", null), Collections.emptyList());
        Assert.assertEquals(Utils.findFiles("*nothing", null), Collections.emptyList());
        Assert.assertEquals(Utils.findFiles("n?thing", null), Collections.emptyList());
        Assert.assertEquals(Utils.findFiles("target/classes/META-INF", ".x"), Collections.emptyList());

        Assert.assertEquals(Utils.findFiles("pom*", null), Collections.singletonList(Utils.getPath("pom.xml", true)));
        Assert.assertEquals(Utils.findFiles("target/test-classes/docker-*", ".yml"),
                Collections.singletonList(Utils.getPath("target/test-classes/docker-compose.yml", true)));
        Assert.assertEquals(Utils.findFiles("target/classes/META-INF", ""),
                Arrays.asList(Utils.getPath("target/classes/META-INF/LICENSE", true),
                        Utils.getPath("target/classes/META-INF/NOTICE", true)));
    }

    @Test(groups = "unit")
    public void testToPrimitiveType() {
        Assert.assertThrows(IllegalArgumentException.class, () -> Utils.toPrimitiveType(null));

        for (Class<?>[] classes : new Class[][] {
                { Object.class, Object.class },
                { String.class, String.class },
                { Boolean.class, boolean.class },
                { Character.class, char.class },
                { Byte.class, byte.class },
                { Short.class, short.class },
                { Integer.class, int.class },
                { Long.class, long.class },
                { Float.class, float.class },
                { Double.class, double.class },
        }) {
            Assert.assertEquals(Utils.toPrimitiveType(classes[0]), classes[1]);
            Assert.assertEquals(Utils.toPrimitiveType(classes[1]), classes[1]);
        }
    }

    @Test(groups = "unit")
    public void testNewInstance() {
        Assert.assertThrows(IllegalArgumentException.class, () -> Utils.newInstance(null, null, null));
        Assert.assertThrows(IllegalArgumentException.class, () -> Utils.newInstance(List.class, null, null));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> Utils.newInstance(null, ArrayList.class.getName(), null));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> Utils.newInstance(List.class, ArrayList.class.getName(), null));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> Utils.newInstance(List.class, ArrayList.class.getName(), new Object[] { null }));

        Assert.assertEquals(Utils.newInstance(List.class, ArrayList.class.getName()), new ArrayList<>());
        Assert.assertEquals(Utils.newInstance(List.class, ArrayList.class.getName(), 2), new ArrayList<>());
        Assert.assertEquals(Utils.newInstance(Properties.class, Properties.class.getName(), new Properties()),
                new Properties());
    }

    @Test(groups = "unit")
    public void testGetHost() {
        Assert.assertNotNull(Utils.getHost(null));
        Assert.assertNotNull(Utils.getHost(""));
        Assert.assertNotNull(Utils.getHost("bing.com"));
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
        Assert.assertEquals(Utils.toKeyValuePairs((String) null), Collections.emptyMap());
        Assert.assertEquals(Utils.toKeyValuePairs(""), Collections.emptyMap());
        Assert.assertEquals(Utils.toKeyValuePairs(",,,"), Collections.emptyMap());
        Assert.assertEquals(Utils.toKeyValuePairs(" , , , "), Collections.emptyMap());

        Assert.assertEquals(Utils.toKeyValuePairs((Map<String, String>) null), "");
        Assert.assertEquals(Utils.toKeyValuePairs(Collections.emptyMap()), "");

        Map<String, String> expected = new LinkedHashMap<>();
        expected.put("Content-Type", "application/json");
        Assert.assertEquals(Utils.toKeyValuePairs("Content-Type=application/json"), expected);
        Assert.assertEquals(Utils.toKeyValuePairs("Content-Type = application/json"), expected);
        Assert.assertEquals(Utils.toKeyValuePairs(" Content-Type = application/json ,"), expected);
        // Assert.assertEquals(Utils.toKeyValuePairs(", Content-Type = application/json
        // ,"), expected);
        Assert.assertEquals(Utils.toKeyValuePairs(expected), "Content-Type=application/json");

        expected.put("Authorization", "Bear 1234 5");
        Assert.assertEquals(Utils.toKeyValuePairs("Content-Type=application/json,Authorization=Bear 1234 5"), expected);
        Assert.assertEquals(Utils.toKeyValuePairs("Content-Type = application/json , Authorization = Bear 1234 5 "),
                expected);
        Assert.assertEquals(Utils.toKeyValuePairs(expected), "Content-Type=application/json,Authorization=Bear 1234 5");

        expected.clear();
        expected.put("a 1", "select 1");
        Assert.assertEquals(Utils.toKeyValuePairs("a%201=select+1", '&', true), expected);
        Assert.assertEquals(Utils.toKeyValuePairs(expected, '&', true), "a+1=select+1");

        expected.clear();
        expected.put("a 1", "b ");
        expected.put("c", "3");
        Assert.assertEquals(Utils.toKeyValuePairs("a%201=b%20&c=3", '&', true), expected);
        Assert.assertEquals(Utils.toKeyValuePairs(expected, '&', true), "a+1=b+&c=3");

        expected.clear();
        expected.put("a%201", "b%20");
        expected.put("c", "3");
        Assert.assertEquals(Utils.toKeyValuePairs("a%201=b%20&c=3", '&', false), expected);
        Assert.assertEquals(Utils.toKeyValuePairs(expected, '&', false), "a%201=b%20&c=3");
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

    @Test(groups = { "unit" })
    public void testToImmutableList() {
        Assert.assertEquals(Utils.toImmutableList(String.class, null), Collections.emptyList());
        Assert.assertEquals(Utils.toImmutableList(String.class, null, false, false), Collections.emptyList());

        Assert.assertEquals(Utils.toImmutableList(String.class, new String[0]), Collections.emptyList());
        Assert.assertEquals(Utils.toImmutableList(String.class, new String[0], false, false), Collections.emptyList());

        Assert.assertEquals(Utils.toImmutableList(String.class, new String[1]), Collections.emptyList());
        Assert.assertEquals(Utils.toImmutableList(String.class, new String[1], false, false),
                Collections.singletonList(null));

        Assert.assertEquals(Utils.toImmutableList(String.class, new String[2]), Collections.emptyList());
        Assert.assertEquals(Utils.toImmutableList(String.class, new String[2], false, false),
                Arrays.asList(null, null));
        Assert.assertEquals(Utils.toImmutableList(String.class, new String[2], false, true),
                Collections.singletonList(null));

        Assert.assertEquals(Utils.toImmutableList(String.class, new String[] { "1", null, "1" }),
                Collections.singletonList("1"));
        Assert.assertEquals(Utils.toImmutableList(String.class, new String[] { "1", null, "1" }, false, false),
                Arrays.asList("1", null, "1"));
        Assert.assertEquals(Utils.toImmutableList(String.class, new String[] { "1", null, "1" }, true, false),
                Arrays.asList("1", "1"));
        Assert.assertEquals(Utils.toImmutableList(String.class, new String[] { "1", null, "1" }, false, true),
                Arrays.asList("1", null));
        Assert.assertEquals(Utils.toImmutableList(String.class, new String[] { "1", null, "1" }, true, true),
                Collections.singletonList("1"));
    }
}
