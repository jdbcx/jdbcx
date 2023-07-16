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
package io.github.jdbcx.impl;

import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

public class QueryParserTest {
    @Test(groups = { "unit" })
    public void testExtractQuotedString() {
        Assert.assertThrows(IllegalArgumentException.class, () -> QueryParser.extractQuotedPart("'", 2, 1, '\''));
        Assert.assertThrows(IllegalArgumentException.class, () -> QueryParser.extractQuotedPart("' ", 1, 2, '\''));
        Assert.assertThrows(IllegalArgumentException.class, () -> QueryParser.extractQuotedPart("'", 1, 1, '\''));
        Assert.assertThrows(IllegalArgumentException.class, () -> QueryParser.extractQuotedPart("`123", 1, 4, '`'));

        String str;
        Assert.assertEquals(QueryParser.extractQuotedPart(str = "'", 0, 1, '\''), QueryParser.newPart(1, ""));
        Assert.assertEquals(QueryParser.extractQuotedPart(str = " '' \\''", 1, str.length(), '\''),
                QueryParser.newPart(7, "' '"));
        Assert.assertEquals(QueryParser.extractQuotedPart(str = "a = \"1'2`3\\\"\",", 5, str.length(), '"'),
                QueryParser.newPart(str.length() - 1, "1'2`3\""));
    }

    @Test(groups = { "unit" })
    public void testExtractPartBefore() {
        String str;
        Assert.assertEquals(QueryParser.extractPartBefore(str = "", 0, str.length()),
                QueryParser.newPart(str.length(), ""));
        Assert.assertEquals(QueryParser.extractPartBefore(str = "abc", 0, str.length()),
                QueryParser.newPart(str.length(), "abc"));
        Assert.assertEquals(QueryParser.extractPartBefore(str = "abc", 1, str.length()),
                QueryParser.newPart(str.length(), "bc"));
        Assert.assertEquals(QueryParser.extractPartBefore(str = "abc", 1, str.length(), 'b'),
                QueryParser.newPart(str.length() - 2, ""));
        Assert.assertEquals(QueryParser.extractPartBefore(str = "abc", 1, str.length(), 'c'),
                QueryParser.newPart(str.length() - 1, "b"));

        Assert.assertEquals(QueryParser.extractPartBefore(str = "a=b,c=1", 2, str.length(), ','),
                QueryParser.newPart(3, "b"));
        Assert.assertEquals(QueryParser.extractPartBefore(str = "a=b ) c=1", 2, str.length(), ',', ')'),
                QueryParser.newPart(4, "b"));
    }

    @Test(groups = { "unit" })
    public void testExtractPropertyName() {
        Assert.assertThrows(IllegalArgumentException.class, () -> QueryParser.extractPropertyNamePart("", 0, 0));

        String str;
        Assert.assertEquals(QueryParser.extractPropertyNamePart(str = "a1=", 0, str.length()),
                QueryParser.newPart(2, "a1"));
        Assert.assertEquals(QueryParser.extractPropertyNamePart(str = " a = ", 0, str.length()),
                QueryParser.newPart(2, "a"));
    }

    @Test(groups = { "unit" })
    public void testExtractPropertyValue() {
        Assert.assertThrows(IllegalArgumentException.class, () -> QueryParser.extractPropertyValuePart("'", 0, 1));
        Assert.assertEquals(QueryParser.extractPropertyValuePart("", 0, 0), QueryParser.newPart(0, ""));
        Assert.assertEquals(QueryParser.extractPropertyValuePart("'", 0, 0), QueryParser.newPart(0, ""));

        String str;
        Assert.assertEquals(QueryParser.extractPropertyValuePart(str = "a='x\\'1'''", 2, str.length()),
                QueryParser.newPart(str.length(), "x'1'"));
        Assert.assertEquals(QueryParser.extractPropertyValuePart(str = "a = 123", 3, str.length()),
                QueryParser.newPart(str.length(), "123"));
    }

    @Test(groups = { "unit" })
    public void testExtractProperties() {
        Properties props = new Properties();
        Properties expected = new Properties();

        String str;
        Assert.assertEquals(QueryParser.extractProperties(str = "", 0, str.length(), props), -1);
        Assert.assertEquals(props, expected);

        expected.setProperty("a.b", "");
        Assert.assertEquals(QueryParser.extractProperties(str = "a.b=", 0, str.length(), props), str.length() - 1);
        Assert.assertEquals(props, expected);

        props.clear();
        Assert.assertEquals(QueryParser.extractProperties(str = " a.b = ", 0, str.length(), props), str.length() - 1);
        Assert.assertEquals(props, expected);

        props.clear();
        expected.setProperty("cc", "233");
        expected.setProperty("dd", "322");
        expected.setProperty("e.e", "5");
        Assert.assertEquals(
                QueryParser.extractProperties(str = " a.b =, cc=233,dd='322'\t,e.e=5 )", 0, str.length(), props),
                str.length() - 1);
        Assert.assertEquals(props, expected);
    }

    @Test(groups = { "unit" })
    public void testIndexOf() {
        String str;
        Assert.assertEquals(QueryParser.indexOf(str = "{{}}", 0, "{{", '\\'), 2);
        Assert.assertEquals(QueryParser.indexOf(str = "{{}}", 0, "}}", '\\'), str.length());
        Assert.assertEquals(QueryParser.indexOf(str = "{{}} ", 0, "}}", '\\'), str.length() - 1);
        Assert.assertEquals(QueryParser.indexOf(str = "{{\\}}", 0, "}}", '\\'), -1);
        Assert.assertEquals(QueryParser.indexOf(str = "{{\\}}}", 0, "}}", '\\'), str.length());
    }

    @Test(groups = { "unit" })
    public void testParseExecutableBlock() {
        Properties props = new Properties();
        Properties expected = new Properties();

        String str;
        Assert.assertEquals(QueryParser.parseExecutableBlock(str = "", props), new String[] { "", "" });
        Assert.assertEquals(props, expected);

        Assert.assertEquals(QueryParser.parseExecutableBlock(str = "script", props), new String[] { "", "script" });
        Assert.assertEquals(props, expected);
        Assert.assertEquals(QueryParser.parseExecutableBlock(str = "select 1", props), new String[] { "", "select 1" });
        Assert.assertEquals(props, expected);

        expected.setProperty("custom.classpath", "/usr/local/lib");
        expected.setProperty("cli.timeout", "1500");
        expected.setProperty("cli.path", "~/.cargo/bin/prqlc");
        Assert.assertEquals(QueryParser.parseExecutableBlock(
                str = "prql(custom.classpath=/usr/local/lib,cli.timeout=1500, cli.path='~/.cargo/bin/prqlc'): from `test.test1` | take 10",
                props), new String[] { "prql", "from `test.test1` | take 10" });
        Assert.assertEquals(props, expected);

        props.clear();
        Assert.assertEquals(QueryParser.parseExecutableBlock(
                str = " prql\r\n\t(\ncustom.classpath = /usr/local/lib, cli.timeout =1500,\r\ncli.path= '~/.cargo/bin/prqlc' ) :\nfrom `test.test1` | take 10 ",
                props), new String[] { "prql", "from `test.test1` | take 10 " });
        Assert.assertEquals(props, expected);
    }

    @Test(groups = { "unit" })
    public void testParse() {
        String str;
        Assert.assertEquals(QueryParser.parse(str = ""),
                new ParsedQuery(Collections.emptyList(), Collections.emptyList()));
        Assert.assertEquals(QueryParser.parse(str = "select 1"),
                new ParsedQuery(Arrays.asList(str), Collections.emptyList()));
        Assert.assertEquals(QueryParser.parse(str = "select\\{%} {{ 1"),
                new ParsedQuery(Arrays.asList("select{%} {{ 1"), Collections.emptyList()));
        Assert.assertEquals(QueryParser.parse(str = "select {{1}}"),
                new ParsedQuery(Arrays.asList("select ", ""),
                        Arrays.asList(new ExecutableBlock(1, "", new Properties(), "1", true))));
        Assert.assertEquals(
                QueryParser
                        .parse(str = "select {{ script: helper.format('%s', '*')}} from {% shell: echo 'mytable' %}"),
                new ParsedQuery(Arrays.asList("select ", "", " from ", ""),
                        Arrays.asList(
                                new ExecutableBlock(1, "script", new Properties(), "helper.format('%s', '*')", true),
                                new ExecutableBlock(3, "shell", new Properties(), "echo 'mytable' ", false))));
    }
}
