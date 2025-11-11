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
package io.github.jdbcx.driver;

import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.github.jdbcx.Constants;
import io.github.jdbcx.Option;
import io.github.jdbcx.VariableTag;

public class QueryParserTest {
    @Test(groups = { "unit" })
    public void testExtractQuotedString() {
        Assert.assertEquals(QueryParser.extractQuotedPart("\0", 0, 1, null, null, '\0'), QueryParser.newPart(1, ""));

        Properties props = new Properties();
        Assert.assertThrows(IllegalArgumentException.class,
                () -> QueryParser.extractQuotedPart("'", 2, 1, null, props, '\''));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> QueryParser.extractQuotedPart("' ", 1, 2, null, props, '\''));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> QueryParser.extractQuotedPart("'", 1, 1, null, props, '\''));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> QueryParser.extractQuotedPart("`123", 1, 4, null, props, '`'));

        String str;
        Assert.assertEquals(QueryParser.extractQuotedPart(str = "'", 0, str.length(), null, props, '\''),
                QueryParser.newPart(1, ""));
        Assert.assertEquals(QueryParser.extractQuotedPart(str = " '' \\''", 1, str.length(), null, props, '\''),
                QueryParser.newPart(7, "' '"));
        Assert.assertEquals(
                QueryParser.extractQuotedPart(str = "a = \"1'2`3\\\"\",", 5, str.length(), null, props, '"'),
                QueryParser.newPart(str.length() - 1, "1'2`3\""));

        props.setProperty("secret", "***");
        Assert.assertEquals(QueryParser.extractQuotedPart(str = " ${secret}' ", 0, str.length(), null, props, '\''),
                QueryParser.newPart(str.length() - 1, " ***"));
    }

    @Test(groups = { "unit" })
    public void testExtractPartBefore() {
        Assert.assertEquals(QueryParser.extractPartBefore("", 0, 0, null, null), QueryParser.newPart(0, ""));

        Properties props = new Properties();
        String str;
        Assert.assertEquals(QueryParser.extractPartBefore(str = "", 0, str.length(), null, props),
                QueryParser.newPart(str.length(), ""));
        Assert.assertEquals(QueryParser.extractPartBefore(str = "abc", 0, str.length(), null, props),
                QueryParser.newPart(str.length(), "abc"));
        Assert.assertEquals(QueryParser.extractPartBefore(str = "abc", 1, str.length(), null, props),
                QueryParser.newPart(str.length(), "bc"));
        Assert.assertEquals(QueryParser.extractPartBefore(str = "abc", 1, str.length(), null, props, 'b'),
                QueryParser.newPart(str.length() - 2, ""));
        Assert.assertEquals(QueryParser.extractPartBefore(str = "abc", 1, str.length(), null, props, 'c'),
                QueryParser.newPart(str.length() - 1, "b"));

        Assert.assertEquals(QueryParser.extractPartBefore(str = "a=b,c=1", 2, str.length(), null, props, ','),
                QueryParser.newPart(3, "b"));
        Assert.assertEquals(QueryParser.extractPartBefore(str = "a=b ) c=1", 2, str.length(), null, props, ',', ')'),
                QueryParser.newPart(4, "b"));

        props.setProperty("secret", "321");
        props.setProperty("magic", "123");
        Assert.assertEquals(
                QueryParser.extractPartBefore(str = " ${secret} ${magic} ) c=1", 1, str.length(), null, props, ',',
                        ')'),
                QueryParser.newPart(20, "321 123"));
    }

    @Test(groups = { "unit" })
    public void testExtractPropertyName() {
        Assert.assertEquals(QueryParser.extractPropertyNamePart("a =", 0, 3, null, null), QueryParser.newPart(1, "a"));

        Properties props = new Properties();
        Assert.assertThrows(IllegalArgumentException.class,
                () -> QueryParser.extractPropertyNamePart("", 0, 0, null, props));

        String str;
        Assert.assertEquals(QueryParser.extractPropertyNamePart(str = "a1=", 0, str.length(), null, props),
                QueryParser.newPart(2, "a1"));
        Assert.assertEquals(QueryParser.extractPropertyNamePart(str = " a = ", 0, str.length(), null, props),
                QueryParser.newPart(2, "a"));

        props.setProperty("x", "y");
        Assert.assertEquals(QueryParser.extractPropertyNamePart(str = " ${x}a = ", 0, str.length(), null, props),
                QueryParser.newPart(6, "ya"));
    }

    @Test(groups = { "unit" })
    public void testExtractPropertyValue() {
        Assert.assertEquals(QueryParser.extractPropertyValuePart("", 0, 0, null, null), QueryParser.newPart(0, ""));

        Properties props = new Properties();
        Assert.assertThrows(IllegalArgumentException.class,
                () -> QueryParser.extractPropertyValuePart("'", 0, 1, null, props));
        Assert.assertEquals(QueryParser.extractPropertyValuePart("", 0, 0, null, props), QueryParser.newPart(0, ""));
        Assert.assertEquals(QueryParser.extractPropertyValuePart("'", 0, 0, null, props), QueryParser.newPart(0, ""));

        String str;
        Assert.assertEquals(QueryParser.extractPropertyValuePart(str = "a='x\\'1'''", 2, str.length(), null, props),
                QueryParser.newPart(str.length(), "x'1'"));
        Assert.assertEquals(QueryParser.extractPropertyValuePart(str = "a = 123", 3, str.length(), null, props),
                QueryParser.newPart(str.length(), "123"));

        props.setProperty("magic", "***");
        Assert.assertEquals(QueryParser.extractPropertyValuePart(str = "a = ${magic}", 3, str.length(), null, props),
                QueryParser.newPart(str.length(), "***"));
    }

    @Test(groups = { "unit" })
    public void testExtractProperties() {
        Assert.assertThrows(NullPointerException.class, () -> QueryParser.extractProperties("a=1", 0, 3, null, null));

        Properties props = new Properties();
        Properties expected = new Properties();

        String str;
        Assert.assertEquals(QueryParser.extractProperties(str = "", 0, str.length(), null, props), -1);
        Assert.assertEquals(props, expected);

        expected.setProperty("a.b", "");
        Assert.assertEquals(QueryParser.extractProperties(str = "a.b=", 0, str.length(), null, props),
                str.length() - 1);
        Assert.assertEquals(props, expected);

        props.clear();
        expected.setProperty("a.b", "c d e");
        Assert.assertEquals(QueryParser.extractProperties(str = " a.b=c d\\ e", 0, str.length(), null, props),
                str.length() - 1);
        Assert.assertEquals(props, expected);

        props.clear();
        expected.setProperty("a.b", "");
        Assert.assertEquals(QueryParser.extractProperties(str = " a.b = ", 0, str.length(), null, props),
                str.length() - 1);
        Assert.assertEquals(props, expected);

        props.clear();
        expected.setProperty("cc", "b=2&c=3");
        expected.setProperty("dd", "d=4,e=5");
        Assert.assertEquals(
                QueryParser.extractProperties(str = " a.b = ,cc =b=2&c=3, dd=d=4\\,e=5 )", 0, str.length(), null, props),
                str.length() - 1);
        Assert.assertEquals(props, expected);

        props.clear();
        expected.setProperty("cc", "233");
        expected.setProperty("dd", "322");
        expected.setProperty("e.e", "5");
        Assert.assertEquals(
                QueryParser.extractProperties(str = " a.b =, cc=233,dd='322'\t,e.e=5 )", 0, str.length(), null, props),
                str.length() - 1);
        Assert.assertEquals(props, expected);

        props.clear();
        props.setProperty("secret", "110");
        props.setProperty("magic", "233");
        expected.clear();
        expected.setProperty("110", "233");
        expected.setProperty("secret", "123");
        expected.setProperty("magic", "321");
        expected.setProperty("123", "5");
        Assert.assertEquals(
                QueryParser.extractProperties(str = " ${secret} = ${magic}, secret=123,magic='321'\t,${secret}=5 )", 0,
                        str.length(), null, props),
                str.length() - 1);
        Assert.assertEquals(props, expected);
    }

    @Test(groups = { "unit" })
    public void testIndexOf() {
        Assert.assertThrows(NullPointerException.class, () -> QueryParser.indexOf(null, 1, ' ', '\\'));
        Assert.assertThrows(NullPointerException.class, () -> QueryParser.indexOf(null, 1, "", '\\'));

        String str;
        Assert.assertEquals(QueryParser.indexOf(str = "", 0, "{{", '\\'), -1);
        Assert.assertEquals(QueryParser.indexOf(str = "{{}}", 0, "{{", '\\'), 2);
        Assert.assertEquals(QueryParser.indexOf(str = "{{}}", 0, "}}", '\\'), str.length());
        Assert.assertEquals(QueryParser.indexOf(str = "{{}} ", 0, "}}", '\\'), str.length() - 1);
        Assert.assertEquals(QueryParser.indexOf(str = "{{}} ", 10, "}}", '\\'), -1);
        Assert.assertEquals(QueryParser.indexOf(str = "{{\\}}", 0, "}}", '\\'), -1);
        Assert.assertEquals(QueryParser.indexOf(str = "{{\\}}}", 0, "}}", '\\'), str.length());

        Assert.assertEquals(QueryParser.indexOf(str = "", 0, ')', '\\'), -1);
        Assert.assertEquals(QueryParser.indexOf(str = ")", 0, ')', '\\'), 1);
        Assert.assertEquals(QueryParser.indexOf(str = ")", 5, ')', '\\'), -1);
        Assert.assertEquals(QueryParser.indexOf(str = "\\)", 0, ')', '\\'), -1);
        Assert.assertEquals(QueryParser.indexOf(str = "a(a=1\\))", 0, ')', '\\'), str.length());
        Assert.assertEquals(QueryParser.indexOf(str = "a(a=1\\)", 0, ')', '\\'), -1);
    }

    @Test(groups = { "unit" })
    public void testParseIndentifier() {
        Properties props = new Properties();

        String str;
        Assert.assertEquals(QueryParser.parseIdentifier(str = "", 0, 0, VariableTag.BRACE, props),
                new int[] { 0, 0, 0 });
        Assert.assertEquals(Option.ID.getValue(props), "");
        Assert.assertEquals(QueryParser.parseIdentifier(str = "a.?", 2, 3, VariableTag.BRACE, props),
                new int[] { 3, 3, 1 });
        Assert.assertEquals(Option.ID.getValue(props), "?");
        Assert.assertEquals(QueryParser.parseIdentifier(str = "a.?:", 2, 4, VariableTag.BRACE, props),
                new int[] { 2, 3, 1 });
        Assert.assertEquals(Option.ID.getValue(props), "?");
        Assert.assertEquals(QueryParser.parseIdentifier(str = "a.?()", 2, 5, VariableTag.BRACE, props),
                new int[] { 2, 3, 1 });
        Assert.assertEquals(Option.ID.getValue(props), "?");
        props.clear();

        Assert.assertEquals(QueryParser.parseIdentifier(str = "a.(x=1)", 2, str.length(), VariableTag.BRACE, props),
                new int[] { 1, 2, 0 });
        Assert.assertEquals(Option.ID.getValue(props), "");
        Assert.assertEquals(QueryParser.parseIdentifier(str = "a.b(x=1)", 2, str.length(), VariableTag.BRACE, props),
                new int[] { 2, 3, 0 });
        Assert.assertEquals(Option.ID.getValue(props), "b");

        Assert.assertEquals(QueryParser.parseIdentifier(str = "a.b?(x=1)", 2, str.length(), VariableTag.BRACE, props),
                new int[] { 3, 4, 1 });
        Assert.assertEquals(Option.ID.getValue(props), "b?");

        Assert.assertEquals(
                QueryParser.parseIdentifier(str = "a.\\b\\?(x=1)", 2, str.length(), VariableTag.BRACE, props),
                new int[] { 5, 6, 0 });
        Assert.assertEquals(Option.ID.getValue(props), "\\b\\?");

        Assert.assertEquals(
                QueryParser.parseIdentifier(str = "a.[a-b.*]c(x=1)", 2, str.length(), VariableTag.BRACE, props),
                new int[] { 9, 10, 1 });
        Assert.assertEquals(Option.ID.getValue(props), "[a-b.*]c");

        Assert.assertEquals(
                QueryParser.parseIdentifier(str = "a.[a-b.*]\\c(x=1)", 2, str.length(), VariableTag.BRACE, props),
                new int[] { 10, 11, 1 });
        Assert.assertEquals(Option.ID.getValue(props), "[a-b.*]\\c");

        Assert.assertEquals(
                QueryParser.parseIdentifier(str = "a.a[a-b.*]", 2, str.length(), VariableTag.SQUARE_BRACKET, props),
                new int[] { 10, 10, 0 });
        Assert.assertEquals(Option.ID.getValue(props), "a[a-b.*]");
        Assert.assertEquals(
                QueryParser.parseIdentifier(str = "a.a[a-b.*]:", 2, str.length(), VariableTag.SQUARE_BRACKET, props),
                new int[] { 9, 10, 0 });
        Assert.assertEquals(Option.ID.getValue(props), "a[a-b.*]");

        Assert.assertEquals(
                QueryParser.parseIdentifier(str = "a.ch-[ap]*:", 2, str.length(), VariableTag.BRACE, props),
                new int[] { 9, 10, 2 });
        Assert.assertEquals(Option.ID.getValue(props), "ch-[ap]*");
    }

    @Test(groups = { "unit" })
    public void testParseExecutableBlock() {
        Assert.assertThrows(NullPointerException.class,
                () -> QueryParser.parseExecutableBlock("a(b=1)", VariableTag.BRACE, null));

        Properties props = new Properties();
        Properties expected = new Properties();

        String str;
        Assert.assertEquals(QueryParser.parseExecutableBlock(str = "", VariableTag.BRACE, props),
                new String[] { "", "", "" });
        Assert.assertEquals(props, expected);

        Assert.assertEquals(QueryParser.parseExecutableBlock(str = "script", VariableTag.BRACE, props),
                new String[] { "script", "", "" });
        Assert.assertEquals(props, expected);
        Assert.assertEquals(QueryParser.parseExecutableBlock(str = "select 1", VariableTag.BRACE, props),
                new String[] { "", "select 1", "" });
        Assert.assertEquals(props, expected);

        Assert.assertEquals(QueryParser.parseExecutableBlock(str = "prql()", VariableTag.BRACE, props),
                new String[] { "prql", "", "" });
        Assert.assertEquals(QueryParser.parseExecutableBlock(str = "prql ( ) ", VariableTag.BRACE, props),
                new String[] { "prql", "", "" });
        Assert.assertEquals(QueryParser.parseExecutableBlock(str = "prql ( ) : ", VariableTag.BRACE, props),
                new String[] { "prql", "", "" });
        Assert.assertEquals(QueryParser.parseExecutableBlock(str = "prql():", VariableTag.BRACE, props),
                new String[] { "prql", "", "" });

        expected.setProperty("custom.classpath", "/usr/local/lib");
        expected.setProperty("cli.timeout", "1500");
        expected.setProperty("cli.path", "~/.cargo/bin/prqlc");
        Assert.assertEquals(QueryParser.parseExecutableBlock(
                str = "prql(custom.classpath=/usr/local/lib,cli.timeout=1500, cli.path='~/.cargo/bin/prqlc')",
                VariableTag.BRACE, props), new String[] { "prql", "", "" });
        Assert.assertEquals(props, expected);

        props.clear();
        Assert.assertEquals(QueryParser.parseExecutableBlock(
                str = "prql(custom.classpath=/usr/local/lib,cli.timeout=1500, cli.path='~/.cargo/bin/prqlc'): from `test.test1` | take 10",
                VariableTag.BRACE, props), new String[] { "prql", "from `test.test1` | take 10", "" });
        Assert.assertEquals(props, expected);

        props.clear();
        Assert.assertEquals(QueryParser.parseExecutableBlock(
                str = " prql\r\n\t(\ncustom.classpath = /usr/local/lib, cli.timeout =1500,\r\ncli.path= '~/.cargo/bin/prqlc' ) :\nfrom `test.test1` | take 10 ",
                VariableTag.BRACE, props), new String[] { "prql", "from `test.test1` | take 10 ", "" });
        Assert.assertEquals(props, expected);

        props.clear();
        props.setProperty("my.option", "trim");
        props.setProperty("my.preference", Constants.TRUE_EXPR);
        expected.clear();
        expected.putAll(props);
        expected.setProperty("result.string.trim", Constants.TRUE_EXPR);
        Assert.assertEquals(QueryParser.parseExecutableBlock(
                str = " shell\r\n\t(\nresult.string.${my.option} = '${my.preference}' ) :\nfrom `test.test1` | take 10 ",
                VariableTag.BRACE, props), new String[] { "shell", "from `test.test1` | take 10 ", "" });
        Assert.assertEquals(props, expected);
    }

    @Test(groups = { "unit" })
    public void testSplit() {
        Assert.assertEquals(QueryParser.split(null), Collections.emptyList());
        Assert.assertEquals(QueryParser.split(""), Collections.emptyList());
        Assert.assertEquals(QueryParser.split("--;; "), Collections.emptyList());
        Assert.assertEquals(QueryParser.split("--;; \n--;; "), Collections.emptyList());
        Assert.assertEquals(QueryParser.split("--;; \n--;; \n--;; "), Collections.emptyList());
        Assert.assertEquals(QueryParser.split("--;; \n--;; \n--;; \n--;; "), Collections.emptyList());
        Assert.assertEquals(QueryParser.split(" \r\n\t  "), Collections.emptyList());
        Assert.assertEquals(QueryParser.split(" \r\n--;; \t  "), Collections.emptyList());
        Assert.assertEquals(QueryParser.split("--;; \n--;; ...\r\n \r\n--;; \t  "), Collections.emptyList());

        String[][] expected = new String[][] { { "Query #1", "select 1" } };
        Assert.assertEquals(QueryParser.split("select 1").toArray(expected), expected);
        Assert.assertEquals(QueryParser.split("--;; the first query\nselect 1").toArray(expected),
                expected = new String[][] { { "the first query", "select 1" } });
        Assert.assertEquals(
                QueryParser.split(
                        "\n--;; q1\n--;; q2\n\n--;; q3 \n\n\n--;; the first query\n--;; my query\n--;; \nselect 1\n--;; test query")
                        .toArray(expected),
                expected = new String[][] { { "Query #1", "select 1" } });
        Assert.assertEquals(QueryParser.split(" select 1 \n--;; \t  \r\n\tselect 2\r\n\n").toArray(expected),
                expected = new String[][] { { "Query #1", "select 1" }, { "Query #2", "select 2" } });
        Assert.assertEquals(
                QueryParser.split("--;; q1\n select 1 \n--;; q3 \n--;;  q2\t  \r\n\tselect 2\r\n--;; ")
                        .toArray(expected),
                expected = new String[][] { { "q1", "select 1" }, { "q2", "select 2" } });
    }

    @Test(groups = { "unit" })
    public void testParse() {
        Properties props = new Properties();
        String str;
        Assert.assertEquals(QueryParser.parse(str = "", VariableTag.BRACE, props),
                new ParsedQuery(Collections.emptyList(), Collections.emptyList()));
        Assert.assertEquals(QueryParser.parse(str = "select 1", VariableTag.BRACE, props),
                new ParsedQuery(Arrays.asList(str), Collections.emptyList()));

        Assert.assertEquals(QueryParser.parse("{%var%}", VariableTag.BRACE, props),
                new ParsedQuery(Arrays.asList("", ""),
                        Arrays.asList(new ExecutableBlock(1, "var", new Properties(), "", false))));
        Assert.assertEquals(QueryParser.parse("{%var:%}", VariableTag.BRACE, props),
                new ParsedQuery(Arrays.asList("", ""),
                        Arrays.asList(new ExecutableBlock(1, "var", new Properties(), "", false))));
        Assert.assertEquals(QueryParser.parse("{% var: %}", VariableTag.BRACE, props),
                new ParsedQuery(Arrays.asList("", ""),
                        Arrays.asList(new ExecutableBlock(1, "var", new Properties(), "", false))));
        Assert.assertEquals(QueryParser.parse("{%var: %}", VariableTag.BRACE, props),
                new ParsedQuery(Arrays.asList("", ""),
                        Arrays.asList(new ExecutableBlock(1, "var", new Properties(), "", false))));
        Assert.assertEquals(QueryParser.parse("{% var %}", VariableTag.BRACE, props),
                new ParsedQuery(Arrays.asList("", ""),
                        Arrays.asList(new ExecutableBlock(1, "var", new Properties(), "", false))));
        Assert.assertEquals(QueryParser.parse("{% var: %}", VariableTag.BRACE, props),
                new ParsedQuery(Arrays.asList("", ""),
                        Arrays.asList(new ExecutableBlock(1, "var", new Properties(), "", false))));
        Assert.assertEquals(QueryParser.parse("{% var: a=%%%}", VariableTag.BRACE, props),
                new ParsedQuery(Arrays.asList("", ""),
                        Arrays.asList(new ExecutableBlock(1, "var", new Properties(), "a=%%", false))));

        Assert.assertEquals(QueryParser.parse("{{web}}", VariableTag.BRACE, props),
                new ParsedQuery(Arrays.asList("", ""),
                        Arrays.asList(new ExecutableBlock(1, "web", new Properties(), "", true))));
        Assert.assertEquals(QueryParser.parse("{{web:}}", VariableTag.BRACE, props),
                new ParsedQuery(Arrays.asList("", ""),
                        Arrays.asList(new ExecutableBlock(1, "web", new Properties(), "", true))));
        Assert.assertEquals(QueryParser.parse("{{ web:}}", VariableTag.BRACE, props),
                new ParsedQuery(Arrays.asList("", ""),
                        Arrays.asList(new ExecutableBlock(1, "web", new Properties(), "", true))));
        Assert.assertEquals(QueryParser.parse("{{web: }}", VariableTag.BRACE, props),
                new ParsedQuery(Arrays.asList("", ""),
                        Arrays.asList(new ExecutableBlock(1, "web", new Properties(), "", true))));
        Assert.assertEquals(QueryParser.parse("{{ web }}", VariableTag.BRACE, props),
                new ParsedQuery(Arrays.asList("", ""),
                        Arrays.asList(new ExecutableBlock(1, "web", new Properties(), "", true))));
        Assert.assertEquals(QueryParser.parse("{{ web: }}", VariableTag.BRACE, props),
                new ParsedQuery(Arrays.asList("", ""),
                        Arrays.asList(new ExecutableBlock(1, "web", new Properties(), "", true))));

        Assert.assertEquals(QueryParser.parse("{{script:1+1}}", VariableTag.BRACE, props),
                new ParsedQuery(Arrays.asList("", ""),
                        Arrays.asList(new ExecutableBlock(1, "script", new Properties(), "1+1", true))));
        Assert.assertEquals(QueryParser.parse("{{ script: 1+1 }}", VariableTag.BRACE, props),
                new ParsedQuery(Arrays.asList("", ""),
                        Arrays.asList(new ExecutableBlock(1, "script", new Properties(), "1+1", true))));
        Assert.assertEquals(QueryParser.parse("{{ script: {1+1}}}", VariableTag.BRACE, props),
                new ParsedQuery(Arrays.asList("", ""),
                        Arrays.asList(new ExecutableBlock(1, "script", new Properties(), "{1+1}", true))));
        Assert.assertEquals(QueryParser.parse("{{ script: {1+1}\n}}", VariableTag.BRACE, props),
                new ParsedQuery(Arrays.asList("", ""),
                        Arrays.asList(new ExecutableBlock(1, "script", new Properties(), "{1+1}", true))));

        Assert.assertEquals(
                QueryParser.parse(
                        str = "helper.format('select now() t, arrayJoin(splitByChar('\\n', '%s')) a\", \"a\\nb\\nc\")",
                        VariableTag.BRACE, props),
                new ParsedQuery(Arrays.asList(str), Collections.emptyList()));

        Assert.assertEquals(QueryParser.parse(str = "select\\{%} {\\{{ 1{", VariableTag.BRACE, props),
                new ParsedQuery(Arrays.asList("select{%} {\\{{ 1{"), Collections.emptyList()));
        Assert.assertEquals(
                QueryParser.parse(str = "select {%-no interested%}{{-same here}}", VariableTag.BRACE, props),
                new ParsedQuery(Arrays.asList("select "), Collections.emptyList()));

        Properties props1 = new Properties();
        props1.setProperty("a", "1");
        Properties props2 = new Properties();
        props2.setProperty("b", "2");
        // without body
        Assert.assertEquals(QueryParser.parse(str = "select {%proc(a=1)%}{{func(b=2)}}", VariableTag.BRACE, props),
                new ParsedQuery(Arrays.asList("select ", "", "", ""),
                        Arrays.asList(new ExecutableBlock(1, "proc", props1, "", false),
                                new ExecutableBlock(3, "func", props2, "", true))));

        // full expression
        Assert.assertEquals(
                QueryParser.parse(str = "select {%proc(a=1):execute()%}{{func(b=2):return x;}}", VariableTag.BRACE,
                        props),
                new ParsedQuery(Arrays.asList("select ", "", "", ""),
                        Arrays.asList(new ExecutableBlock(1, "proc", props1, "execute()", false),
                                new ExecutableBlock(3, "func", props2, "return x;", true))));

        Assert.assertEquals(QueryParser.parse(str = "select{%2%} {{{1}}}\\\\{{\\}}", VariableTag.BRACE, props),
                new ParsedQuery(Arrays.asList("select", "", " ", "", "\\\\{{\\}}"),
                        Arrays.asList(new ExecutableBlock(1, "", new Properties(), "2", false),
                                new ExecutableBlock(3, "", new Properties(), "{1}", true))));
        Assert.assertEquals(
                QueryParser
                        .parse(str = "select {{ script: helper.format('%s', '*')}} from {% shell: echo 'mytable' %}",
                                VariableTag.BRACE, props),
                new ParsedQuery(Arrays.asList("select ", "", " from ", ""),
                        Arrays.asList(
                                new ExecutableBlock(1, "script", new Properties(), "helper.format('%s', '*')", true),
                                new ExecutableBlock(3, "shell", new Properties(), "echo 'mytable'", false))));
    }

    @Test(groups = { "unit" })
    public void testParseDotNotation() {
        Properties props = new Properties();
        String str;

        Assert.assertThrows(IllegalArgumentException.class,
                () -> QueryParser.parse("{{web.{}}", VariableTag.BRACE, props));

        Assert.assertEquals(QueryParser.parse(str = "{{web}}", VariableTag.BRACE, props),
                new ParsedQuery(Arrays.asList("", ""), Arrays.asList(new ExecutableBlock(1, "web", props, "", true))));
        Assert.assertEquals(QueryParser.parse(str = "{{web.}}", VariableTag.BRACE, props),
                new ParsedQuery(Arrays.asList("", ""), Arrays.asList(new ExecutableBlock(1, "web", props, "", true))));
        Assert.assertEquals(QueryParser.parse(str = "{{ web. }}", VariableTag.BRACE, props),
                new ParsedQuery(Arrays.asList("", ""), Arrays.asList(new ExecutableBlock(1, "web", props, "", true))));

        Properties newProps = new Properties();
        Option.ID.setValue(newProps, "a?*[!x]c");
        Assert.assertEquals(QueryParser.parse(str = "{{ web.a?*[!x]c() }}", VariableTag.BRACE, props),
                new ParsedQuery(Arrays.asList("", ""),
                        Arrays.asList(new ExecutableBlock(1, "web", newProps, "", true))));

        Assert.assertEquals(QueryParser.parse(str = "{{web.()}}", VariableTag.BRACE, props),
                new ParsedQuery(Arrays.asList("", ""), Arrays.asList(new ExecutableBlock(1, "web", props, "", true))));
        Assert.assertEquals(QueryParser.parse(str = "{{web.:}}", VariableTag.BRACE, props),
                new ParsedQuery(Arrays.asList("", ""), Arrays.asList(new ExecutableBlock(1, "web", props, "", true))));
        Assert.assertEquals(QueryParser.parse(str = "{{web.():}}", VariableTag.BRACE, props),
                new ParsedQuery(Arrays.asList("", ""), Arrays.asList(new ExecutableBlock(1, "web", props, "", true))));
        Assert.assertEquals(QueryParser.parse(str = "{{  web. (   ) :  }}", VariableTag.BRACE, props),
                new ParsedQuery(Arrays.asList("", ""), Arrays.asList(new ExecutableBlock(1, "web", props, "", true))));
        Assert.assertEquals(QueryParser.parse(str = "{{  web. (   ) :  request body }}", VariableTag.BRACE, props),
                new ParsedQuery(Arrays.asList("", ""),
                        Arrays.asList(new ExecutableBlock(1, "web", props, "request body", true))));

        Properties expectedProps = new Properties();
        Option.ID.setValue(expectedProps, "ch-dev");
        Assert.assertEquals(QueryParser.parse(str = "{{ web. ( id = 'ch-dev' ) }}", VariableTag.BRACE, props),
                new ParsedQuery(Arrays.asList("", ""),
                        Arrays.asList(new ExecutableBlock(1, "web", expectedProps, "", true))));
        Assert.assertEquals(QueryParser.parse(str = "{{ web. (id = ch-dev): request }}", VariableTag.BRACE, props),
                new ParsedQuery(Arrays.asList("", ""),
                        Arrays.asList(new ExecutableBlock(1, "web", expectedProps, "request", true))));
        Assert.assertEquals(QueryParser.parse(str = "{{ web.ch-dev }}", VariableTag.BRACE, props),
                new ParsedQuery(Arrays.asList("", ""),
                        Arrays.asList(new ExecutableBlock(1, "web", expectedProps, "", true))));
        Assert.assertEquals(QueryParser.parse(str = "{{ web.ch-dev: my request }}", VariableTag.BRACE, props),
                new ParsedQuery(Arrays.asList("", ""),
                        Arrays.asList(new ExecutableBlock(1, "web", expectedProps, "my request", true))));
        Assert.assertEquals(
                QueryParser.parse(str = "{{ web.ch-dev1 (id=ch-dev): my request }}", VariableTag.BRACE, props),
                new ParsedQuery(Arrays.asList("", ""),
                        Arrays.asList(new ExecutableBlock(1, "web", expectedProps, "my request", true))));

        Option.ID.setValue(expectedProps, "$");
        Assert.assertEquals(QueryParser.parse(str = "{{ web.$: my request }}", VariableTag.BRACE, props),
                new ParsedQuery(Arrays.asList("", ""),
                        Arrays.asList(new ExecutableBlock(1, "web", expectedProps, "my request", true))));
        Option.ID.setValue(expectedProps, "${}");
        Assert.assertEquals(QueryParser.parse(str = "{{ web.${}}}", VariableTag.BRACE, props),
                new ParsedQuery(Arrays.asList("", ""),
                        Arrays.asList(new ExecutableBlock(1, "web", expectedProps, "", true))));
        Option.ID.setValue(expectedProps, "${x}");
        Assert.assertEquals(QueryParser.parse(str = "{{ web.${x}}}", VariableTag.BRACE, props),
                new ParsedQuery(Arrays.asList("", ""),
                        Arrays.asList(new ExecutableBlock(1, "web", expectedProps, "", true))));
        Option.ID.setValue(expectedProps, "${x}x");
        Assert.assertEquals(QueryParser.parse(str = "{{ web.${x}x}}", VariableTag.BRACE, props),
                new ParsedQuery(Arrays.asList("", ""),
                        Arrays.asList(new ExecutableBlock(1, "web", expectedProps, "", true))));
        Assert.assertEquals(QueryParser.parse(str = "{{ web.${x}x }}", VariableTag.BRACE, props),
                new ParsedQuery(Arrays.asList("", ""),
                        Arrays.asList(new ExecutableBlock(1, "web", expectedProps, "", true))));

        Option.ID.setValue(expectedProps, "${x}");
        Assert.assertEquals(
                QueryParser.parse(str = "{% var: x=ch-dev %}{{ web.${x}: my request }}", VariableTag.BRACE, props),
                new ParsedQuery(Arrays.asList("", "", "", ""),
                        Arrays.asList(new ExecutableBlock(1, "var", props, "x=ch-dev", false),
                                new ExecutableBlock(3, "web", expectedProps, "my request", true))));

        // remote table
        props.clear();
        expectedProps.clear();
        Assert.assertEquals(QueryParser.parse("{{ table }}", VariableTag.BRACE, props),
                new ParsedQuery(Arrays.asList("", ""),
                        Arrays.asList(new ExecutableBlock(1, "table", props, "", true))));
        Assert.assertEquals(QueryParser.parse("{{ table. }}", VariableTag.BRACE, props),
                new ParsedQuery(Arrays.asList("", ""),
                        Arrays.asList(new ExecutableBlock(1, "table", props, "", true))));
        Assert.assertEquals(QueryParser.parse("{{ table: }}", VariableTag.BRACE, props),
                new ParsedQuery(Arrays.asList("", ""),
                        Arrays.asList(new ExecutableBlock(1, "table", props, "", true))));
        Assert.assertEquals(QueryParser.parse("{{ table() }}", VariableTag.BRACE, props),
                new ParsedQuery(Arrays.asList("", ""),
                        Arrays.asList(new ExecutableBlock(1, "table", props, "()", true))));
        Assert.assertEquals(QueryParser.parse("{{ table. web() }}", VariableTag.BRACE, props),
                new ParsedQuery(Arrays.asList("", ""),
                        Arrays.asList(new ExecutableBlock(1, "table", props, "web()", true))));
        Assert.assertEquals(QueryParser.parse("select {{ table.db.db1: select 1 }}", VariableTag.BRACE, props),
                new ParsedQuery(Arrays.asList("select ", ""),
                        Arrays.asList(new ExecutableBlock(1, "table", props, "db.db1: select 1", true))));
    }

    @Test(groups = { "unit" })
    public void testPrePostQueries() {
        Properties props = new Properties();
        String str;

        Assert.assertEquals(QueryParser.parse(str = "{{web(pre.query=,post.query=)}}", VariableTag.BRACE, props),
                new ParsedQuery(Arrays.asList("", ""), Arrays.asList(new ExecutableBlock(1, "web", props, "", true))));
        Assert.assertEquals(QueryParser.parse(str = "{{web(pre.query='',post.query=\"\")}}", VariableTag.BRACE, props),
                new ParsedQuery(Arrays.asList("", ""), Arrays.asList(new ExecutableBlock(1, "web", props, "", true))));
        Assert.assertEquals(
                QueryParser.parse(str = "{{web(pre.query=\"db: select 1\",post.query=\"\")}}", VariableTag.BRACE,
                        props),
                new ParsedQuery(Arrays.asList("", "", ""),
                        Arrays.asList(new ExecutableBlock(1, "db", props, "select 1", false),
                                new ExecutableBlock(2, "web", props, "", true))));
        Assert.assertEquals(
                QueryParser.parse(str = "{{web(pre.query=\"db: select 1\",post.query='shell: select 2')}}",
                        VariableTag.BRACE, props),
                new ParsedQuery(Arrays.asList("", "", "", ""),
                        Arrays.asList(new ExecutableBlock(1, "db", props, "select 1", false),
                                new ExecutableBlock(2, "web", props, "", true),
                                new ExecutableBlock(3, "shell", props, "select 2", false))));
    }

    @Test(groups = { "unit" })
    public void testDifferentVariableTags() {
        Properties props = new Properties();
        Assert.assertEquals(QueryParser.parse("{{script}}", null, props), new ParsedQuery(
                Arrays.asList("", ""), Collections.singletonList(new ExecutableBlock(1, "script", props, "", true))));
        Assert.assertEquals(QueryParser.parse("<<script>>", VariableTag.ANGLE_BRACKET, props), new ParsedQuery(
                Arrays.asList("", ""), Collections.singletonList(new ExecutableBlock(1, "script", props, "", true))));
        Assert.assertEquals(QueryParser.parse("[[script]]", VariableTag.SQUARE_BRACKET, props), new ParsedQuery(
                Arrays.asList("", ""), Collections.singletonList(new ExecutableBlock(1, "script", props, "", true))));

        Assert.assertEquals(QueryParser.parse("{%script%}", null, props), new ParsedQuery(
                Arrays.asList("", ""), Collections.singletonList(new ExecutableBlock(1, "script", props, "", false))));
        Assert.assertEquals(QueryParser.parse("<%script%>", VariableTag.ANGLE_BRACKET, props), new ParsedQuery(
                Arrays.asList("", ""), Collections.singletonList(new ExecutableBlock(1, "script", props, "", false))));
        Assert.assertEquals(QueryParser.parse("[%script%]", VariableTag.SQUARE_BRACKET, props), new ParsedQuery(
                Arrays.asList("", ""), Collections.singletonList(new ExecutableBlock(1, "script", props, "", false))));
    }

    @Test(groups = { "unit" })
    public void testStatements() {
        ParsedQuery query = QueryParser.parse("select 1\n  -- {%context: a=1,b=2%}\n ; select 2", VariableTag.BRACE,
                null);
        Assert.assertNotNull(query);
    }
}
