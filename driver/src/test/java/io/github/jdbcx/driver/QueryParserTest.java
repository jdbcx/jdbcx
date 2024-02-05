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
package io.github.jdbcx.driver;

import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.github.jdbcx.Constants;
import io.github.jdbcx.Option;

public class QueryParserTest {
    @Test(groups = { "unit" })
    public void testExtractQuotedString() {
        Assert.assertEquals(QueryParser.extractQuotedPart("\0", 0, 1, null, '\0'), QueryParser.newPart(1, ""));

        Properties props = new Properties();
        Assert.assertThrows(IllegalArgumentException.class,
                () -> QueryParser.extractQuotedPart("'", 2, 1, props, '\''));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> QueryParser.extractQuotedPart("' ", 1, 2, props, '\''));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> QueryParser.extractQuotedPart("'", 1, 1, props, '\''));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> QueryParser.extractQuotedPart("`123", 1, 4, props, '`'));

        String str;
        Assert.assertEquals(QueryParser.extractQuotedPart(str = "'", 0, 1, props, '\''), QueryParser.newPart(1, ""));
        Assert.assertEquals(QueryParser.extractQuotedPart(str = " '' \\''", 1, str.length(), props, '\''),
                QueryParser.newPart(7, "' '"));
        Assert.assertEquals(QueryParser.extractQuotedPart(str = "a = \"1'2`3\\\"\",", 5, str.length(), props, '"'),
                QueryParser.newPart(str.length() - 1, "1'2`3\""));

        props.setProperty("secret", "***");
        Assert.assertEquals(QueryParser.extractQuotedPart(str = " ${secret}' ", 0, str.length(), props, '\''),
                QueryParser.newPart(str.length() - 1, " ***"));
    }

    @Test(groups = { "unit" })
    public void testExtractPartBefore() {
        Assert.assertEquals(QueryParser.extractPartBefore("", 0, 0, null), QueryParser.newPart(0, ""));

        Properties props = new Properties();
        String str;
        Assert.assertEquals(QueryParser.extractPartBefore(str = "", 0, str.length(), props),
                QueryParser.newPart(str.length(), ""));
        Assert.assertEquals(QueryParser.extractPartBefore(str = "abc", 0, str.length(), props),
                QueryParser.newPart(str.length(), "abc"));
        Assert.assertEquals(QueryParser.extractPartBefore(str = "abc", 1, str.length(), props),
                QueryParser.newPart(str.length(), "bc"));
        Assert.assertEquals(QueryParser.extractPartBefore(str = "abc", 1, str.length(), props, 'b'),
                QueryParser.newPart(str.length() - 2, ""));
        Assert.assertEquals(QueryParser.extractPartBefore(str = "abc", 1, str.length(), props, 'c'),
                QueryParser.newPart(str.length() - 1, "b"));

        Assert.assertEquals(QueryParser.extractPartBefore(str = "a=b,c=1", 2, str.length(), props, ','),
                QueryParser.newPart(3, "b"));
        Assert.assertEquals(QueryParser.extractPartBefore(str = "a=b ) c=1", 2, str.length(), props, ',', ')'),
                QueryParser.newPart(4, "b"));

        props.setProperty("secret", "321");
        props.setProperty("magic", "123");
        Assert.assertEquals(
                QueryParser.extractPartBefore(str = " ${secret} ${magic} ) c=1", 1, str.length(), props, ',', ')'),
                QueryParser.newPart(20, "321 123"));
    }

    @Test(groups = { "unit" })
    public void testExtractPropertyName() {
        Assert.assertEquals(QueryParser.extractPropertyNamePart("a =", 0, 3, null), QueryParser.newPart(1, "a"));

        Properties props = new Properties();
        Assert.assertThrows(IllegalArgumentException.class, () -> QueryParser.extractPropertyNamePart("", 0, 0, props));

        String str;
        Assert.assertEquals(QueryParser.extractPropertyNamePart(str = "a1=", 0, str.length(), props),
                QueryParser.newPart(2, "a1"));
        Assert.assertEquals(QueryParser.extractPropertyNamePart(str = " a = ", 0, str.length(), props),
                QueryParser.newPart(2, "a"));

        props.setProperty("x", "y");
        Assert.assertEquals(QueryParser.extractPropertyNamePart(str = " ${x}a = ", 0, str.length(), props),
                QueryParser.newPart(6, "ya"));
    }

    @Test(groups = { "unit" })
    public void testExtractPropertyValue() {
        Assert.assertEquals(QueryParser.extractPropertyValuePart("", 0, 0, null), QueryParser.newPart(0, ""));

        Properties props = new Properties();
        Assert.assertThrows(IllegalArgumentException.class,
                () -> QueryParser.extractPropertyValuePart("'", 0, 1, props));
        Assert.assertEquals(QueryParser.extractPropertyValuePart("", 0, 0, props), QueryParser.newPart(0, ""));
        Assert.assertEquals(QueryParser.extractPropertyValuePart("'", 0, 0, props), QueryParser.newPart(0, ""));

        String str;
        Assert.assertEquals(QueryParser.extractPropertyValuePart(str = "a='x\\'1'''", 2, str.length(), props),
                QueryParser.newPart(str.length(), "x'1'"));
        Assert.assertEquals(QueryParser.extractPropertyValuePart(str = "a = 123", 3, str.length(), props),
                QueryParser.newPart(str.length(), "123"));

        props.setProperty("magic", "***");
        Assert.assertEquals(QueryParser.extractPropertyValuePart(str = "a = ${magic}", 3, str.length(), props),
                QueryParser.newPart(str.length(), "***"));
    }

    @Test(groups = { "unit" })
    public void testExtractProperties() {
        Assert.assertThrows(NullPointerException.class, () -> QueryParser.extractProperties("a=1", 0, 3, null));

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
                        str.length(), props),
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
        Assert.assertThrows(NullPointerException.class, () -> QueryParser.parseExecutableBlock("a(b=1)", null));

        Properties props = new Properties();
        Properties expected = new Properties();

        String str;
        Assert.assertEquals(QueryParser.parseExecutableBlock(str = "", props), new String[] { "", "" });
        Assert.assertEquals(props, expected);

        Assert.assertEquals(QueryParser.parseExecutableBlock(str = "script", props), new String[] { "script", "" });
        Assert.assertEquals(props, expected);
        Assert.assertEquals(QueryParser.parseExecutableBlock(str = "select 1", props), new String[] { "", "select 1" });
        Assert.assertEquals(props, expected);

        Assert.assertEquals(QueryParser.parseExecutableBlock(str = "prql()", props), new String[] { "prql", "" });
        Assert.assertEquals(QueryParser.parseExecutableBlock(str = "prql ( ) ", props), new String[] { "prql", "" });
        Assert.assertEquals(QueryParser.parseExecutableBlock(str = "prql ( ) : ", props), new String[] { "prql", "" });
        Assert.assertEquals(QueryParser.parseExecutableBlock(str = "prql():", props), new String[] { "prql", "" });

        expected.setProperty("custom.classpath", "/usr/local/lib");
        expected.setProperty("cli.timeout", "1500");
        expected.setProperty("cli.path", "~/.cargo/bin/prqlc");
        Assert.assertEquals(QueryParser.parseExecutableBlock(
                str = "prql(custom.classpath=/usr/local/lib,cli.timeout=1500, cli.path='~/.cargo/bin/prqlc')", props),
                new String[] { "prql", "" });
        Assert.assertEquals(props, expected);

        props.clear();
        Assert.assertEquals(QueryParser.parseExecutableBlock(
                str = "prql(custom.classpath=/usr/local/lib,cli.timeout=1500, cli.path='~/.cargo/bin/prqlc'): from `test.test1` | take 10",
                props), new String[] { "prql", "from `test.test1` | take 10" });
        Assert.assertEquals(props, expected);

        props.clear();
        Assert.assertEquals(QueryParser.parseExecutableBlock(
                str = " prql\r\n\t(\ncustom.classpath = /usr/local/lib, cli.timeout =1500,\r\ncli.path= '~/.cargo/bin/prqlc' ) :\nfrom `test.test1` | take 10 ",
                props), new String[] { "prql", "from `test.test1` | take 10 " });
        Assert.assertEquals(props, expected);

        props.clear();
        props.setProperty("my.option", "trim");
        props.setProperty("my.preference", Constants.TRUE_EXPR);
        expected.clear();
        expected.putAll(props);
        expected.setProperty("result.string.trim", Constants.TRUE_EXPR);
        Assert.assertEquals(QueryParser.parseExecutableBlock(
                str = " shell\r\n\t(\nresult.string.${my.option} = '${my.preference}' ) :\nfrom `test.test1` | take 10 ",
                props), new String[] { "shell", "from `test.test1` | take 10 " });
        Assert.assertEquals(props, expected);
    }

    @Test(groups = { "unit" })
    public void testParse() {
        Properties props = new Properties();
        String str;
        Assert.assertEquals(QueryParser.parse(str = "", props),
                new ParsedQuery(Collections.emptyList(), Collections.emptyList()));
        Assert.assertEquals(QueryParser.parse(str = "select 1", props),
                new ParsedQuery(Arrays.asList(str), Collections.emptyList()));

        Assert.assertEquals(QueryParser.parse("{%var%}", props), new ParsedQuery(Arrays.asList("", ""),
                Arrays.asList(new ExecutableBlock(1, "var", new Properties(), "", false))));
        Assert.assertEquals(QueryParser.parse("{%var:%}", props), new ParsedQuery(Arrays.asList("", ""),
                Arrays.asList(new ExecutableBlock(1, "var", new Properties(), "", false))));
        Assert.assertEquals(QueryParser.parse("{% var: %}", props), new ParsedQuery(Arrays.asList("", ""),
                Arrays.asList(new ExecutableBlock(1, "var", new Properties(), "", false))));
        Assert.assertEquals(QueryParser.parse("{%var: %}", props), new ParsedQuery(Arrays.asList("", ""),
                Arrays.asList(new ExecutableBlock(1, "var", new Properties(), "", false))));
        Assert.assertEquals(QueryParser.parse("{% var %}", props), new ParsedQuery(Arrays.asList("", ""),
                Arrays.asList(new ExecutableBlock(1, "var", new Properties(), "", false))));
        Assert.assertEquals(QueryParser.parse("{% var: %}", props), new ParsedQuery(Arrays.asList("", ""),
                Arrays.asList(new ExecutableBlock(1, "var", new Properties(), "", false))));
        Assert.assertEquals(QueryParser.parse("{% var: a=%%%}", props), new ParsedQuery(Arrays.asList("", ""),
                Arrays.asList(new ExecutableBlock(1, "var", new Properties(), "a=%%", false))));

        Assert.assertEquals(QueryParser.parse("{{web}}", props), new ParsedQuery(Arrays.asList("", ""),
                Arrays.asList(new ExecutableBlock(1, "web", new Properties(), "", true))));
        Assert.assertEquals(QueryParser.parse("{{web:}}", props), new ParsedQuery(Arrays.asList("", ""),
                Arrays.asList(new ExecutableBlock(1, "web", new Properties(), "", true))));
        Assert.assertEquals(QueryParser.parse("{{ web:}}", props), new ParsedQuery(Arrays.asList("", ""),
                Arrays.asList(new ExecutableBlock(1, "web", new Properties(), "", true))));
        Assert.assertEquals(QueryParser.parse("{{web: }}", props), new ParsedQuery(Arrays.asList("", ""),
                Arrays.asList(new ExecutableBlock(1, "web", new Properties(), "", true))));
        Assert.assertEquals(QueryParser.parse("{{ web }}", props), new ParsedQuery(Arrays.asList("", ""),
                Arrays.asList(new ExecutableBlock(1, "web", new Properties(), "", true))));
        Assert.assertEquals(QueryParser.parse("{{ web: }}", props), new ParsedQuery(Arrays.asList("", ""),
                Arrays.asList(new ExecutableBlock(1, "web", new Properties(), "", true))));

        Assert.assertEquals(QueryParser.parse("{{script:1+1}}", props), new ParsedQuery(Arrays.asList("", ""),
                Arrays.asList(new ExecutableBlock(1, "script", new Properties(), "1+1", true))));
        Assert.assertEquals(QueryParser.parse("{{ script: 1+1 }}", props), new ParsedQuery(Arrays.asList("", ""),
                Arrays.asList(new ExecutableBlock(1, "script", new Properties(), "1+1", true))));
        Assert.assertEquals(QueryParser.parse("{{ script: {1+1}}}", props), new ParsedQuery(Arrays.asList("", ""),
                Arrays.asList(new ExecutableBlock(1, "script", new Properties(), "{1+1}", true))));
        Assert.assertEquals(QueryParser.parse("{{ script: {1+1}\n}}", props), new ParsedQuery(Arrays.asList("", ""),
                Arrays.asList(new ExecutableBlock(1, "script", new Properties(), "{1+1}", true))));

        Assert.assertEquals(
                QueryParser.parse(
                        str = "helper.format('select now() t, arrayJoin(splitByChar('\\n', '%s')) a\", \"a\\nb\\nc\")",
                        props),
                new ParsedQuery(Arrays.asList(str), Collections.emptyList()));

        Assert.assertEquals(QueryParser.parse(str = "select\\{%} {\\{{ 1{", props),
                new ParsedQuery(Arrays.asList("select{%} {\\{{ 1{"), Collections.emptyList()));
        Assert.assertEquals(QueryParser.parse(str = "select {%-no interested%}{{-same here}}", props),
                new ParsedQuery(Arrays.asList("select "), Collections.emptyList()));

        Properties props1 = new Properties();
        props1.setProperty("a", "1");
        Properties props2 = new Properties();
        props2.setProperty("b", "2");
        // without body
        Assert.assertEquals(QueryParser.parse(str = "select {%proc(a=1)%}{{func(b=2)}}", props),
                new ParsedQuery(Arrays.asList("select ", "", "", ""),
                        Arrays.asList(new ExecutableBlock(1, "proc", props1, "", false),
                                new ExecutableBlock(3, "func", props2, "", true))));

        // full expression
        Assert.assertEquals(QueryParser.parse(str = "select {%proc(a=1):execute()%}{{func(b=2):return x;}}", props),
                new ParsedQuery(Arrays.asList("select ", "", "", ""),
                        Arrays.asList(new ExecutableBlock(1, "proc", props1, "execute()", false),
                                new ExecutableBlock(3, "func", props2, "return x;", true))));

        Assert.assertEquals(QueryParser.parse(str = "select{%2%} {{{1}}}\\\\{{\\}}", props),
                new ParsedQuery(Arrays.asList("select", "", " ", "", "\\\\{{\\}}"),
                        Arrays.asList(new ExecutableBlock(1, "", new Properties(), "2", false),
                                new ExecutableBlock(3, "", new Properties(), "{1}", true))));
        Assert.assertEquals(
                QueryParser
                        .parse(str = "select {{ script: helper.format('%s', '*')}} from {% shell: echo 'mytable' %}",
                                props),
                new ParsedQuery(Arrays.asList("select ", "", " from ", ""),
                        Arrays.asList(
                                new ExecutableBlock(1, "script", new Properties(), "helper.format('%s', '*')", true),
                                new ExecutableBlock(3, "shell", new Properties(), "echo 'mytable'", false))));
    }

    @Test(groups = { "unit" })
    public void testParseDotNotation() {
        Properties props = new Properties();
        String str;

        Assert.assertThrows(IllegalArgumentException.class, () -> QueryParser.parse("{{web.{}}", props));

        Assert.assertEquals(QueryParser.parse(str = "{{web}}", props),
                new ParsedQuery(Arrays.asList("", ""), Arrays.asList(new ExecutableBlock(1, "web", props, "", true))));
        Assert.assertEquals(QueryParser.parse(str = "{{web.}}", props),
                new ParsedQuery(Arrays.asList("", ""), Arrays.asList(new ExecutableBlock(1, "web", props, "", true))));
        Assert.assertEquals(QueryParser.parse(str = "{{ web. }}", props),
                new ParsedQuery(Arrays.asList("", ""), Arrays.asList(new ExecutableBlock(1, "web", props, "", true))));

        Assert.assertEquals(QueryParser.parse(str = "{{web.()}}", props),
                new ParsedQuery(Arrays.asList("", ""), Arrays.asList(new ExecutableBlock(1, "web", props, "", true))));
        Assert.assertEquals(QueryParser.parse(str = "{{web.:}}", props),
                new ParsedQuery(Arrays.asList("", ""), Arrays.asList(new ExecutableBlock(1, "web", props, "", true))));
        Assert.assertEquals(QueryParser.parse(str = "{{web.():}}", props),
                new ParsedQuery(Arrays.asList("", ""), Arrays.asList(new ExecutableBlock(1, "web", props, "", true))));
        Assert.assertEquals(QueryParser.parse(str = "{{  web. (   ) :  }}", props),
                new ParsedQuery(Arrays.asList("", ""), Arrays.asList(new ExecutableBlock(1, "web", props, "", true))));
        Assert.assertEquals(QueryParser.parse(str = "{{  web. (   ) :  request body }}", props),
                new ParsedQuery(Arrays.asList("", ""),
                        Arrays.asList(new ExecutableBlock(1, "web", props, "request body", true))));

        Properties expectedProps = new Properties();
        Option.ID.setValue(expectedProps, "ch-dev");
        Assert.assertEquals(QueryParser.parse(str = "{{ web. ( id = 'ch-dev' ) }}", props),
                new ParsedQuery(Arrays.asList("", ""),
                        Arrays.asList(new ExecutableBlock(1, "web", expectedProps, "", true))));
        Assert.assertEquals(QueryParser.parse(str = "{{ web. (id = ch-dev): request }}", props),
                new ParsedQuery(Arrays.asList("", ""),
                        Arrays.asList(new ExecutableBlock(1, "web", expectedProps, "request", true))));
        Assert.assertEquals(QueryParser.parse(str = "{{ web.ch-dev }}", props),
                new ParsedQuery(Arrays.asList("", ""),
                        Arrays.asList(new ExecutableBlock(1, "web", expectedProps, "", true))));
        Assert.assertEquals(QueryParser.parse(str = "{{ web.ch-dev: my request }}", props),
                new ParsedQuery(Arrays.asList("", ""),
                        Arrays.asList(new ExecutableBlock(1, "web", expectedProps, "my request", true))));
        Assert.assertEquals(QueryParser.parse(str = "{{ web.ch-dev1 (id=ch-dev): my request }}", props),
                new ParsedQuery(Arrays.asList("", ""),
                        Arrays.asList(new ExecutableBlock(1, "web", expectedProps, "my request", true))));

        Option.ID.setValue(expectedProps, "$");
        Assert.assertEquals(QueryParser.parse(str = "{{ web.$: my request }}", props),
                new ParsedQuery(Arrays.asList("", ""),
                        Arrays.asList(new ExecutableBlock(1, "web", expectedProps, "my request", true))));
        Option.ID.setValue(expectedProps, "${}");
        Assert.assertEquals(QueryParser.parse(str = "{{ web.${}}}", props),
                new ParsedQuery(Arrays.asList("", ""),
                        Arrays.asList(new ExecutableBlock(1, "web", expectedProps, "", true))));
        Option.ID.setValue(expectedProps, "${x}");
        Assert.assertEquals(QueryParser.parse(str = "{{ web.${x}}}", props),
                new ParsedQuery(Arrays.asList("", ""),
                        Arrays.asList(new ExecutableBlock(1, "web", expectedProps, "", true))));
        Option.ID.setValue(expectedProps, "${x}x");
        Assert.assertEquals(QueryParser.parse(str = "{{ web.${x}x}}", props),
                new ParsedQuery(Arrays.asList("", ""),
                        Arrays.asList(new ExecutableBlock(1, "web", expectedProps, "", true))));
        Assert.assertEquals(QueryParser.parse(str = "{{ web.${x}x }}", props),
                new ParsedQuery(Arrays.asList("", ""),
                        Arrays.asList(new ExecutableBlock(1, "web", expectedProps, "", true))));

        Option.ID.setValue(expectedProps, "${x}");
        Assert.assertEquals(QueryParser.parse(str = "{% var: x=ch-dev %}{{ web.${x}: my request }}", props),
                new ParsedQuery(Arrays.asList("", "", "", ""),
                        Arrays.asList(new ExecutableBlock(1, "var", props, "x=ch-dev", false),
                                new ExecutableBlock(3, "web", expectedProps, "my request", true))));
    }

    @Test(groups = { "unit" })
    public void testPrePostQueries() {
        Properties props = new Properties();
        String str;

        Assert.assertEquals(QueryParser.parse(str = "{{web(pre.query=,post.query=)}}", props),
                new ParsedQuery(Arrays.asList("", ""), Arrays.asList(new ExecutableBlock(1, "web", props, "", true))));
        Assert.assertEquals(QueryParser.parse(str = "{{web(pre.query='',post.query=\"\")}}", props),
                new ParsedQuery(Arrays.asList("", ""), Arrays.asList(new ExecutableBlock(1, "web", props, "", true))));
        Assert.assertEquals(QueryParser.parse(str = "{{web(pre.query=\"db: select 1\",post.query=\"\")}}", props),
                new ParsedQuery(Arrays.asList("", "", ""),
                        Arrays.asList(new ExecutableBlock(1, "db", props, "select 1", false),
                                new ExecutableBlock(2, "web", props, "", true))));
        Assert.assertEquals(
                QueryParser.parse(str = "{{web(pre.query=\"db: select 1\",post.query='shell: select 2')}}", props),
                new ParsedQuery(Arrays.asList("", "", "", ""),
                        Arrays.asList(new ExecutableBlock(1, "db", props, "select 1", false),
                                new ExecutableBlock(2, "web", props, "", true),
                                new ExecutableBlock(3, "shell", props, "select 2", false))));
    }
}
