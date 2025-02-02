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
package io.github.jdbcx.extension;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.Locale;
import java.util.Properties;
import java.util.Map.Entry;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.github.jdbcx.Compression;
import io.github.jdbcx.Format;
import io.github.jdbcx.JdbcDialect;
import io.github.jdbcx.Option;
import io.github.jdbcx.QueryContext;
import io.github.jdbcx.QueryMode;
import io.github.jdbcx.ResultMapper;
import io.github.jdbcx.VariableTag;
import io.github.jdbcx.interpreter.WebInterpreter;

public class BridgeDriverExtensionTest {
    static class TestJdbcDialect implements JdbcDialect {
        @Override
        public ResultMapper getMapper() {
            return null;
        }
    }

    @Test(groups = { "unit" })
    public void testBuild() {
        Properties props = new Properties();
        props.setProperty("_secret1_", "xxx");
        props.setProperty("_secret2_", "***");

        try (QueryContext context = QueryContext.newContext()) {
            Assert.assertThrows(NullPointerException.class, () -> BridgeDriverExtension.build(context, props));
            context.put(QueryContext.KEY_BRIDGE, new Properties());
            Properties newProps = BridgeDriverExtension.build(context, props);
            for (Entry<Object, Object> e : props.entrySet()) {
                Assert.assertEquals(newProps.getProperty((String) e.getKey()), e.getValue());
            }
            Assert.assertEquals(WebInterpreter.OPTION_URL_TEMPLATE.getValue(newProps), "");
            Assert.assertEquals(WebInterpreter.OPTION_AUTH_BEARER_TOKEN.getValue(newProps), "");
            Assert.assertEquals(WebInterpreter.OPTION_REQUEST_HEADERS.getValue(newProps),
                    "User-Agent=JDBCX,accept=text/csv,accept-encoding=identity");

            Properties bridgeCtx = new Properties();
            context.put(QueryContext.KEY_BRIDGE, bridgeCtx);
            context.put(QueryContext.KEY_DIALECT, new TestJdbcDialect());
            BridgeDriverExtension.OPTION_URL.setValue(props, "http://my.server:9090/bridge/");
            BridgeDriverExtension.OPTION_QUERY_MODE.setValue(props, QueryMode.MUTATION.name().toLowerCase(Locale.ROOT));
            BridgeDriverExtension.OPTION_FORMAT.setValue(props, Format.ARROW_STREAM.fileExtension(false));
            BridgeDriverExtension.OPTION_COMPRESSION.setValue(props, Compression.BROTLI.fileExtension(false));
            BridgeDriverExtension.OPTION_TOKEN.setValue(props, "321321123123");
            Option.TAG.setValue(props, VariableTag.SQUARE_BRACKET.name());
            bridgeCtx.setProperty("product", "MyDatabase/0.1");
            bridgeCtx.setProperty("user", "me");
            newProps = BridgeDriverExtension.build(context, props);
            for (Entry<Object, Object> e : props.entrySet()) {
                Assert.assertEquals(newProps.getProperty((String) e.getKey()), e.getValue());
            }
            Assert.assertEquals(WebInterpreter.OPTION_URL_TEMPLATE.getValue(newProps), "http://my.server:9090/bridge/");
            Assert.assertEquals(WebInterpreter.OPTION_AUTH_BEARER_TOKEN.getValue(newProps), "321321123123");
            Assert.assertEquals(WebInterpreter.OPTION_REQUEST_HEADERS.getValue(newProps),
                    "User-Agent=MyDatabase/0.1,x-query-user=me,x-query-mode=m,accept=application/vnd.apache.arrow.stream;text/csv,accept-encoding=br;identity");
        }
    }

    @Test(groups = { "unit" })
    public void testCreateListener() throws SQLException {
        Properties props = new Properties();
        props.setProperty("_secret1_", "xxx");
        props.setProperty("_secret2_", "***");
        props.setProperty("exec.error", "warn");
        try (Connection conn = DriverManager.getConnection("jdbcx:db:sqlite::memory:", props);
                QueryContext context = QueryContext.newContext()) {
            BridgeDriverExtension ext = new BridgeDriverExtension();
            context.put(QueryContext.KEY_BRIDGE, new Properties());
            Properties newProps = BridgeDriverExtension.build(context, props);
            for (Entry<Object, Object> e : props.entrySet()) {
                Assert.assertEquals(newProps.getProperty((String) e.getKey()), e.getValue());
            }

            Assert.assertEquals(newProps.getProperty("product"), null);
            Assert.assertThrows(SQLWarning.class, () -> ext.createListener(context, conn, props).onQuery("select 1"));

            props.setProperty("url", "http://localhost:8080/");
            Assert.assertThrows(SQLException.class, () -> ext.createListener(context, conn, props).onQuery("select 1"));
        }
    }

    @Test(groups = { "unit" })
    public void testQueryRewrite() throws SQLException {
        try (Connection conn = DriverManager.getConnection("jdbcx:db:sqlite::memory:");
                QueryContext context = QueryContext.newContext()) {
            BridgeDriverExtension ext = new BridgeDriverExtension();
            context.put(QueryContext.KEY_BRIDGE, new Properties());
            BridgeDriverExtension.ActivityListener listener = ext.createListener(context, conn, new Properties());

            Assert.assertEquals(listener.rewrite(null), null);
            Assert.assertEquals(listener.rewrite(""), "");
            Assert.assertEquals(listener.rewrite("select 1"), "select 1");

            Assert.assertEquals(listener.rewrite("\\select 1\\"), "\\select 1\\");
            Assert.assertEquals(listener.rewrite("\\\\select 1\\\\"), "\\select 1\\");

            Assert.assertEquals(listener.rewrite("{{select 1}}"), "{{select 1}}");
            Assert.assertEquals(listener.rewrite("{{select 1\\}}"), "{{select 1}}");
            Assert.assertEquals(listener.rewrite("{{select 1}\\}"), "{{select 1}}");
            Assert.assertEquals(listener.rewrite("{\\{select 1}}"), "{\\{select 1}}");
            Assert.assertEquals(listener.rewrite("{\\{selec\\t} 1}}"), "{\\{selec\\t} 1}}");
        }
    }
}
