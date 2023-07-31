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
package io.github.jdbcx.interpreter;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletionException;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.github.jdbcx.BaseIntegrationTest;
import io.github.jdbcx.Option;
import io.github.jdbcx.QueryContext;

public class WebInterpreterTest extends BaseIntegrationTest {
    @Test(groups = { "unit" })
    public void testConstructor() throws IOException {
        WebInterpreter interpreter = null;
        Assert.assertEquals(interpreter = new WebInterpreter(null, null), interpreter);
        Assert.assertEquals(interpreter.getDefaultUrl(), WebInterpreter.OPTION_URL_TEMPLATE.getDefaultValue());
        Assert.assertEquals(interpreter.getDefaultRequestHeaders(), Collections.emptyMap());
        Assert.assertEquals(interpreter.getDefaultRequestTemplate(), "");

        QueryContext context = QueryContext.newContext();
        Properties config = new Properties();
        Assert.assertEquals(interpreter = new WebInterpreter(context, config), interpreter);
        Assert.assertEquals(interpreter.getDefaultUrl(), WebInterpreter.OPTION_URL_TEMPLATE.getDefaultValue());
        Assert.assertEquals(interpreter.getDefaultRequestHeaders(), Collections.emptyMap());
        Assert.assertEquals(interpreter.getDefaultRequestTemplate(), "");

        WebInterpreter.OPTION_BASE_URL.setValue(config, "my base url");
        Assert.assertEquals(interpreter = new WebInterpreter(context, config), interpreter);
        Assert.assertEquals(interpreter.getDefaultUrl(), "my base url");

        WebInterpreter.OPTION_URL_TEMPLATE.setValue(config, "your base url");
        Assert.assertEquals(interpreter = new WebInterpreter(context, config), interpreter);
        Assert.assertEquals(interpreter.getDefaultUrl(), "your base url");

        WebInterpreter.OPTION_URL_TEMPLATE.setValue(config, "${your} url ${template}");
        Assert.assertEquals(interpreter = new WebInterpreter(context, config), interpreter);
        Assert.assertEquals(interpreter.getDefaultUrl(), "${your} url ${template}");

        config.setProperty("template", "is this");
        Assert.assertEquals(interpreter = new WebInterpreter(context, config), interpreter);
        Assert.assertEquals(interpreter.getDefaultUrl(), "${your} url is this");

        config.setProperty("your", "my");
        Assert.assertEquals(interpreter = new WebInterpreter(context, config), interpreter);
        Assert.assertEquals(interpreter.getDefaultUrl(), "my url is this");
        Assert.assertEquals(interpreter.getDefaultRequestHeaders(), Collections.emptyMap());
        Assert.assertEquals(interpreter.getDefaultRequestTemplate(), "");

        Map<String, String> headers = new HashMap<>();
        headers.put("API-Key", "Bear 1 2 3");
        headers.put("Org-Key", "orgid");
        config.setProperty("secret.number", "2");
        WebInterpreter.OPTION_REQUEST_HEADERS.setValue(config, "API-Key=Bear 1 ${secret.number} 3, Org-Key=orgid");
        Assert.assertEquals(interpreter = new WebInterpreter(context, config), interpreter);
        Assert.assertEquals(interpreter.getDefaultUrl(), "my url is this");
        Assert.assertEquals(interpreter.getDefaultRequestHeaders(), headers);
        Assert.assertEquals(interpreter.getDefaultRequestTemplate(), "");

        WebInterpreter.OPTION_REQUEST_TEMPLATE_FILE.setValue(config, "target/test-classes/non-existent.file");
        Assert.assertThrows(IllegalArgumentException.class, () -> new WebInterpreter(context, config));

        String classFile = "target/classes/" + WebInterpreter.class.getName().replace('.', '/') + ".class";
        WebInterpreter.OPTION_REQUEST_TEMPLATE_FILE.setValue(config, classFile);
        Assert.assertEquals(interpreter = new WebInterpreter(context, config), interpreter);
        Assert.assertEquals(interpreter.getDefaultUrl(), "my url is this");
        Assert.assertEquals(interpreter.getDefaultRequestHeaders(), headers);
        Assert.assertTrue(interpreter.getDefaultRequestTemplate().length() > 0);

        WebInterpreter.OPTION_REQUEST_TEMPLATE.setValue(config, "321");
        Assert.assertEquals(interpreter = new WebInterpreter(context, config), interpreter);
        Assert.assertEquals(interpreter.getDefaultUrl(), "my url is this");
        Assert.assertEquals(interpreter.getDefaultRequestHeaders(), headers);
        Assert.assertEquals(interpreter.getDefaultRequestTemplate(), "321");
    }

    @Test(groups = { "integration" })
    public void testInterpret() throws IOException {
        QueryContext context = QueryContext.newContext();
        Properties config = new Properties();

        WebInterpreter interpreter = new WebInterpreter(context, config);
        Assert.assertEquals(interpreter.getDefaultUrl(), WebInterpreter.OPTION_URL_TEMPLATE.getDefaultValue());

        Assert.assertThrows(CompletionException.class, () -> new WebInterpreter(context, config).interpret(null, null));

        Properties props = new Properties();
        Assert.assertThrows(CompletionException.class,
                () -> new WebInterpreter(context, config).interpret("select 1", props));
        WebInterpreter.OPTION_BASE_URL.setValue(props, "http://" + getClickHouseServer());
        Assert.assertEquals(interpreter.interpret("select 1", props).get(String.class), "1\n");

        WebInterpreter.OPTION_URL_TEMPLATE.setValue(props, "${base.url}?query=${query}");
        props.setProperty("query", "select+2");
        Assert.assertEquals(interpreter.interpret(null, props).get(String.class), "2\n");

        WebInterpreter.OPTION_BASE_URL.setValue(config, "https://bing.com");
        props.setProperty("query", "");
        Assert.assertEquals(interpreter.interpret("select 3", props).get(String.class), "3\n");
    }

    @Test(groups = { "private" })
    public void testGraphQL() throws IOException {
        QueryContext context = QueryContext.newContext();
        Properties config = new Properties();
        WebInterpreter.OPTION_BASE_URL.setValue(config, "https://countries.trevorblades.com");
        WebInterpreter.OPTION_REQUEST_HEADERS.setValue(config, "Content-Type=application/json; charset=utf-8");

        WebInterpreter interpreter = new WebInterpreter(context, config);
        Properties props = new Properties();
        Option.RESULT_JSON_PATH.setValue(props, "data.country.states[?code == '川'].name");
        String message = interpreter.interpret("{\"query\":\"query {\\n" + //
                "  country(code: \\\"CN\\\") {\\n" + //
                "    code\\n" + //
                "    emoji\\n" + //
                "    phone\\n" + //
                "    awsRegion\\n" + //
                "    states {\\n" + //
                "      code\\n" + //
                "      name\\n" + //
                "    }\\n" + //
                "  }\\n" + //
                "}\\n" + //
                "\"}", props).get(String.class);
        Assert.assertEquals(message, "四川");
    }
}
