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

import java.util.Properties;
import java.util.concurrent.CompletionException;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.github.jdbcx.BaseIntegrationTest;
import io.github.jdbcx.QueryContext;
import io.github.jdbcx.Row;
import io.github.jdbcx.executor.McpExecutor;

public class McpInterpreterTest extends BaseIntegrationTest {
    @Test(groups = { "unit" })
    public void testConstructor() {
        McpInterpreter interpreter = null;
        Assert.assertEquals(interpreter = new McpInterpreter(null, null), interpreter);
    }

    @Test(groups = { "integration" })
    public void testInterpret() {
        skipTestsIfJdkIsOlderThan(17);
        
        QueryContext context = QueryContext.newContext();
        Properties config = new Properties();

        McpInterpreter interpreter = new McpInterpreter(context, config);

        Assert.assertThrows(CompletionException.class, () -> interpreter.interpret(null, null));

        Properties props = new Properties();
        Assert.assertThrows(CompletionException.class, () -> interpreter.interpret("select 1", props));
        McpExecutor.OPTION_SERVER_CMD.setValue(props, "npx");
        McpExecutor.OPTION_SERVER_ARGS.setValue(props, "-y @modelcontextprotocol/server-everything");
        McpExecutor.OPTION_SERVER_TOOL.setValue(props, "echo");
        int count = 0;
        for (Row row : interpreter.interpret("{\"message\":\"lucky\"}", props).rows()) {
            count++;
        }
        Assert.assertEquals(count, 1);

        McpExecutor.OPTION_SERVER_CMD.setValue(config, "npx");
        McpExecutor.OPTION_SERVER_ARGS.setValue(config, "-y @modelcontextprotocol/server-everything");
        McpExecutor.OPTION_SERVER_TOOL.setValue(config, "echo");
        props.clear();
        count = 0;
        for (Row row : new McpInterpreter(context, config).interpret("{\"message\":\"lucky\"}", props).rows()) {
            count++;
        }
        Assert.assertEquals(count, 1);
    }
}
