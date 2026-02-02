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

import java.io.IOException;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.github.jdbcx.BaseIntegrationTest;
import io.github.jdbcx.Option;
import io.github.jdbcx.QueryContext;
import io.github.jdbcx.executor.CommandLineExecutor;

public class CodeQLInterpreterTest extends BaseIntegrationTest {
    @Test(groups = { "private" })
    public void testInterpret() throws IOException {
        QueryContext context = QueryContext.newContext();
        Properties config = new Properties();
        CommandLineExecutor.OPTION_CLI_PATH.setValue(config, "/opt/homebrew/bin/codeql");
        CommandLineExecutor.OPTION_CLI_TEST_ARGS.setValue(config, "-V");

        String query = "import java\nfrom Class c\nwhere c.getName() = \"Main\"\nselect c";
        CodeQLInterpreter i = new CodeQLInterpreter(context, config);
        Properties props = new Properties();
        CodeQLInterpreter.OPTION_DATABASE.setValue(props, "~/Sources/Local/CodeQL/jdbcx");
        Option.WORK_DIRECTORY.setValue(props, "~/Sources/Github/jdbcx");
        Assert.assertEquals(i.interpret(query, props).get(String.class), "|  c   |\n+------+\n| Main |\n");

        CodeQLInterpreter.OPTION_FORMAT.setValue(props, "");
        Assert.assertEquals(i.interpret(query, props).get(String.class), "|  c   |\n+------+\n| Main |\n");

        CodeQLInterpreter.OPTION_FORMAT.setValue(props, "bqrs");
        Assert.assertTrue(i.interpret(query, props).get(String.class).length() > 0, "Should have content");

        CodeQLInterpreter.OPTION_FORMAT.setValue(props, "csv");
        Assert.assertEquals(i.interpret(query, props).get(String.class), "\"c\"\n\"Main\"\n");

        CodeQLInterpreter.OPTION_FORMAT.setValue(props, "json");
        Assert.assertEquals(i.interpret(query, props).get(String.class), "{\"#select\":{\"columns\":[\n" +
                "   {\"name\":\"c\",\"kind\":\"Entity\"}]\n ,\"tuples\":[\n   [{\"label\":\"Main\"}]]\n }}\n");

        Option.EXEC_TIMEOUT.setValue(props, "1000");
        CodeQLInterpreter.OPTION_FORMAT.setValue(props, "text");
        Assert.assertEquals(i.interpret(query, props).get(String.class), "|  c   |\n+------+\n| Main |\n");
    }
}
