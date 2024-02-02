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
package io.github.jdbcx.interpreter;

import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.github.jdbcx.QueryContext;

public class VarInterpreterTest {
    @Test(groups = { "unit" })
    public void testInterpret() {
        QueryContext context = QueryContext.newContext();
        Properties config = new Properties();

        VarInterpreter interpreter = new VarInterpreter(context, config);
        Assert.assertEquals(interpreter.defaultDelimiter, VarInterpreter.OPTION_DELIMITER.getDefaultValue().charAt(0));
        Assert.assertEquals(interpreter.defaultPrefix, VarInterpreter.OPTION_PREFIX.getDefaultValue());

        config.put(VarInterpreter.OPTION_DELIMITER.getName(), ";");
        interpreter = new VarInterpreter(context, config);
        Assert.assertEquals(interpreter.defaultDelimiter, ';');
        Assert.assertEquals(interpreter.defaultPrefix, VarInterpreter.OPTION_PREFIX.getDefaultValue());

        config.put(VarInterpreter.OPTION_PREFIX.getName(), "Prefix_");
        interpreter = new VarInterpreter(context, config);
        Assert.assertEquals(interpreter.defaultDelimiter, ';');
        Assert.assertEquals(interpreter.defaultPrefix, "Prefix_");
    }
}
