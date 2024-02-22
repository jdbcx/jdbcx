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

import java.util.Collections;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.github.jdbcx.Field;
import io.github.jdbcx.QueryContext;
import io.github.jdbcx.Result;
import io.github.jdbcx.Row;

public class VarInterpreterTest {
    @Test(groups = { "unit" })
    public void testConstructor() {
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

    @Test(groups = { "unit" })
    public void testInterpret() {
        QueryContext context = QueryContext.newContext();
        Properties config = new Properties();

        VarInterpreter interpreter = new VarInterpreter(context, config);
        Result<?> result = interpreter.interpret(" a = 1, b = '2', c-d=['a\\,b'] ", config);
        Assert.assertEquals(result.fields(), Collections.singletonList(Field.DEFAULT));
        for (Row r : result.rows()) {
            Assert.assertEquals(r.fields(), result.fields());
            Assert.assertEquals(r.values().size(), 1);
            Assert.assertEquals(r.value(0).asString(), "");
        }
        Properties vars = new Properties();
        vars.setProperty("a", "1");
        vars.setProperty("b", "'2'");
        vars.setProperty("c-d", "['a,b']");
        Assert.assertEquals(context.getVariable("a"), vars.getProperty("a"));
        Assert.assertEquals(context.getVariable("b"), vars.getProperty("b"));
        Assert.assertEquals(context.getVariable("c-d"), vars.getProperty("c-d"));
        // FIXME Maps do not have the same size:4 != 3
        // Assert.assertEquals(context.getMergedVariables(), vars);

        String prefix = "1-2-3";
        context = QueryContext.newContext();
        VarInterpreter.OPTION_DELIMITER.setValue(config, ";");
        VarInterpreter.OPTION_PREFIX.setValue(config, prefix);

        interpreter = new VarInterpreter(context, config);
        result = interpreter.interpret("a=[1,'2',3];b='c'", config);
        Assert.assertEquals(result.fields(), Collections.singletonList(Field.DEFAULT));
        Assert.assertEquals(result.fields(), Collections.singletonList(Field.DEFAULT));
        for (Row r : result.rows()) {
            Assert.assertEquals(r.fields(), result.fields());
            Assert.assertEquals(r.values().size(), 1);
            Assert.assertEquals(r.value(0).asString(), "");
        }
        vars = new Properties();
        String keya = prefix + "a";
        String keyb = prefix + "b";
        vars.setProperty(keya, "[1,'2',3]");
        vars.setProperty(keyb, "'c'");
        Assert.assertEquals(context.getVariable(keya), vars.getProperty(keya));
        Assert.assertEquals(context.getVariable(keyb), vars.getProperty(keyb));
        // Assert.assertEquals(context.getMergedVariables(), vars);
    }
}
