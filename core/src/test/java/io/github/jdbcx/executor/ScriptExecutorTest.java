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
package io.github.jdbcx.executor;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ScriptExecutorTest {
    @Test(groups = { "unit" })
    public void testConstructor() {
        Assert.assertThrows(IllegalArgumentException.class, () -> new ScriptExecutor(null, null, null));
        Assert.assertThrows(IllegalArgumentException.class, () -> new ScriptExecutor(null, new Properties(), null));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> new ScriptExecutor(null, new Properties(), new HashMap<>()));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> new ScriptExecutor("lua", new Properties(), new HashMap<>()));

        Assert.assertEquals(new ScriptExecutor("rhino", null, null).getDefaultLanguage(), "rhino");
        Assert.assertEquals(new ScriptExecutor("Groovy", new Properties(), new HashMap<>()).getDefaultLanguage(),
                "Groovy");
    }

    @Test(groups = { "unit" })
    public void testExecute() throws IOException, TimeoutException {
        Properties props = new Properties();
        Map<String, Object> vars = new HashMap<>();
        Map<String, Object> tmp = new HashMap<>();
        Assert.assertEquals(new ScriptExecutor("rhino", props, vars).execute(null, props, tmp), "");
        // Assert.assertEquals(new Scripting("rhino", props, vars).execute("1+1",
        // tmp), 2L);
        Assert.assertEquals(new ScriptExecutor("Groovy", props, vars).execute(null, props, tmp), "");
        Assert.assertEquals(new ScriptExecutor("Groovy", props, vars).execute("1+1", props, tmp), 2);

        vars.put("a", 5);
        tmp.put("b", 6L);
        // Assert.assertEquals(new Scripting("rhino", props, vars).execute("a+b",
        // tmp), 11L);
        Assert.assertEquals(new ScriptExecutor("Groovy", props, vars).execute("a+b", props, tmp), 11L);
    }
}
