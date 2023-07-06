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
package io.github.jdbcx;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.script.ScriptException;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ScriptingTest {
    @Test(groups = { "unit" })
    public void testConstructor() {
        Assert.assertThrows(IllegalArgumentException.class, () -> new Scripting(null, null, null));
        Assert.assertThrows(IllegalArgumentException.class, () -> new Scripting(null, new Properties(), null));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> new Scripting(null, new Properties(), new HashMap<>()));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> new Scripting("lua", new Properties(), new HashMap<>()));

        Assert.assertEquals(new Scripting("javascript", null, null).getDefaultLanguage(), "javascript");
        Assert.assertEquals(new Scripting("Groovy", new Properties(), new HashMap<>()).getDefaultLanguage(), "Groovy");
    }

    @Test(groups = { "unit" })
    public void testExecute() throws ScriptException {
        Properties props = new Properties();
        Map<String, Object> vars = new HashMap<>();
        Map<String, Object> tmp = new HashMap<>();
        Assert.assertEquals(new Scripting("javascript", props, vars).execute(null, tmp), "");
        // Assert.assertEquals(new Scripting("javascript", props, vars).execute("1+1", tmp), 2L);
        Assert.assertEquals(new Scripting("Groovy", props, vars).execute(null, tmp), "");
        Assert.assertEquals(new Scripting("Groovy", props, vars).execute("1+1", tmp), 2);

        vars.put("a", 5);
        tmp.put("b", 6L);
        // Assert.assertEquals(new Scripting("javascript", props, vars).execute("a+b", tmp), 11L);
        Assert.assertEquals(new Scripting("Groovy", props, vars).execute("a+b", tmp), 11L);
    }
}
