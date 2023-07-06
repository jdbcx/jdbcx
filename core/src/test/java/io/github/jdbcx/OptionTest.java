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

import org.testng.Assert;
import org.testng.annotations.Test;

public class OptionTest {
    @Test(groups = { "unit" })
    public void testConstructor() {
        Assert.assertThrows(IllegalArgumentException.class, () -> Option.of(null));
        Assert.assertThrows(IllegalArgumentException.class, () -> Option.of(new String[0]));
        Assert.assertThrows(IllegalArgumentException.class, () -> Option.of(new String[] { null }));
        Assert.assertThrows(IllegalArgumentException.class, () -> Option.of(new String[] { "" }));

        Assert.assertThrows(IllegalArgumentException.class, () -> Option.of(null, null, null));
        Assert.assertThrows(IllegalArgumentException.class, () -> Option.of("", null, null));
        Assert.assertThrows(IllegalArgumentException.class, () -> Option.of("test", null, null, new String[] { " " }));

        Option option = Option.of("test", null, null, null);
        Assert.assertEquals(option.getName(), "test");
        Assert.assertEquals(option.getDescription(), "");
        Assert.assertEquals(option.getDefaultValue(), "");
        Assert.assertEquals(option.getChoices(), new String[0]);

        option = Option.of("test", "desc", "x");
        Assert.assertEquals(option.getName(), "test");
        Assert.assertEquals(option.getDescription(), "desc");
        Assert.assertEquals(option.getDefaultValue(), "x");
        Assert.assertEquals(option.getChoices(), new String[0]);

        option = Option.of("test", "desc", "x", new String[] { "y", "x" });
        Assert.assertEquals(option.getName(), "test");
        Assert.assertEquals(option.getDescription(), "desc");
        Assert.assertEquals(option.getDefaultValue(), "x");
        Assert.assertEquals(option.getChoices(), new String[] { "y", "x" });
    }

    @Test(groups = { "unit" })
    public void testEquals() {
        Assert.assertEquals(Option.of("t.1.2.3", null, null, null), Option.of(new String[] { "t.1.2.3" }));
        Assert.assertEquals(Option.of("t.1.2.3", "", ""), Option.of(new String[] { "t.1.2.3" }));
        Assert.assertEquals(Option.of("t.1.2.3", "", ""),
                Option.of(new String[] { "t.1.2.3", null, null, null, null, null, null }));
        Assert.assertEquals(Option.of("t.1.2.3", "desc", "123"), Option.of(new String[] { "t.1.2.3", "desc", "123" }));
        Assert.assertEquals(Option.of("t.1.2.3", "desc", "123", "123", ""),
                Option.of(new String[] { "t.1.2.3", "desc", "123", "" }));
    }

    @Test(groups = { "unit" })
    public void testGetEffectiveDefaultValue() {
        // environment variables are set in pom.xml
        Option option = Option.of("test.str", "string option", "string");
        Assert.assertEquals(option.getDefaultValue(), "string");
        Assert.assertEquals(option.getEffectiveDefaultValue(null), "string");
        Assert.assertEquals(option.getEffectiveDefaultValue(""), "string");
        Assert.assertEquals(option.getEffectiveDefaultValue("jdbcx"), "string");
        Assert.assertEquals(option.getEffectiveDefaultValue("jdbcx."), "env_str");
        Assert.assertEquals(option.getEffectiveDefaultValue("jdbcx.de."), "eee");

        option = Option.of(new String[] { "test.int", "int option", "233", "123", "321" });
        Assert.assertEquals(option.getDefaultValue(), "233");
        Assert.assertEquals(option.getEffectiveDefaultValue(null), "233");
        Assert.assertEquals(option.getEffectiveDefaultValue(""), "233");
        Assert.assertEquals(option.getEffectiveDefaultValue("jdbcx"), "233");
        Assert.assertEquals(option.getEffectiveDefaultValue("jdbcx."), "321");
        Assert.assertEquals(option.getEffectiveDefaultValue("jdbcx.de."), "123");
    }
}
