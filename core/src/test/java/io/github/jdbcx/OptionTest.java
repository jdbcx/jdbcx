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
package io.github.jdbcx;

import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

public class OptionTest {
    static enum Dummy {
    }

    @Test(groups = { "unit" })
    public void testBuilder() {
        Assert.assertNotNull(Option.builder(), "Builder should never be null");
        Assert.assertNotNull(Option.CUSTOM_CLASSPATH.update(), "Builder should never be null");
        Assert.assertNotNull(Option.of(new String[] { "x" }).update(), "Builder should never be null");

        Assert.assertThrows(IllegalArgumentException.class, () -> Option.builder().build());
        Assert.assertEquals(Option.builder().name("test").build(), Option.of(new String[] { "test" }));
        Assert.assertFalse(Option.builder().name("test").build() == Option.of(new String[] { "test" }));

        Assert.assertTrue(Option.CUSTOM_CLASSPATH.update().build() == Option.CUSTOM_CLASSPATH);
        Assert.assertTrue(Option.CUSTOM_CLASSPATH.update().name("test").name("custom.classpath")
                .build() == Option.CUSTOM_CLASSPATH);
        Assert.assertFalse(Option.CUSTOM_CLASSPATH.update().name("test").build() == Option.CUSTOM_CLASSPATH);
        Assert.assertFalse(Option.CUSTOM_CLASSPATH.update().description(null).build() == Option.CUSTOM_CLASSPATH);
    }

    @Test(groups = { "unit" })
    public void testConstructor() {
        Assert.assertThrows(IllegalArgumentException.class, () -> Option.of(null));
        Assert.assertThrows(IllegalArgumentException.class, () -> Option.of(new String[0]));
        Assert.assertThrows(IllegalArgumentException.class, () -> Option.of(new String[] { null }));
        Assert.assertThrows(IllegalArgumentException.class, () -> Option.of(new String[] { "" }));

        Assert.assertThrows(IllegalArgumentException.class, () -> Option.of(null, null, null));
        Assert.assertThrows(IllegalArgumentException.class, () -> Option.of("", null, null));
        // Assert.assertThrows(IllegalArgumentException.class, () -> Option.of("test",
        // null, null, new String[] { " " }));
        Assert.assertEquals(Option.of("test", "desc", "", new String[] { "y", "x" }).getDefaultValue(), "y");
        Assert.assertEquals(Option.of("test", "desc", "ttt", new String[] { "y", "x" }).getDefaultValue(), "y");

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
    public void testOfEnum() {
        Option option = Option.ofEnum("n", null, null, null);
        Assert.assertEquals(option.getDefaultValue(), "");
        Assert.assertEquals(option.getChoices(), new String[0]);

        option = Option.ofEnum("n", null, null, Dummy.class);
        Assert.assertEquals(option.getDefaultValue(), "");
        Assert.assertEquals(option.getChoices(), new String[0]);

        option = Option.ofEnum("n", null, null, Format.class);
        Assert.assertEquals(option.getDefaultValue(), Format.CSV.name());
        Assert.assertEquals(option.getChoices().length, Format.values().length);

        option = Option.ofEnum("n", null, "invalid", Compression.class);
        Assert.assertEquals(option.getDefaultValue(), Compression.NONE.name());
        Assert.assertEquals(option.getChoices().length, Compression.values().length);

        option = Option.ofEnum("n", null, "LZ4", Compression.class);
        Assert.assertEquals(option.getDefaultValue(), Compression.LZ4.name());
        Assert.assertEquals(option.getChoices().length, Compression.values().length);
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

    @Test(groups = { "unit" })
    public void testGetValue() {
        Assert.assertEquals(Option.of("key", null, "default").getValue(null), "default");
        Assert.assertEquals(Option.of("key", null, "default").getValue(new Properties()), "default");

        Properties props = new Properties();
        props.setProperty("key1", "");
        props.setProperty("key2", " ");
        Assert.assertEquals(Option.of("key", null, "default").getValue(props), "default");
        Assert.assertEquals(Option.of("key1", null, "default").getValue(props), "");
        Assert.assertEquals(Option.of("key2", null, "default").getValue(props), " ");
    }

    @Test(groups = { "unit" })
    public void testGetValueWithPreferredDefault() {
        Assert.assertEquals(Option.of("key", null, "default").getValue(null, null), "default");
        Assert.assertEquals(Option.of("key", null, "default").getValue(new Properties(), null), "default");
        Assert.assertEquals(Option.of("key", null, "default").getValue(null, ""), "");
        Assert.assertEquals(Option.of("key", null, "default").getValue(new Properties(), ""), "");
        Assert.assertEquals(Option.of("key", null, "default").getValue(null, "x"), "x");
        Assert.assertEquals(Option.of("key", null, "default").getValue(new Properties(), "x"), "x");

        Properties props = new Properties();
        props.setProperty("key1", "");
        props.setProperty("key2", " ");
        Assert.assertEquals(Option.of("key", null, "default").getValue(props, null), "default");
        Assert.assertEquals(Option.of("key1", null, "default").getValue(props, null), "");
        Assert.assertEquals(Option.of("key2", null, "default").getValue(props, null), " ");
        Assert.assertEquals(Option.of("key", null, "default").getValue(props, ""), "");
        Assert.assertEquals(Option.of("key1", null, "default").getValue(props, ""), "");
        Assert.assertEquals(Option.of("key2", null, "default").getValue(props, ""), " ");
        Assert.assertEquals(Option.of("key", null, "default").getValue(props, "x"), "x");
        Assert.assertEquals(Option.of("key1", null, "default").getValue(props, "x"), "");
        Assert.assertEquals(Option.of("key2", null, "default").getValue(props, "x"), " ");
    }

    @Test(groups = { "unit" })
    public void testSetValue() {
        Assert.assertEquals(Option.of("key", null, "default").setValue(null), null);
        Assert.assertEquals(Option.of("key", null, "default").setValue(new Properties()), null);

        Properties props = new Properties();
        props.setProperty("key1", "");
        props.setProperty("key2", " ");
        Assert.assertEquals(Option.of("key", null, "default").setValue(props), null);
        Assert.assertEquals(props.get("key"), "default");
        Assert.assertEquals(Option.of("key1", null, "default").setValue(props), "");
        Assert.assertEquals(props.get("key1"), "default");
        Assert.assertEquals(Option.of("key2", null, "default").setValue(props), " ");
        Assert.assertEquals(props.get("key2"), "default");
        Assert.assertEquals(props.size(), 3); // key + key1 + key2
    }

    @Test(groups = { "unit" })
    public void testSetValueIfNoPresent() {
        final String key = "key";
        final Option o = Option.of(key, null, "default");
        Properties props = null;
        o.setValueIfNotPresent(props, null);
        o.setDefaultValueIfNotPresent(props);

        props = new Properties();
        o.setValueIfNotPresent(props, null);
        Assert.assertEquals(props.getProperty(key), "default");
        props.clear();
        o.setDefaultValueIfNotPresent(props);
        Assert.assertEquals(props.getProperty(key), "default");

        o.setValueIfNotPresent(props, "v1");
        Assert.assertEquals(props.getProperty(key), "default");
        props.clear();
        o.setValueIfNotPresent(props, "v1");
        Assert.assertEquals(props.getProperty(key), "v1");

        props.clear();
        o.setDefaultValueIfNotPresent(props);
        Assert.assertEquals(props.getProperty(key), "default");
    }

    @Test(groups = { "unit" })
    public void testSetValueWithPreferredDefault() {
        Assert.assertEquals(Option.of("key", null, "default").setValue(null, null), null);
        Assert.assertEquals(Option.of("key", null, "default").setValue(new Properties(), null), null);
        Assert.assertEquals(Option.of("key", null, "default").setValue(null, ""), null);
        Assert.assertEquals(Option.of("key", null, "default").setValue(new Properties(), ""), null);
        Assert.assertEquals(Option.of("key", null, "default").setValue(null, "x"), null);
        Assert.assertEquals(Option.of("key", null, "default").setValue(new Properties(), "x"), null);

        Properties props = new Properties();
        props.setProperty("key1", "");
        props.setProperty("key2", " ");
        Assert.assertEquals(Option.of("key", null, "default").setValue(props, null), null);
        Assert.assertEquals(props.get("key"), "default");
        Assert.assertEquals(Option.of("key1", null, "default").setValue(props, null), "");
        Assert.assertEquals(props.get("key1"), "default");
        Assert.assertEquals(Option.of("key2", null, "default").setValue(props, null), " ");
        Assert.assertEquals(props.get("key2"), "default");
        Assert.assertEquals(Option.of("key", null, "default").setValue(props, ""), "default");
        Assert.assertEquals(props.get("key"), "");
        Assert.assertEquals(Option.of("key1", null, "default").setValue(props, ""), "default");
        Assert.assertEquals(props.get("key1"), "");
        Assert.assertEquals(Option.of("key2", null, "default").setValue(props, ""), "default");
        Assert.assertEquals(props.get("key2"), "");
        Assert.assertEquals(Option.of("key", null, "default").setValue(props, "x"), "");
        Assert.assertEquals(props.get("key"), "x");
        Assert.assertEquals(Option.of("key1", null, "default").setValue(props, "x"), "");
        Assert.assertEquals(props.get("key1"), "x");
        Assert.assertEquals(Option.of("key2", null, "default").setValue(props, "x"), "");
        Assert.assertEquals(props.get("key2"), "x");
        Assert.assertEquals(props.size(), 3); // key + key1 + key2
    }
}
