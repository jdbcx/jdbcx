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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.github.jdbcx.driver.DefaultDriverExtension;
import io.github.jdbcx.extension.PrqlDriverExtension;

public class DriverExtensionTest {
    static final class TestConfigManager extends ConfigManager {
        private final Map<String, Properties> config;

        protected TestConfigManager(Map<String, Properties> config) {
            super(null);

            this.config = new HashMap<>();
            if (config != null) {
                this.config.putAll(config);
            }
        }

        @Override
        public Properties getConfig(String category, String id) {
            Properties props = new Properties();
            Properties defined = config.get(id);
            if (defined != null) {
                props.putAll(defined);
            }
            Option.ID.setValue(props, id);
            return props;
        }

        @Override
        public boolean hasConfig(String category, String id) {
            Properties props = config.get(category);
            return props != null && props.containsKey(id);
        }
    }

    static final class TestDriverExtension implements DriverExtension {
        private final List<Option> defaultOptions;

        TestDriverExtension(Option... options) {
            this.defaultOptions = Collections.unmodifiableList(Arrays.asList(options));
        }

        @Override
        public List<Option> getDefaultOptions() {
            return defaultOptions;
        }
    }

    @Test(groups = { "unit" })
    public void testGetName() {
        Assert.assertEquals(DefaultDriverExtension.getInstance().getName(), "default");
        Assert.assertEquals(new PrqlDriverExtension().getName(), "prql");
    }

    @Test(groups = { "unit" })
    public void testExtractProperties() {
        Properties props = new Properties();
        Assert.assertEquals(DriverExtension.extractProperties(null, null),
                DefaultDriverExtension.getInstance().getDefaultConfig());
        Assert.assertEquals(DriverExtension.extractProperties(DefaultDriverExtension.getInstance(), props),
                DefaultDriverExtension.getInstance().getDefaultConfig());

        props.setProperty("custom.property1", "value1");
        Properties expected = new Properties();
        expected.putAll(DefaultDriverExtension.getInstance().getDefaultConfig());
        Assert.assertEquals(DriverExtension.extractProperties(DefaultDriverExtension.getInstance(), props), expected);

        props.setProperty("jdbcx.custom.property2", "value2");
        expected.setProperty("custom.property2", "value2");
        Assert.assertEquals(DriverExtension.extractProperties(DefaultDriverExtension.getInstance(), props), expected);

        props.setProperty("jdbcx.prql.cli.path", "/path/to/prqlc");
        expected.setProperty("prql.cli.path", "/path/to/prqlc");
        Assert.assertEquals(DriverExtension.extractProperties(DefaultDriverExtension.getInstance(), props), expected);

        PrqlDriverExtension ext = new PrqlDriverExtension();
        expected.remove("custom.property2");
        expected.remove("prql.cli.path");
        expected.putAll(ext.getDefaultConfig());
        expected.setProperty("cli.path", "/path/to/prqlc");
        Assert.assertEquals(DriverExtension.extractProperties(ext, props), expected);
    }

    @Test(groups = { "unit" })
    public void testGetConfig() {
        Properties config = new Properties();
        config.setProperty("x1", "y1");
        ConfigManager manager = new TestConfigManager(Collections.singletonMap("x", config));
        TestDriverExtension test = new TestDriverExtension(Option.of(new String[] { "x1", "", "y1", "y11" }),
                Option.of(new String[] { "x2", "" }));
        Assert.assertEquals(manager.getConfig(test.getName(), "y"), Collections.singletonMap("id", "y"));

        Properties props = new Properties();
        Option.ID.setValue(props, "x");

        Properties expected = new Properties();
        expected.setProperty("id", "x");
        expected.setProperty("x1", "y1");
        expected.setProperty("x2", "");
        Assert.assertEquals(test.getConfig(manager, props), expected);

        manager = new TestConfigManager(Collections.singletonMap("x", config));
        test = new TestDriverExtension(Option.of(new String[] { "x1", "" }),
                Option.of(new String[] { "x2", "", "y2" }));
        expected.setProperty("x2", "y2");
        Assert.assertEquals(test.getConfig(manager, props), expected);

        Option.ID.setValue(props, "y");
        expected.setProperty("id", "y");
        expected.setProperty("x1", "");
        Assert.assertEquals(test.getConfig(manager, props), expected);
    }

    @Test(groups = { "unit" })
    public void testGetConfigOverriding() {
        Properties config = new Properties();
        config.setProperty("x1", "y1");
        config.setProperty("x2", "y2");
        config.setProperty("x3", "y3");
        ConfigManager manager = new TestConfigManager(Collections.singletonMap("x", config));
        TestDriverExtension test = new TestDriverExtension(Option.of(new String[] { "x1", "" }),
                Option.of(new String[] { "x2", "" }));

        Properties expected = new Properties();
        expected.putAll(config);
        expected.setProperty("id", "x");
        Assert.assertEquals(manager.getConfig(test.getName(), "x"), expected);

        Properties props = new Properties();
        Option.ID.setValue(props, "y");
        Assert.assertEquals(manager.getConfig(test.getName(), "y"), Collections.singletonMap("id", "y"));

        Option.ID.setValue(props, "x");
        props.setProperty("x1", "");
        props.setProperty("x2", "y22");
        props.setProperty("x4", "y4");
        Assert.assertEquals(test.getConfig(manager, props), expected);
        Assert.assertEquals(test.getConfig(manager, props).getProperty("x4"), "y4");
    }
}
