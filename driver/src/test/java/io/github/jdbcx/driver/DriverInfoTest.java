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
package io.github.jdbcx.driver;

import java.sql.DriverPropertyInfo;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.github.jdbcx.DriverExtension;
import io.github.jdbcx.Option;
import io.github.jdbcx.Utils;
import io.github.jdbcx.extension.PrqlDriverExtension;

public class DriverInfoTest {
    @Test(groups = { "unit" })
    public void testCreate() {
        Properties props = new Properties();
        String prefix = "1.2.3.";
        Option option = Option.of("k", null, null);
        DriverPropertyInfo info = DriverInfo.create(prefix, option, props);
        Assert.assertEquals(info.name, "1.2.3.k");
        Assert.assertEquals(info.value, "");
        Assert.assertEquals(info.description, "");
        Assert.assertEquals(info.choices, new String[0]);

        option = Option.of("k", "d", null);
        info = DriverInfo.create(prefix, option, props);
        Assert.assertEquals(info.name, "1.2.3.k");
        Assert.assertEquals(info.value, "");
        Assert.assertEquals(info.description, "d");
        Assert.assertEquals(info.choices, new String[0]);

        option = Option.of(new String[] { "k", "d", "1" });
        info = DriverInfo.create(prefix, option, props);
        Assert.assertEquals(info.name, "1.2.3.k");
        Assert.assertEquals(info.value, "1");
        Assert.assertEquals(info.description, "d");
        Assert.assertEquals(info.choices, new String[0]);

        option = Option.of(new String[] { "k", "d", "1", "2" });
        props.setProperty("k", "0");
        info = DriverInfo.create(prefix, option, props);
        Assert.assertEquals(info.name, "1.2.3.k");
        Assert.assertEquals(info.value, "0");
        Assert.assertEquals(info.description, "d");
        Assert.assertEquals(info.choices, new String[] { "1", "2" });
    }

    @Test(groups = { "unit" })
    public void testGetDriverExtension() {
        Assert.assertNotNull(DriverInfo.getDriverExtension("", null));
        Assert.assertTrue(DriverInfo.getDriverExtension("1", null) == DriverInfo.getDriverExtension("2", null));

        Map<String, DriverExtension> extensions = new HashMap<>();
        extensions.put("prql", new PrqlDriverExtension());
        Assert.assertEquals(DriverInfo.getDriverExtension("jdbcX:prql:mysql://localhost", extensions).getClass(),
                PrqlDriverExtension.class);
    }

    @Test(groups = { "unit" })
    public void testActualUrl() {
        Assert.assertEquals(new DriverInfo("jdbcx:mysql:localhost", null).actualUrl, "jdbc:mysql:localhost");
        Assert.assertEquals(new DriverInfo("jdbcx:web:mysql:localhost", null).actualUrl, "jdbc:mysql:localhost");
    }

    @Test(groups = { "unit" })
    public void testExtractExtendedProperties() {
        Properties props = new Properties();
        Assert.assertEquals(new DriverInfo(null, props).extensionProps,
                DefaultDriverExtension.getInstance().getDefaultConfig());
        props.setProperty("jdbcx.custom.classpath", "123");
        Assert.assertEquals(new DriverInfo(null, props).extensionProps.getProperty("custom.classpath"), "123");

        props.clear();
        Assert.assertEquals(new DriverInfo("jdbcx:prql:", props).extensionProps.size(),
                DefaultDriverExtension.getInstance().getDefaultConfig().size()
                        + new PrqlDriverExtension().getDefaultConfig().size());
    }

    @Test(groups = { "unit" })
    public void testDefaultConfig() {
        Properties props = new Properties();
        props.setProperty("non-related", "");
        props.setProperty("jdbcx.config.path", "target/test-classes/test-config.properties");
        for (Entry<String, DriverExtension> e : new DriverInfo(null, props).getExtensions().entrySet()) {
            Assert.assertEquals(DriverExtension.extractProperties(e.getValue(), props).getProperty("custom.classpath"),
                    "");
            DriverInfo d = new DriverInfo(Utils.format("jdbcx:%s:", e.getValue().getName()), props);
            Assert.assertEquals(d.extensionProps.getProperty("custom.classpath"), "my-classpath");
            for (Entry<Object, Object> p : props.entrySet()) {
                Assert.assertEquals(d.mergedInfo.get(p.getKey()), p.getValue());
                if (((String) p.getKey()).startsWith(Option.PROPERTY_PREFIX)) {
                    Assert.assertNull(d.normalizedInfo.get(p.getKey()), "Should not have property with JDBCX prefix");
                } else {
                    Assert.assertEquals(d.normalizedInfo.get(p.getKey()), p.getValue());
                }
            }
        }
    }
}
