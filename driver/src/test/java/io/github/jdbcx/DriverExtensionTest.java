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

import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.github.jdbcx.driver.DefaultDriverExtension;
import io.github.jdbcx.extension.BlackholeDriverExtension;
import io.github.jdbcx.extension.PrqlDriverExtension;

public class DriverExtensionTest {
    @Test(groups = { "unit" })
    public void testGetName() {
        Assert.assertEquals(DefaultDriverExtension.getInstance().getName(), "default");
        Assert.assertEquals(new BlackholeDriverExtension().getName(), "blackhole");
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
        Assert.assertEquals(DriverExtension.extractProperties(DefaultDriverExtension.getInstance(), props), expected);

        props.setProperty("jdbcx.prql.cli.path", "/path/to/prqlc");
        Assert.assertEquals(DriverExtension.extractProperties(DefaultDriverExtension.getInstance(), props), expected);

        PrqlDriverExtension ext = new PrqlDriverExtension();
        expected.putAll(ext.getDefaultConfig());
        expected.setProperty("cli.path", "/path/to/prqlc");
        Assert.assertEquals(DriverExtension.extractProperties(ext, props), expected);
    }
}
