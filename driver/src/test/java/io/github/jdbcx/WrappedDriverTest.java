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

import java.sql.Connection;
import java.sql.DriverPropertyInfo;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.Test;

import io.github.jdbcx.impl.DefaultDriverExtension;
import io.github.jdbcx.prql.PrqlDriverExtension;

public class WrappedDriverTest {
    @Test(groups = { "unit" })
    public void testCreate() {
        Properties props = new Properties();
        String prefix = "1.2.3.";
        Option option = Option.of("k", null, null);
        DriverPropertyInfo info = WrappedDriver.create(prefix, option, props);
        Assert.assertEquals(info.name, "1.2.3.k");
        Assert.assertEquals(info.value, "");
        Assert.assertEquals(info.description, "");
        Assert.assertEquals(info.choices, new String[0]);

        option = Option.of("k", "d", null);
        info = WrappedDriver.create(prefix, option, props);
        Assert.assertEquals(info.name, "1.2.3.k");
        Assert.assertEquals(info.value, "");
        Assert.assertEquals(info.description, "d");
        Assert.assertEquals(info.choices, new String[0]);

        option = Option.of(new String[] { "k", "d", "1" });
        info = WrappedDriver.create(prefix, option, props);
        Assert.assertEquals(info.name, "1.2.3.k");
        Assert.assertEquals(info.value, "1");
        Assert.assertEquals(info.description, "d");
        Assert.assertEquals(info.choices, new String[0]);

        option = option.of(new String[] { "k", "d", "1", "2" });
        props.setProperty("k", "0");
        info = WrappedDriver.create(prefix, option, props);
        Assert.assertEquals(info.name, "1.2.3.k");
        Assert.assertEquals(info.value, "0");
        Assert.assertEquals(info.description, "d");
        Assert.assertEquals(info.choices, new String[] { "1", "2" });
    }

    @Test(groups = { "unit" })
    public void testGetDriverExtension() {
        WrappedDriver d = new WrappedDriver();
        Assert.assertNotNull(d.getDriverExtension(""));
        Assert.assertTrue(d.getDriverExtension("1") == d.getDriverExtension("2"));

        Assert.assertEquals(d.getDriverExtension("JDBCX:blackhole:mysql://localhost").getClass(),
                BlackholeDriverExtension.class);
        Assert.assertEquals(d.getDriverExtension("jdbcX:prql:mysql://localhost").getClass(), PrqlDriverExtension.class);
    }

    @Test(groups = { "unit" })
    public void testGetExtensionName() {
        WrappedDriver d = new WrappedDriver();
        Assert.assertEquals(d.getExtensionName(DefaultDriverExtension.getInstance()), "default");
        Assert.assertEquals(d.getExtensionName(new BlackholeDriverExtension()), "blackhole");
        Assert.assertEquals(d.getExtensionName(new PrqlDriverExtension()), "prql");
    }

    @Test(groups = { "unit" })
    public void testNormalizeUrl() {
        WrappedDriver d = new WrappedDriver();
        Assert.assertEquals(d.normalizeUrl(DefaultDriverExtension.getInstance(), "jdbcx:mysql:localhost"),
                "jdbc:mysql:localhost");
        Assert.assertEquals(d.normalizeUrl(new BlackholeDriverExtension(), "jdbcx:blackhole:mysql:localhost"),
                "jdbc:mysql:localhost");
    }

    @Test(groups = { "unit" })
    public void testConnect() throws Exception {
        Properties props = new Properties();
        WrappedDriver d = new WrappedDriver();

        try (Connection conn = d.connect("jdbcx:ch://explorer@play.clickhouse.com:443?ssl=true", props);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("select * from numbers(10)")) {
            int count = 0;
            while (rs.next()) {
                Assert.assertEquals(count++, rs.getInt(1));
            }
            Assert.assertEquals(count, 10);
        }

        // props.setProperty("jdbcx.custom.classpath", "~/Back");
        // props.setProperty("jdbcx.prql.cli.path", "~/.cargo/bin/prqlc");
        // try (Connection conn = d.connect("jdbcx:prql:ch://explorer@play.clickhouse.com:443?ssl=true", props);
        //         Statement stmt = conn.createStatement();
        //         ResultSet rs = stmt.executeQuery("from `system.events` | take 10")) {
        //     int count = 0;
        //     while (rs.next()) {
        //         count++;
        //     }
        //     Assert.assertEquals(count, 10);
        // }

        try (Connection conn = d.connect("jdbcx:script:ch://explorer@play.clickhouse.com:443?ssl=true", props);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("'select * ' + 'from numbers(10)'")) {
            int count = 0;
            while (rs.next()) {
                Assert.assertEquals(count++, rs.getInt(1));
            }
            Assert.assertEquals(count, 10);
        }
    }

    @Test(groups = { "unit" })
    public void testReconnect() throws Exception {
        throw new SkipException("Skip prqlc test due to extra dependency");
        // String url = "jdbcx:prql:ch://explorer@play.clickhouse.com:443?ssl=true";
        // Properties props = new Properties();
        // WrappedDriver d = new WrappedDriver();
        // props.setProperty("jdbcx.custom.classpath", "~/Back");
        // props.setProperty("jdbcx.prql.cli.path", "~/.cargo/bin/prqlc");
        // d.getPropertyInfo(url, new Properties());
        // try (Connection conn = d.connect(url, props);
        //         Statement stmt = conn.createStatement();
        //         ResultSet rs = stmt.executeQuery("select * from numbers(10)")) {
        //     int count = 0;
        //     while (rs.next()) {
        //         Assert.assertEquals(count++, rs.getInt(1));
        //     }
        //     Assert.assertEquals(count, 10);
        // }

        // props.setProperty("jdbcx.prql.compile.fallback", "false");
        // try (Connection conn = d.connect(url, props);
        //         Statement stmt = conn.createStatement();) {
        //     Assert.assertThrows(SQLException.class, () -> stmt.executeQuery("select * from numbers(10)"));
        // }
    }

    @Test(groups = { "unit" })
    public void testExtractExtendedProperties() {
        Properties props = new Properties();
        Assert.assertEquals(WrappedDriver.extractExtendedProperties(DefaultDriverExtension.getInstance(), props),
                DefaultDriverExtension.getInstance().getDefaultConfig());
        props.setProperty("jdbcx.custom.classpath", "123");
        Assert.assertEquals(WrappedDriver.extractExtendedProperties(DefaultDriverExtension.getInstance(), props)
                .getProperty("custom.classpath"), "123");

        props.clear();
        Assert.assertEquals(WrappedDriver.extractExtendedProperties(new PrqlDriverExtension(), props).size(),
                DefaultDriverExtension.getInstance().getDefaultConfig().size()
                        + new PrqlDriverExtension().getDefaultConfig().size());
    }
}
