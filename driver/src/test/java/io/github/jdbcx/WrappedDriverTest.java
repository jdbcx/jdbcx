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
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Statement;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.github.jdbcx.impl.BlackholeDriverExtension;
import io.github.jdbcx.impl.DefaultDriverExtension;
import io.github.jdbcx.prql.PrqlDriverExtension;
import io.github.jdbcx.shell.ShellDriverExtension;

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

        int tables = 0;
        try (Connection conn = d.connect("jdbcx:derby:memory:x;create=True", props);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("select * from SYS.SYSTABLES")) {
            while (rs.next()) {
                tables++;
            }
            Assert.assertTrue(tables > 0, "Should have at least one table in the database");
        }

        // props.setProperty("jdbcx.custom.classpath", "~/Backup");
        // props.setProperty("jdbcx.prql.cli.path", "~/.cargo/bin/prqlc");
        // try (Connection conn = d.connect("jdbcx:prql:derby:memory:x;create=True",
        // props);
        // Statement stmt = conn.createStatement();
        // ResultSet rs = stmt.executeQuery("from `SYS.SYSTABLES`")) {
        // int count = 0;
        // while (rs.next()) {
        // count++;
        // }
        // Assert.assertEquals(count, tables);
        // }

        try (Connection conn = d.connect("jdbcx:script:derby:memory:x;create=True", props);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("'select * ' + 'from SYS.SYSTABLES'")) {
            int count = 0;
            while (rs.next()) {
                count++;
            }
            Assert.assertEquals(count, tables);
        }
    }

    @Test(groups = { "unit" })
    public void testErrorHandling() throws Exception {
        String url = "jdbcx:script:derby:memory:x;create=True";
        Properties props = new Properties();
        WrappedDriver d = new WrappedDriver();
        d.getPropertyInfo(url, props);
        int tables = 0;
        try (Connection conn = d.connect(url, props);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("helper.format('select * from %s.%s', 'SYS', 'SYSTABLES')")) {
            while (rs.next()) {
                tables++;
            }
            Assert.assertTrue(tables > 0, "Should have at least one table in the database");
        }

        try (Connection conn = d.connect(url, props); Statement stmt = conn.createStatement();) {
            Assert.assertThrows(SQLException.class, () -> stmt.executeQuery("helper.invalidMethod()"));
        }

        try (Connection conn = d.connect(url, props);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("select * from SYS.SYSTABLES")) {
            int count = 0;
            while (rs.next()) {
                count++;
            }
            Assert.assertEquals(count, tables);
        }

        props.setProperty("jdbcx.script.error", "throw");
        try (Connection conn = d.connect(url, props); Statement stmt = conn.createStatement();) {
            Assert.assertThrows(SQLException.class, () -> stmt.executeQuery("helper.invalidMethod()"));
        }
        try (Connection conn = d.connect(url, props); Statement stmt = conn.createStatement();) {
            Assert.assertThrows(SQLException.class, () -> stmt.executeQuery("select * from SYS.SYSTABLES"));
        }

        props.setProperty("jdbcx.script.error", "warn");
        try (Connection conn = d.connect(url, props); Statement stmt = conn.createStatement();) {
            Assert.assertThrows(SQLSyntaxErrorException.class, () -> stmt.executeQuery("helper.invalidMethod()"));
        }
        props.setProperty("jdbcx.shell.cli.error", "warn");
        try (Connection conn = DriverManager.getConnection("jdbcx:shell:derby:memory:x;create=True", props);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("select * from SYS.SYSTABLES")) {
            Assert.assertNotNull(stmt.getWarnings(), "Should have SQLWarning");
        }
        try (Connection conn = d.connect(url, props);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("select * from SYS.SYSTABLES")) {
            Assert.assertNotNull(stmt.getWarnings(), "Should have SQLWarning");
            int count = 0;
            while (rs.next()) {
                count++;
            }
            Assert.assertEquals(count, tables);
        }
    }

    @Test(groups = { "unit" })
    public void testExtractExtendedProperties() {
        Properties props = new Properties();
        WrappedDriver d = new WrappedDriver();
        Assert.assertEquals(d.extractExtendedProperties(DefaultDriverExtension.getInstance(), props),
                DefaultDriverExtension.getInstance().getDefaultConfig());
        props.setProperty("jdbcx.custom.classpath", "123");
        Assert.assertEquals(d.extractExtendedProperties(DefaultDriverExtension.getInstance(), props)
                .getProperty("custom.classpath"), "123");

        props.clear();
        Assert.assertEquals(d.extractExtendedProperties(new PrqlDriverExtension(), props).size(),
                DefaultDriverExtension.getInstance().getDefaultConfig().size()
                        + new PrqlDriverExtension().getDefaultConfig().size());
    }

    @Test(groups = { "unit" })
    public void testLoadDefaultConfig() {
        Properties props = new Properties();
        props.setProperty("non-related", "");
        props.setProperty("jdbcx.config.path", "target/test-classes/test-config.properties");
        WrappedDriver d = new WrappedDriver();
        Properties extProps = d.extractExtendedProperties(new ShellDriverExtension(), props);
        Assert.assertEquals(extProps.getProperty("custom.classpath"), "my-classpath");
        extProps = d.extractExtendedProperties(new BlackholeDriverExtension(), props);
        Assert.assertEquals(extProps.getProperty("custom.classpath"), "my-classpath");
    }
}
