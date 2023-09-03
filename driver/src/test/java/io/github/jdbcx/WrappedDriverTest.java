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
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class WrappedDriverTest extends BaseIntegrationTest {
    private ResultSet executeQuery(Statement stmt, String query, boolean useExecuteQuery) throws SQLException {
        final ResultSet rs;
        if (useExecuteQuery) {
            return stmt.executeQuery(query);
        } else {
            Assert.assertTrue(stmt.execute(query));
            rs = stmt.getResultSet();
        }
        return rs;
    }

    @DataProvider(name = "queryMethod")
    public static Object[][] getQueryMethod() {
        return new Object[][] {
                { true }, // use executeQuery()
                { false } // use execute()
        };
    }

    @Test(groups = { "unit" })
    public void testAcceptsURL() throws SQLException {
        final WrappedDriver d = new WrappedDriver();
        Assert.assertFalse(d.acceptsURL(null), "Null URL should NOT be supported");
        Assert.assertFalse(d.acceptsURL(""), "Empty URL should NOT be supported");
        Assert.assertFalse(d.acceptsURL(" \t\r\n "), "Blank URL should NOT be supported");
        Assert.assertFalse(d.acceptsURL("jdbc:"), "JDBC URL should NOT be supported");

        Assert.assertTrue(d.acceptsURL("jdbcx:"), "JDBCX URL with only prefix should be supported");
        Assert.assertTrue(d.acceptsURL("jdbcx:mysql"), "JDBCX URL should be supported");
    }

    @Test(groups = { "unit" })
    public void testConnectURL() throws SQLException {
        final WrappedDriver d = new WrappedDriver();
        Assert.assertNotNull(d.connect(null, null));
        Assert.assertNotNull(d.connect(null, new Properties()));
        Assert.assertNotNull(d.connect("", null));

        try (Connection c = d.connect("jdbc:derby:memory:x;create=true", null)) {
            Assert.assertNotNull(c);
        }

        try (Connection c = d.connect("jdbcx:derby:memory:x;create=true", null)) {
            Assert.assertNotNull(c);
        }
    }

    @Test(groups = { "unit" })
    public void testVariable() throws Exception {
        String url = "jdbcx:derby:memory:x;create=true";
        Properties props = new Properties();
        try (Connection conn = DriverManager.getConnection(url, props); Statement stmt = conn.createStatement()) {
            try (ResultSet rs = stmt.executeQuery(
                    "{% vars: db=SYS1, tbl='SYS1TABLES' %}select * {{ vars(prefix='x.'): db=SYS, tbl=SYSTABLES }}from ${x.db}.${x.tbl}")) {
                Assert.assertTrue(rs.next());
            }

            try (ResultSet rs = stmt.executeQuery(
                    "{% vars: m=true %}select '{{ shell(result.string.trim=${m}): echo ${m}}}' a from SYS.SYSTABLES")) {
                while (rs.next()) {
                    Assert.assertEquals(rs.getString(1), "true");
                }
            }
        }
    }

    @Test(groups = { "unit" })
    public void testErrorHandling() throws Exception {
        String url = "jdbcx:script:derby:memory:x;create=true";
        WrappedDriver d = new WrappedDriver();
        Assert.assertNotNull(d.getPropertyInfo(url, null));
        int tables = 0;
        try (Connection conn = d.connect(url, null);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("helper.format('select * from %s.%s', 'SYS', 'SYSTABLES')")) {
            while (rs.next()) {
                tables++;
            }
            Assert.assertTrue(tables > 0, "Should have at least one table in the database");
        }

        try (Connection conn = d.connect(url, null); Statement stmt = conn.createStatement();) {
            Assert.assertThrows(SQLException.class, () -> stmt.executeQuery("helper.invalidMethod()"));
        }

        try (Connection conn = d.connect(url, null);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("select * from SYS.SYSTABLES")) {
            int count = 0;
            while (rs.next()) {
                count++;
            }
            Assert.assertEquals(count, tables);
        }

        Properties props = new Properties();
        props.setProperty("jdbcx.script.exec.error", "throw");
        try (Connection conn = d.connect(url, props); Statement stmt = conn.createStatement();) {
            Assert.assertThrows(SQLException.class, () -> stmt.executeQuery("helper.invalidMethod()"));
        }
        try (Connection conn = d.connect(url, props); Statement stmt = conn.createStatement();) {
            Assert.assertThrows(SQLException.class, () -> stmt.executeQuery("select * from SYS.SYSTABLES"));
        }

        props = new Properties();
        props.setProperty("jdbcx.script.exec.error", "warn");
        try (Connection conn = d.connect(url, props); Statement stmt = conn.createStatement();) {
            Assert.assertThrows(SQLSyntaxErrorException.class, () -> stmt.executeQuery("helper.invalidMethod()"));
        }

        props = new Properties();
        props.setProperty("jdbcx.shell.exec.error", "warn");
        try (Connection conn = DriverManager.getConnection("jdbcx:shell:derby:memory:x;create=true", props);
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

    @Test(groups = { "integration" })
    public void testConnect() throws Exception {
        Properties props = new Properties();
        WrappedDriver d = new WrappedDriver();

        final int expected = 15;
        final String query = Utils.format("select * from numbers(%d)", expected);

        try (Connection conn = d.connect("jdbcx:ch://" + getClickHouseServer(), props);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(query)) {
            int count = 0;
            while (rs.next()) {
                Assert.assertEquals(count++, rs.getInt(1));
            }
            Assert.assertEquals(count, expected);
        }

        // props.setProperty("jdbcx.custom.classpath", "~/Backup");
        // props.setProperty("jdbcx.prql.cli.path", "~/.cargo/bin/prqlc");
        // try (Connection conn = d.connect("jdbcx:prql:derby:memory:x;create=true",
        // props);
        // Statement stmt = conn.createStatement();
        // ResultSet rs = stmt.executeQuery("from `SYS.SYSTABLES`")) {
        // int count = 0;
        // while (rs.next()) {
        // count++;
        // }
        // Assert.assertEquals(count, tables);
        // }

        try (Connection conn = d.connect("jdbcx:script:ch://" + getClickHouseServer(), props);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("'" + query + "'")) {
            int count = 0;
            while (rs.next()) {
                Assert.assertEquals(count++, rs.getInt(1));
            }
            Assert.assertEquals(count, expected);
        }
    }

    @Test(groups = { "integration" })
    public void testGetDriverProperties() throws Exception {
        Properties props = new Properties();
        WrappedDriver d = new WrappedDriver();

        final String address = "ch://" + getClickHouseServer();
        Driver driver = DriverManager.getDriver("jdbcx:" + address);

        boolean found = false;
        for (DriverPropertyInfo info : driver.getPropertyInfo("jdbc:" + address, props)) {
            if (!info.name.startsWith("jdbcx.")) {
                found = true;
                break;
            }
        }
        Assert.assertTrue(found, "Should have driver-specific properties");
    }

    @Test(groups = { "integration" })
    public void testMultiLanguageQuery() throws Exception {
        Properties props = new Properties();
        WrappedDriver d = new WrappedDriver();

        final String address = getClickHouseServer();
        props.setProperty("jdbcx.web.url.template", "http://" + address + "/${api.path}");
        try (Connection conn = d.connect("jdbcx:ch://" + address, props);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT {{ shell: echo 1 }} as shell, "
                        + "{{ script: 2 }} as script, '{{ web(api.path=ping, result.string.trim=true) }}' as web")) {
            Assert.assertTrue(rs.next(), "Should have at least one row");
            Assert.assertEquals(rs.getInt("shell"), 1);
            Assert.assertEquals(rs.getInt("script"), 2);
            Assert.assertEquals(rs.getString("web"), "Ok.");
            Assert.assertFalse(rs.next(), "Should have only one row");
        }
    }

    @Test(dataProvider = "queryMethod", groups = { "integration" })
    public void testCombinedQueryResult(boolean useExecuteQuery) throws Exception {
        Properties props = new Properties();
        WrappedDriver d = new WrappedDriver();

        final String address = getClickHouseServer();
        try (Connection conn = d.connect("jdbcx:ch://" + address, props);
                Statement stmt = conn.createStatement()) {
            try (ResultSet rs = executeQuery(stmt, "SELECT {{script:[1,2,3]}}", useExecuteQuery)) {
                int counter = 0;
                while (rs.next()) {
                    Assert.assertEquals(rs.getInt(1), ++counter);
                }
                Assert.assertEquals(counter, counter);
            }

            try (ResultSet rs = executeQuery(stmt, "SELECT {{sql: select * from numbers(100)}}", useExecuteQuery)) {
                int counter = 0;
                while (rs.next()) {
                    Assert.assertEquals(rs.getInt(1), counter++);
                }
                Assert.assertEquals(counter, 100);
            }

            try (ResultSet rs = executeQuery(stmt, "SELECT {{script:[1,2,3]}} + {{sql: select * from numbers(100)}}",
                    useExecuteQuery)) {
                int counter = 0;
                while (rs.next()) {
                    int value = 1 + counter % 3;
                    Assert.assertEquals(rs.getInt(1), value + (counter++) / 3);
                }
                Assert.assertEquals(counter, 300);
            }
        }
    }

    @Test(groups = { "integration" })
    public void testDirectQuery() throws Exception {
        Properties props = new Properties();
        WrappedDriver d = new WrappedDriver();

        final String address = getClickHouseServer();
        try (Connection conn = d.connect("jdbcx:derby:memory:x;create=true", props);
                Statement stmt = conn.createStatement()) {
            try (ResultSet rs = stmt
                    .executeQuery("{{sql(url='jdbc:ch://" + address + "'): select * from numbers(7)}}")) {
                int counter = 0;
                while (rs.next()) {
                    Assert.assertEquals(rs.getInt(1), counter++);
                }
                Assert.assertEquals(counter, 7);
            }

            try (ResultSet rs = stmt.executeQuery(" {% vars: ch-server=" + address + " %}\r\n"
                    + "{{sql(url='jdbc:ch://${ch-server}'): select * from numbers(7)}} {% shell: echo 1 %}")) {
                int counter = 0;
                while (rs.next()) {
                    Assert.assertEquals(rs.getInt(1), counter++);
                }
                Assert.assertEquals(counter, 7);
            }

            try (ResultSet rs = stmt.executeQuery("{{shell: curl -s 'http://" + address + "?query=select+7'}}")) {
                Assert.assertTrue(rs.next(), "Should have at least one row");
                Assert.assertEquals(rs.getInt(1), 7);
                Assert.assertFalse(rs.next(), "Should have only one row");
            }

            try (ResultSet rs = stmt.executeQuery("{{shell: echo yes}}")) {
                Assert.assertTrue(rs.next(), "Should have at least one row");
                Assert.assertEquals(rs.getString(1), "yes");
                Assert.assertFalse(rs.next(), "Should have only one row");
            }
        }
    }

    @Test(groups = { "integration" })
    public void testExecTimeout() throws Exception {
        Properties props = new Properties();
        WrappedDriver d = new WrappedDriver();

        final String address = getClickHouseServer();
        try (Connection conn = d.connect("jdbcx:ch://" + address, props); Statement stmt = conn.createStatement()) {
            // final String query = "{{ shell(exec.timeout=1000, exec.parallelism=%d): sleep
            // 3 && echo Yes }}";
            final String query = "{{ script(exec.timeout=1000, exec.parallelism=%d): java.lang.Thread.sleep(3000); 1; }}";
            // final String query = "{{
            // web(exec.dryrun=false,base.url=\"https://www.google.com\", exec.timeout=100,
            // exec.parallelism=%d) }}";
            try (ResultSet rs = stmt.executeQuery(Utils.format(query, 0))) {
                while (rs.next()) {
                    // do nothing
                }
                Assert.fail("Should never success");
            } catch (SQLException e) {
                Assert.assertEquals(e.getCause().getClass(), TimeoutException.class);
            }

            try (ResultSet rs = stmt.executeQuery(Utils.format(query, 1))) {
                while (rs.next()) {
                    // do nothing
                }
                Assert.fail("Should never success");
            } catch (SQLException e) {
                Assert.assertEquals(e.getCause().getClass(), TimeoutException.class);
            }

            try (ResultSet rs = stmt.executeQuery(Utils.format(query, 2))) {
                while (rs.next()) {
                    // do nothing
                }
                Assert.fail("Should never success");
            } catch (SQLException e) {
                Assert.assertEquals(e.getCause().getClass(), TimeoutException.class);
            }
        }
    }

    @Test(groups = { "unit" })
    public void testConnectToLocal() throws Exception {
        Properties props = new Properties();
        WrappedDriver d = new WrappedDriver();

        try (Connection conn = d.connect("jdbcx:", props)) {
            DatabaseMetaData metaData = conn.getMetaData();
            Assert.assertNotNull(metaData);

            try (Statement stmt = conn.createStatement()) {
                Assert.assertTrue(stmt.execute("{{ help }}"));
                ResultSet rs = stmt.getResultSet();
                while (rs.next()) {
                    Assert.assertNotNull(rs.getString(1));
                }
            }
        }
    }

    @Test(groups = { "integration" })
    public void testInspection() throws Exception {
        Properties props = new Properties();
        WrappedDriver d = new WrappedDriver();

        final String address = getClickHouseServer();
        try (Connection conn = d.connect("jdbcx:ch://" + address, props)) {
            DatabaseMetaData metaData = conn.getMetaData();
            Assert.assertNotNull(metaData);
            try (Statement stmt = conn.createStatement()) {
                Assert.assertThrows(SQLException.class, () -> stmt.executeQuery("{{ java }}"));
                try (ResultSet rs = stmt.executeQuery("{{ script }}")) {
                    Assert.assertNotNull(rs.getMetaData());
                }
            }
        }
    }
}
