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

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import io.github.jdbcx.executor.Stream;

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

    @DataProvider(name = "databaseTypes")
    public Object[][] getDatabaseTypes() {
        return new Object[][] {
                {
                        "jdbcx:postgresql://" + getPostgreSqlServer() + "/postgres?user=postgres", // url
                        "/postgres/" // prefix
                },
                { "jdbcx:duckdb:", "/duckdb/" },
        };
    }

    @DataProvider(name = "queryMethod")
    public Object[][] getQueryMethod() {
        return new Object[][] {
                { true }, // use executeQuery()
                { false } // use execute()
        };
    }

    @DataProvider(name = "simpleUrlAndQuery")
    public Object[][] getSimpleUrlAndQuery() {
        return new Object[][] {
                { "jdbcx:shell", "echo 1" },
                { "jdbcx:script", "1" },
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

        try (Connection c = d.connect("jdbc:sqlite::memory:", null)) {
            Assert.assertNotNull(c);
        }

        try (Connection c = d.connect("jdbc:sqlite::memory:", null)) {
            Assert.assertNotNull(c);
        }
    }

    @Test(groups = { "unit" })
    public void testVariable() throws SQLException {
        String url = "jdbcx:sqlite::memory:";
        Properties props = new Properties();
        try (Connection conn = DriverManager.getConnection(url, props); Statement stmt = conn.createStatement()) {
            try (ResultSet rs = stmt.executeQuery(
                    "select * from sqlite_schema")) {
                Assert.assertFalse(rs.next());
            }

            try (ResultSet rs = stmt.executeQuery(
                    "{% vars: first=sqlite1, second='schema1' %}select * {{ vars(prefix='x.'): first=sqlite, second=schema }}from ${x.first}_${x.second}")) {
                Assert.assertFalse(rs.next());
            }

            try (ResultSet rs = stmt.executeQuery(
                    "{% vars: m=true %}select '{{ shell(result.string.trim=${m}): echo ${m}}}' a")) {
                while (rs.next()) {
                    Assert.assertEquals(rs.getString(1), Constants.TRUE_EXPR);
                }
            }
        }
    }

    @Test(groups = { "unit" })
    public void testErrorHandling() throws SQLException {
        String url = "jdbcx:script:sqlite::memory:";
        WrappedDriver d = new WrappedDriver();
        Assert.assertNotNull(d.getPropertyInfo(url, null));
        try (Connection conn = d.connect(url, null);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("helper.format('SELECT 1 AS %s, 2 AS %s', 'a', 'b')")) {
            Assert.assertTrue(rs.next(), "Should have at least one row in result");
            Assert.assertEquals(rs.getInt("a"), 1);
            Assert.assertEquals(rs.getInt("b"), 2);
            Assert.assertFalse(rs.next(), "Should have ONLY one row in result");
        }

        try (Connection conn = d.connect(url, null); Statement stmt = conn.createStatement();) {
            Assert.assertThrows(SQLException.class, () -> stmt.executeQuery("helper.invalidMethod()"));
        }

        try (Connection conn = d.connect(url, null);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("select '3'")) {
            Assert.assertTrue(rs.next(), "Should have at least one row in result");
            Assert.assertEquals(rs.getString(1), "3");
            Assert.assertFalse(rs.next(), "Should have ONLY one row in result");
        }

        Properties props = new Properties();
        props.setProperty("jdbcx.script.exec.error", "throw");
        try (Connection conn = d.connect(url, props); Statement stmt = conn.createStatement();) {
            Assert.assertThrows(SQLException.class, () -> stmt.executeQuery("helper.invalidMethod()"));
        }
        try (Connection conn = d.connect(url, props); Statement stmt = conn.createStatement();) {
            Assert.assertThrows(SQLException.class, () -> stmt.executeQuery("select 1"));
        }

        props = new Properties();
        props.setProperty("jdbcx.script.exec.error", "warn");
        try (Connection conn = d.connect(url, props); Statement stmt = conn.createStatement();) {
            Assert.assertThrows(SQLException.class, () -> stmt.executeQuery("helper.invalidMethod()"));
        }

        props = new Properties();
        props.setProperty("jdbcx.shell.exec.error", "warn");
        try (Connection conn = DriverManager.getConnection("jdbcx:shell:sqlite::memory:", props);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("select * from sqlite_schema")) {
            Assert.assertNotNull(stmt.getWarnings(), "Should have SQLWarning");
            Assert.assertFalse(rs.next(), "Should NOT have any result");
        }
        try (Connection conn = d.connect(url, props);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("select 5")) {
            Assert.assertNotNull(stmt.getWarnings(), "Should have SQLWarning");
            Assert.assertTrue(rs.next(), "Should have at least one row in result");
            Assert.assertEquals(rs.getInt(1), 5);
            Assert.assertFalse(rs.next(), "Should have ONLY one row in result");
        }
    }

    @Test(groups = { "integration" })
    public void testConnect() throws SQLException {
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
        // try (Connection conn = d.connect("jdbcx:prql:sqlite::memory:",
        // props);
        // Statement stmt = conn.createStatement();
        // ResultSet rs = stmt.executeQuery("from `sqlite_schema`")) {
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
    public void testGetDriverProperties() throws SQLException {
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
    public void testMultiLanguageQuery() throws SQLException {
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

    @Test(groups = { "integration" })
    public void testVariableTags() throws SQLException {
        Properties props = new Properties();
        WrappedDriver d = new WrappedDriver();

        final String address = getClickHouseServer();
        props.setProperty("jdbcx.tag", "ANGLE_BRACKET");
        props.setProperty("jdbcx.web.url.template", "http://" + address + "/$<api.path>");
        try (Connection conn = d.connect("jdbcx:ch://" + address, props);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("<% var: v={{[[]]}} %>SELECT '<< shell: echo '$<v>' >>' as shell, "
                        + "<< script: 2 >> as script, '<< web(api.path=ping, result.string.trim=true) >>' as web")) {
            Assert.assertTrue(rs.next(), "Should have at least one row");
            Assert.assertEquals(rs.getString("shell"), "{{[[]]}}");
            Assert.assertEquals(rs.getInt("script"), 2);
            Assert.assertEquals(rs.getString("web"), "Ok.");
            Assert.assertFalse(rs.next(), "Should have only one row");
        }

        props.setProperty("jdbcx.tag", "SQUARE_BRACKET");
        props.setProperty("jdbcx.web.url.template", "http://" + address + "/$[api.path]");
        try (Connection conn = d.connect("jdbcx:ch://" + address, props);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("[% var: v={{<<>>}} %]SELECT '[[ shell: echo '$[v]' ]]' as shell, "
                        + "[[ script: 2 ]] as script, '[[ web(api.path=ping, result.string.trim=true) ]]' as web")) {
            Assert.assertTrue(rs.next(), "Should have at least one row");
            Assert.assertEquals(rs.getString("shell"), "{{<<>>}}");
            Assert.assertEquals(rs.getInt("script"), 2);
            Assert.assertEquals(rs.getString("web"), "Ok.");
            Assert.assertFalse(rs.next(), "Should have only one row");
        }

        props.setProperty("jdbcx.tag", "invalid");
        props.setProperty("jdbcx.web.url.template", "http://" + address + "/${api.path}");
        try (Connection conn = d.connect("jdbcx:ch://" + address, props);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("{% var: v=[[<<>>]] %}SELECT '{{ shell: echo '${v}' }}' as shell, "
                        + "{{ script: 2 }} as script, '{{ web(api.path=ping, result.string.trim=true) }}' as web")) {
            Assert.assertTrue(rs.next(), "Should have at least one row");
            Assert.assertEquals(rs.getString("shell"), "[[<<>>]]");
            Assert.assertEquals(rs.getInt("script"), 2);
            Assert.assertEquals(rs.getString("web"), "Ok.");
            Assert.assertFalse(rs.next(), "Should have only one row");
        }
    }

    @Test(dataProvider = "queryMethod", groups = { "integration" })
    public void testCombinedQueryResult(boolean useExecuteQuery) throws SQLException {
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
    public void testDirectQuery() throws SQLException {
        Properties props = new Properties();
        WrappedDriver d = new WrappedDriver();

        final String address = getClickHouseServer();
        try (Connection conn = d.connect("jdbcx:sqlite::memory:", props); Statement stmt = conn.createStatement()) {
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
    public void testMultipleResults() throws SQLException {
        Properties props = new Properties();
        WrappedDriver d = new WrappedDriver();

        final String query = "{{db(url='jdbc:mysql://root@" + getMySqlServer()
                + "?allowMultiQueries=true', result=mergedQueries): select 1; select 2}}";
        try (Connection conn = d.connect("jdbcx:", props); Statement stmt = conn.createStatement()) {
            Assert.assertTrue(stmt.execute(query));

            Assert.assertEquals(stmt.getUpdateCount(), -1);

            try (ResultSet rs = stmt.getResultSet()) {
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getInt(1), 1);
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getInt(1), 2);
                Assert.assertFalse(rs.next());
            }
        }
    }

    @Test(groups = { "integration" })
    public void testExecTimeout() throws SQLException {
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
    public void testConnectToLocal() throws SQLException {
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
    public void testInspection() throws SQLException {
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

    @Test(dataProvider = "simpleUrlAndQuery", groups = { "integration" })
    public void testSimpleUrlAndQuery(String url, String query) throws SQLException {
        Properties props = new Properties();
        WrappedDriver d = new WrappedDriver();

        try (Connection conn = d.connect(url, props);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(query)) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getString(1).trim(), "1", "url=" + url + ", query=" + query);
            Assert.assertFalse(rs.next());
        }
    }

    @Test(dataProvider = "databaseTypes", groups = { "integration" })
    public void testDatabaseTypes(String url, String prefix) throws IOException, SQLException {
        Properties props = new Properties();
        WrappedDriver d = new WrappedDriver();
        try (Connection conn = d.connect(url, props);
                Statement stmt = conn.createStatement()) {
            Assert.assertFalse(stmt.execute("drop table if exists basic_types"));
            // Assert.assertEquals(stmt.getUpdateCount(), 0);
            Assert.assertFalse(stmt.execute(
                    Stream.readAllAsString(getClass().getResourceAsStream(prefix + "create-basic-types.sql"))));
            // Assert.assertEquals(stmt.getUpdateCount(), 0);
            Properties expected = new Properties();
            expected.load(getClass().getResourceAsStream(prefix + "mapped-basic-types.properties"));
            try (Result<ResultSet> rs = Result.of(stmt.executeQuery("select * from basic_types"))) {
                int counter = 0;
                for (Field f : rs.fields()) {
                    counter++;
                    String str = expected.getProperty(f.name());
                    String msg = url + ", " + prefix + ": " + f.toString() + " vs " + str;
                    List<String> value = Utils.split(str, ';');
                    Assert.assertEquals(f.columnType(), value.get(0), msg);
                    Assert.assertEquals(f.type().getVendorTypeNumber(), Integer.parseInt(value.get(1)), msg);
                    Assert.assertEquals(f.precision(), Integer.parseInt(value.get(2)), msg);
                    Assert.assertEquals(f.scale(), Integer.parseInt(value.get(3)), msg);
                    Assert.assertEquals(f.isSigned(), Boolean.parseBoolean(value.get(4)), msg);
                    Assert.assertEquals(f.isNullable(), !f.name().endsWith("serial"), msg);
                }
                Assert.assertEquals(counter, expected.size());
            }
        }
    }

    @Test(groups = { "integration" })
    public void testFederatedQuery() throws IOException, SQLException {
        Properties props = new Properties();
        props.setProperty("jdbcx.base.dir", "target/test-classes/config");
        WrappedDriver d = new WrappedDriver();
        String url = "jdbcx:ch://" + getClickHouseServer() + "/default";
        String query = "select {{db.my-sqlite: select 123}}";
        try (Connection conn = d.connect(url, props);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(query)) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getInt(1), 123);
            Assert.assertFalse(rs.next());
        }

        url = "jdbcx:postgresql://" + getPostgreSqlServer() + "/postgres?user=postgres";
        query = "select {{ db(url=jdbc:duckdb:): select 456 }}";
        try (Connection conn = d.connect(url, props);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(query)) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getInt(1), 456);
            Assert.assertFalse(rs.next());
        }

        query = "select {{ db.my-duckdb: select 789 }} - {{ db.my-duckdb: select 1 }} + {{ db.my-sqlite: select 1 }} + {{db(url=jdbc:ch://"
                + getClickHouseServer() + "): select 210}}" + " * {{ db(url=jdbcx:mysql://root@" + getMySqlServer()
                + "): select 1}}";
        try (Connection conn = d.connect(url, props);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(query)) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getInt(1), 999);
            Assert.assertFalse(rs.next());
        }
    }

    @Test(groups = { "integration" })
    public void testResultVar() throws SQLException {
        Properties props = new Properties();
        WrappedDriver d = new WrappedDriver();
        try (Connection conn = d.connect("jdbcx:", props);
                Statement stmt = conn.createStatement()) {
            try (ResultSet rs = stmt
                    .executeQuery("0,{{ script(result.var=x): 1}},{{ shell(result.var=x): echo 2}},${x}")) {
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getString(1), "0,1,1,1");
                Assert.assertFalse(rs.next());
            }

            try (ResultSet rs = stmt
                    .executeQuery("{%var:x=3%}0,{{ script(result.var=x): 1}},{{ shell(result.var=x): echo 2}},${x}")) {
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getString(1), "0,3,3,3");
                Assert.assertFalse(rs.next());
            }

            // empty result
            try (ResultSet rs = stmt
                    .executeQuery(
                            "0,{{ db(url=jdbc:sqlite::memory:,result.var=x): select name from sqlite_schema where 1=0}},${x},${x.name}")) {
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getString(1), "0,,,${x.name}");
                Assert.assertFalse(rs.next());
            }
            try (ResultSet rs = stmt
                    .executeQuery(
                            "0,{{ db(url=jdbc:sqlite::memory:,result.var=x): select name, type from sqlite_schema where 1=0}},${x},${x.name},${x.type}")) {
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getString(1), "0,,,,");
                Assert.assertFalse(rs.next());
            }

            try (ResultSet rs = stmt
                    .executeQuery("0,{{ db(url=jdbc:sqlite::memory:,result.var=x): select 1 a}},${x},${x.a}")) {
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getString(1), "0,1,1,${x.a}");
                Assert.assertFalse(rs.next());
            }

            try (ResultSet rs = stmt
                    .executeQuery(
                            "0,{{ db(url=jdbc:sqlite::memory:,result.var=x): select 1 A union select 2 union select 3}},${x.A}")) {
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getString(1), "0,1,2,3,${x.A}");
                Assert.assertFalse(rs.next());
            }

            try (ResultSet rs = stmt
                    .executeQuery(
                            "0;{{ db(url=jdbc:sqlite::memory:,result.var=x): select 'a' s, 1 n}};${x.s};${x.n};${x.x}")) {
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getString(1), "0;;a;1;${x.x}");
                Assert.assertFalse(rs.next());
            }

            try (ResultSet rs = stmt
                    .executeQuery(
                            "0;{{ db(url=jdbc:sqlite::memory:,result.var=x): select 'a' s, 1 n union select 'b', 2}};${x.s};${x.n};${x.x}")) {
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getString(1), "0;;a,b;1,2;${x.x}");
                Assert.assertFalse(rs.next());
            }
        }
    }
}
