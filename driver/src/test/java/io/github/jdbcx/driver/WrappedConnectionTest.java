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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.function.Function;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import io.github.jdbcx.BaseIntegrationTest;
import io.github.jdbcx.WrappedDriver;
import io.github.jdbcx.executor.jdbc.CombinedResultSet;

public class WrappedConnectionTest extends BaseIntegrationTest {
    @DataProvider(name = "testConnections")
    public Object[][] getTestConnections() {
        return new Object[][] {
                { "jdbcx:ch://" + getClickHouseServer(), true },
                { "jdbcx:duckdb:", false },
                { "jdbcx:arrow-flight-sql://" + getFlightSqlServer()
                        + "?useEncryption=false&user=flight_username&password=f&disableCertificateVerification=true",
                        false },
                { "jdbcx:mariadb://" + getMariaDbServer() + "/sys?user=root", false },
                { "jdbcx:mysql://root@" + getMySqlServer() + "/mysql", false },
                { "jdbcx:postgresql://" + getPostgreSqlServer() + "/postgres?user=postgres", false },
                { "jdbcx:sqlite::memory:", false },
        };
    }

    @DataProvider(name = "numberQueries")
    public Object[][] getNumberQueries() {
        return new Object[][] {
                { "jdbcx:ch://" + getClickHouseServer(), "select * from numbers(2)",
                        "select 2 + number from numbers(3)" },
                { "jdbcx:duckdb:", "select 0 union select 1 order by 1",
                        "select 2 union select 3 union select 4 order by 1" },
                { "jdbcx:arrow-flight-sql://" + getFlightSqlServer()
                        + "?useEncryption=false&user=flight_username&password=f&disableCertificateVerification=true",
                        "select 0 union select 1 union select 2 order by 1", "select 3 union select 4 order by 1" },
                { "jdbcx:mariadb://" + getMariaDbServer() + "/sys?user=root",
                        "select 0", "select 1 union select 2 union select 3 union select 4" },
                { "jdbcx:mysql://root@" + getMySqlServer() + "/mysql",
                        "select 0", "select 1 union select 2 union select 3 union select 4" },
                { "jdbcx:postgresql://" + getPostgreSqlServer() + "/postgres?user=postgres",
                        "select 0 union select 1 union select 2 union select 3 order by 1", "select 4" },
                { "jdbcx:sqlite::memory:", "select 0 union select 1 union select 2 order by 1",
                        "select 3 union select 4 order by 1" },
        };
    }

    @Test(groups = { "integration" })
    public void testMultipleSelectQueries() throws SQLException {
        Properties props = new Properties();
        WrappedDriver d = new WrappedDriver();

        final String mysqlUrl = "mysql://root@" + getMySqlServer() + "/mysql?allowMultiQueries=true";
        final String query = "select 1; select 2; select 3";
        try (Connection conn = d.connect("jdbcx:" + mysqlUrl, props); Statement stmt = conn.createStatement()) {
            Assert.assertTrue(stmt.execute(query));
            Assert.assertEquals(stmt.getUpdateCount(), -1);
            Object ref = null;
            try (ResultSet rs = stmt.getResultSet()) {
                ref = rs;
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getInt(1), 1);
                Assert.assertFalse(rs.next());
            }

            Assert.assertTrue(stmt.getMoreResults());
            Assert.assertEquals(stmt.getUpdateCount(), -1);
            try (ResultSet rs = stmt.getResultSet()) {
                Assert.assertFalse(ref == rs, "Should be a different ResultSet instance");
                ref = rs;
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getInt(1), 2);
                Assert.assertFalse(rs.next());
            }

            Assert.assertTrue(stmt.getMoreResults());
            Assert.assertEquals(stmt.getUpdateCount(), -1);
            try (ResultSet rs = stmt.getResultSet()) {
                Assert.assertFalse(ref == rs, "Should be a different ResultSet instance");
                ref = rs;
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getInt(1), 3);
                Assert.assertFalse(rs.next());
            }

            Assert.assertFalse(stmt.getMoreResults());
            Assert.assertEquals(stmt.getUpdateCount(), -1);
        }
    }

    @Test(groups = { "integration" })
    public void testMultipleNonSelectQueries() throws SQLException {
        Properties props = new Properties();
        WrappedDriver d = new WrappedDriver();

        final String mysqlUrl = "mysql://root@" + getMySqlServer() + "/mysql?allowMultiQueries=true";
        final String query = "create table if not exists test_multiple_nonselect_queries(i INTEGER, s VARCHAR(10));"
                + " insert into test_multiple_nonselect_queries values(1, 'a'); insert into test_multiple_nonselect_queries values(2, 'b'), (3, 'c')";
        try (Connection conn = d.connect("jdbcx:" + mysqlUrl, props); Statement stmt = conn.createStatement()) {
            Assert.assertFalse(stmt.execute(query));

            Assert.assertNull(stmt.getResultSet(), "DDL should never return ResultSet");
            Assert.assertEquals(stmt.getUpdateCount(), 0);

            Assert.assertFalse(stmt.getMoreResults());
            Assert.assertNull(stmt.getResultSet(), "INSERT should never return ResultSet");
            Assert.assertEquals(stmt.getUpdateCount(), 1);

            Assert.assertFalse(stmt.getMoreResults());
            Assert.assertNull(stmt.getResultSet(), "INSERT should never return ResultSet");
            Assert.assertEquals(stmt.getUpdateCount(), 2);

            Assert.assertFalse(stmt.getMoreResults());
            Assert.assertEquals(stmt.getUpdateCount(), -1);
        }
    }

    @Test(groups = { "integration" })
    public void testMultipleMixedQueries() throws SQLException {
        Properties props = new Properties();
        WrappedDriver d = new WrappedDriver();

        final String mysqlUrl = "mysql://root@" + getMySqlServer() + "/mysql?allowMultiQueries=true";
        final String query = "create table if not exists test_multiple_mixed_queries(i INTEGER, s VARCHAR(10));"
                + " select 1; insert into test_multiple_mixed_queries values(1, 'a')";
        try (Connection conn = d.connect("jdbcx:" + mysqlUrl, props); Statement stmt = conn.createStatement()) {
            Assert.assertFalse(stmt.execute(query));

            Assert.assertNull(stmt.getResultSet(), "DDL should never return ResultSet");
            Assert.assertEquals(stmt.getUpdateCount(), 0);

            Assert.assertTrue(stmt.getMoreResults());
            Assert.assertEquals(stmt.getUpdateCount(), -1);
            try (ResultSet rs = stmt.getResultSet()) {
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getInt(1), 1);
                Assert.assertFalse(rs.next());
            }

            Assert.assertFalse(stmt.getMoreResults());
            Assert.assertNull(stmt.getResultSet(), "INSERT should never return ResultSet");
            Assert.assertEquals(stmt.getUpdateCount(), 1);

            Assert.assertFalse(stmt.getMoreResults());
            Assert.assertEquals(stmt.getUpdateCount(), -1);
        }
    }

    @Test(groups = { "unit" })
    public void testOrphanStatement() throws SQLException {
        Properties props = new Properties();
        WrappedDriver d = new WrappedDriver();

        try (WrappedConnection conn = (WrappedConnection) d.connect("jdbcx:sqlite::memory:", props)) {
            try (WrappedStatement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery("select 1")) {
                Assert.assertFalse(stmt.isClosed());
                rs.close();
                Assert.assertFalse(stmt.isClosed());
            }

            try (WrappedStatement s = conn.createStatement();
                    Statement stmt = s.newOrphanStatement();
                    ResultSet rs = stmt.executeQuery("select 1")) {
                Assert.assertFalse(stmt.isClosed());
                rs.close();
                Assert.assertTrue(stmt.isClosed());
            }
        }
    }

    @Test(dataProvider = "testConnections", groups = { "integration" })
    public void testMultipleActiveResultSets(String url, boolean support) throws SQLException {
        Properties props = new Properties();
        WrappedDriver d = new WrappedDriver();

        final String query = "select {{ script: [1,2] }} as a";
        final List<Function<Statement, ResultSet>> funcs = new ArrayList<>();
        funcs.add(s -> {
            try {
                return s.executeQuery(query);
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        });
        funcs.add(s -> {
            try {
                Assert.assertTrue(s.execute(query));
                return s.getResultSet();
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        });
        funcs.add(s -> {
            try {
                Assert.assertTrue(s.execute(query, new int[0]));
                return s.getResultSet();
            } catch (UnsupportedOperationException | SQLException e) {
                if (e instanceof UnsupportedOperationException || e instanceof SQLFeatureNotSupportedException
                        || e.getMessage().toLowerCase(Locale.ROOT).contains("supported")) {
                    try {
                        return s.executeQuery(query);
                    } catch (SQLException ex) {
                        throw new IllegalStateException(ex);
                    }
                }

                throw new IllegalStateException(e);
            }
        });
        funcs.add(s -> {
            try {
                Assert.assertTrue(s.execute(query, new int[1]));
                return s.getResultSet();
            } catch (UnsupportedOperationException | SQLException e) {
                if (e instanceof UnsupportedOperationException || e instanceof SQLFeatureNotSupportedException
                        || e.getMessage().toLowerCase(Locale.ROOT).contains("supported")) {
                    try {
                        return s.executeQuery(query);
                    } catch (SQLException ex) {
                        throw new IllegalStateException(ex);
                    }
                }
                throw new IllegalStateException(e);
            }
        });
        funcs.add(s -> {
            try {
                Assert.assertTrue(s.execute(query, new String[0]));
                return s.getResultSet();
            } catch (UnsupportedOperationException | SQLException e) {
                if (e instanceof UnsupportedOperationException || e instanceof SQLFeatureNotSupportedException
                        || e.getMessage().toLowerCase(Locale.ROOT).contains("supported")) {
                    try {
                        return s.executeQuery(query);
                    } catch (SQLException ex) {
                        throw new IllegalStateException(ex);
                    }
                }
                throw new IllegalStateException(e);
            }
        });
        funcs.add(s -> {
            try {
                Assert.assertTrue(s.execute(query, new String[] { "a" }));
                return s.getResultSet();
            } catch (UnsupportedOperationException | SQLException e) {
                if (e instanceof UnsupportedOperationException || e instanceof SQLFeatureNotSupportedException
                        || e.getMessage().toLowerCase(Locale.ROOT).contains("supported")) {
                    try {
                        return s.executeQuery(query);
                    } catch (SQLException ex) {
                        throw new IllegalStateException(ex);
                    }
                }
                throw new IllegalStateException(e);
            }
        });
        try (WrappedConnection conn = (WrappedConnection) d.connect(url, props)) {
            Assert.assertEquals(conn.getDialect().supportMultipleResultSetsPerStatement(), support);

            for (Function<Statement, ResultSet> f : funcs) {
                try (Statement stmt = conn.createStatement(); ResultSet rs = f.apply(stmt)) {
                    Assert.assertFalse(stmt.isClosed());
                    int counter = 0;
                    while (rs.next()) {
                        Assert.assertEquals(rs.getInt(1), ++counter);
                    }
                    Assert.assertEquals(counter, 2);
                    Assert.assertFalse(stmt.isClosed());
                    rs.close();
                    Assert.assertFalse(stmt.isClosed());
                }
            }
        }
    }

    @Test(dataProvider = "numberQueries", groups = { "integration" })
    public void testMultipleResultSets(String url, String q1, String q2) throws SQLException {
        Properties props = new Properties();
        WrappedDriver d = new WrappedDriver();

        try (Connection conn = d.connect(url, props);
                Statement stmt1 = conn.createStatement();
                Statement stmt2 = conn.createStatement()) {
            try (ResultSet rs = new CombinedResultSet(stmt1.executeQuery(q1), stmt2.executeQuery(q2))) {
                int counter = 0;
                Assert.assertNotNull(rs.getMetaData());
                while (rs.next()) {
                    Assert.assertEquals(rs.getInt(1), counter++);
                }
                Assert.assertEquals(counter, 5);
            }
            Assert.assertFalse(stmt1.isClosed());
            Assert.assertFalse(stmt2.isClosed());
        }

        try (WrappedConnection conn = (WrappedConnection) d.connect(url, props);
                WrappedStatement s = conn.createStatement();
                Statement stmt1 = s.newOrphanStatement();
                Statement stmt2 = s.newOrphanStatement()) {
            try (ResultSet rs = new CombinedResultSet(stmt1.executeQuery(q1), stmt2.executeQuery(q2))) {
                int counter = 0;
                Assert.assertNotNull(rs.getMetaData());
                while (rs.next()) {
                    Assert.assertEquals(rs.getInt(1), counter++);
                }
                Assert.assertEquals(counter, 5);
            }
            Assert.assertTrue(stmt1.isClosed());
            Assert.assertTrue(stmt2.isClosed());
        }
    }
}
