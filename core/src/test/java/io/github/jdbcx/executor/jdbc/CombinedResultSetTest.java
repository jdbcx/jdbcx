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
package io.github.jdbcx.executor.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import io.github.jdbcx.BaseIntegrationTest;

public class CombinedResultSetTest extends BaseIntegrationTest {
    @DataProvider(name = "numberQueries")
    public Object[][] getNumberQueries() {
        return new Object[][] {
                { "jdbc:ch://" + getClickHouseServer(), "select * from numbers(2)",
                        "select 2 + number from numbers(3)" },
                { "jdbc:duckdb:", "select 0 union select 1 order by 1",
                        "select 2 union select 3 union select 4 order by 1" },
                { "jdbc:arrow-flight-sql://" + getFlightSqlServer()
                        + "?useEncryption=false&user=flight_username&password=f&disableCertificateVerification=true",
                        "select 0 union select 1 union select 2 order by 1", "select 3 union select 4 order by 1" },
                { "jdbc:mariadb://" + getMariaDbServer() + "/sys?user=root",
                        "select 0", "select 1 union select 2 union select 3 union select 4" },
                { "jdbc:mysql://root@" + getMySqlServer() + "/mysql",
                        "select 0", "select 1 union select 2 union select 3 union select 4" },
                { "jdbc:postgresql://" + getPostgreSqlServer() + "/postgres?user=postgres",
                        "select 0 union select 1 union select 2 union select 3 order by 1", "select 4" },
                { "jdbc:sqlite::memory:", "select 0 union select 1 union select 2 order by 1",
                        "select 3 union select 4 order by 1" },
        };
    }

    @Test(groups = { "unit" })
    public void testIsEmpty() throws SQLException {
        Assert.assertTrue(new CombinedResultSet().isEmpty());
        Assert.assertTrue(new CombinedResultSet(new CombinedResultSet()).isEmpty());
        Assert.assertTrue(new CombinedResultSet(new CombinedResultSet(new CombinedResultSet())).isEmpty());

        Assert.assertTrue(new CombinedResultSet((ResultSet) null).isEmpty());
        Assert.assertTrue(new CombinedResultSet((ResultSet) null, null).isEmpty());

        Assert.assertTrue(new CombinedResultSet((ResultSet[]) null).isEmpty());
        Assert.assertTrue(new CombinedResultSet(new ResultSet[0]).isEmpty());
        Assert.assertTrue(new CombinedResultSet(new ResultSet[1]).isEmpty());
        Assert.assertTrue(new CombinedResultSet(new ResultSet[2], null).isEmpty());

        Assert.assertTrue(new CombinedResultSet((Collection<ResultSet>) null).isEmpty());
        Assert.assertTrue(new CombinedResultSet(Collections.emptyList()).isEmpty());
        Assert.assertTrue(new CombinedResultSet(Collections.singletonList((ResultSet) null)).isEmpty());
        Assert.assertTrue(new CombinedResultSet(Arrays.asList((ResultSet) null, null)).isEmpty(), null);

        try (Connection conn = DriverManager.getConnection("jdbc:sqlite::memory:");
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("select 5")) {
            Assert.assertFalse(new CombinedResultSet(rs).isEmpty());
            Assert.assertFalse(new CombinedResultSet(null, rs).isEmpty());
            Assert.assertFalse(new CombinedResultSet(rs, null).isEmpty());
            Assert.assertFalse(new CombinedResultSet(null, rs, null).isEmpty());
            Assert.assertFalse(
                    new CombinedResultSet(null, new CombinedResultSet(Collections.singleton(rs)), null).isEmpty());
        }
    }

    @Test(dataProvider = "numberQueries", groups = { "integration" })
    public void testMultipleResultSets(String url, String q1, String q2) throws SQLException {
        Properties props = new Properties();
        try (Connection conn = DriverManager.getConnection(url, props);
                Statement stmt1 = conn.createStatement();
                Statement stmt2 = conn.createStatement()) {
            try (ResultSet rs = new CombinedResultSet(stmt1.executeQuery(q1), stmt2.executeQuery(q2))) {
                int counter = 0;
                while (rs.next()) {
                    Assert.assertEquals(rs.getInt(1), counter++);
                }
                Assert.assertEquals(counter, 5);
            }
        }
    }
}
