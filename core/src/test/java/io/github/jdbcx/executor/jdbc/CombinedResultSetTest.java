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
package io.github.jdbcx.executor.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.github.jdbcx.BaseIntegrationTest;

public class CombinedResultSetTest extends BaseIntegrationTest {
    @Test(groups = { "integration" })
    public void testMultipleResultSets() throws SQLException {
        String url = "jdbc:ch://" + getClickHouseServer();
        Properties props = new Properties();
        try (Connection conn = DriverManager.getConnection(url, props);
                Statement stmt1 = conn.createStatement();
                Statement stmt2 = conn.createStatement()) {
            try (ResultSet rs = new CombinedResultSet(stmt1.executeQuery("select * from numbers(10)"),
                    stmt2.executeQuery("select 10 + number from numbers(10)"))) {
                int counter = 0;
                while (rs.next()) {
                    Assert.assertEquals(rs.getInt(1), counter++);
                }
                Assert.assertEquals(counter, 20);
            }
        }
    }
}
