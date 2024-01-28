/*
 * Copyright 2022-2024, Zhichun Wu
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
package io.github.jdbcx.extension;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.github.jdbcx.BaseIntegrationTest;

public class ScriptDriverExtensionTest extends BaseIntegrationTest {
    @Test(groups = { "integration" })
    public void testQuery() throws SQLException {
        final String query = "select '{{ script: 10 + 2 }}'";
        final String address = getClickHouseServer();
        Properties props = new Properties();
        try (Connection conn = DriverManager.getConnection("jdbcx:ch://" + address, props);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(query)) {
            Assert.assertTrue(rs.next(), "Should have at least one row");
            Assert.assertEquals(rs.getString(1), "12");
            Assert.assertFalse(rs.next(), "Should have only one row");
        }

        try (Connection conn = DriverManager.getConnection("jdbcx:script:ch://" + address, props);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("helper.format('select %s', 10 + 2)")) {
            Assert.assertTrue(rs.next(), "Should have at least one row");
            Assert.assertEquals(rs.getString(1), "12");
            Assert.assertFalse(rs.next(), "Should have only one row");
        }
    }
}
