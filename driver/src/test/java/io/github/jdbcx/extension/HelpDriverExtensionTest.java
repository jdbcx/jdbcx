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

import io.github.jdbcx.WrappedDriver;

public class HelpDriverExtensionTest {
    @Test(groups = { "unit" })
    public void testQuery() throws SQLException {
        WrappedDriver d = new WrappedDriver();
        Properties props = new Properties();
        try (Connection conn = DriverManager.getConnection("jdbcx:prql:sqlite::memory:", props);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("{{ help }}")) {
            Assert.assertTrue(rs.next(), "Should have at least one row");
            Assert.assertTrue(rs.getString(1).length() > 0, "Should have content");
        }
    }
}
