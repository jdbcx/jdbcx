/*
 * Copyright 2022-2026, Zhichun Wu
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
package io.github.jdbcx.driver.impl;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.github.jdbcx.WrappedDriver;

public class DefaultConnectionTest {
    @Test(groups = { "unit" })
    public void testDefaultOutput() throws SQLException {
        Properties props = new Properties();
        WrappedDriver d = new WrappedDriver();

        try (Connection conn = d.connect("jdbcx:help", props)) {
            Assert.assertEquals(conn.getClass(), DefaultConnection.class);

            try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery("ls -alF")) {
                while (rs.next()) {
                    Assert.assertNotNull(rs.getString(1));
                }
            }
        }
    }

    @Test(groups = { "unit" })
    public void testChangeCatalog() throws SQLException {
        Properties props = new Properties();
        props.setProperty("jdbcx.exec.error", "warn");
        WrappedDriver d = new WrappedDriver();

        try (Connection conn = d.connect("jdbcx:", props)) {
            Assert.assertEquals(conn.getClass(), DefaultConnection.class);
            Assert.assertEquals(conn.getCatalog(), "default");

            try (Statement stmt = conn.createStatement()) {
                Assert.assertTrue(stmt.execute("select 1"));
                Assert.assertNull(conn.getWarnings(), "Should have no warning in Connection");
                Assert.assertNull(stmt.getWarnings(), "Should have no warning in Statement");
                try (ResultSet rs = stmt.getResultSet()) {
                    Assert.assertTrue(rs.next(), "Should have at least one row");
                    Assert.assertEquals(rs.getString(1), "select 1");
                    Assert.assertFalse(rs.next(), "Should have only one row");
                }
            }

            conn.setCatalog("script");
            Assert.assertEquals(conn.getCatalog(), "script");

            try (Statement stmt = conn.createStatement()) {
                Assert.assertTrue(stmt.execute("select 1"));
                Assert.assertNull(conn.getWarnings(), "Should have no warning in Connection");
                Assert.assertNotNull(stmt.getWarnings(), "Should have warning in Statement");
            }
        }
    }
}
