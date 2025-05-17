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
package io.github.jdbcx.interpreter;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.clickhouse.jdbc.ClickHouseDriver;

import io.github.jdbcx.BaseIntegrationTest;

public class JdbcInterpreterTest extends BaseIntegrationTest {
    @Test(groups = { "unit" })
    public void testGetDriverByClass() throws SQLException {
        Assert.assertThrows(SQLException.class, () -> JdbcInterpreter.getDriverByClass(null, null));
        Assert.assertThrows(SQLException.class, () -> JdbcInterpreter.getDriverByClass("some.unknown.Driver", null));
        Assert.assertThrows(SQLException.class,
                () -> JdbcInterpreter.getDriverByClass(null, getClass().getClassLoader()));

        Assert.assertNotNull(JdbcInterpreter.getDriverByClass(ClickHouseDriver.class.getName(), null),
                "Should have driver");
        Assert.assertNotNull(
                JdbcInterpreter.getDriverByClass(ClickHouseDriver.class.getName(), getClass().getClassLoader()),
                "Should have driver");
    }

    @Test(groups = { "unit" })
    public void testGetDriverByUrl() throws SQLException {
        Assert.assertThrows(SQLException.class, () -> JdbcInterpreter.getDriverByUrl("", null));
        Assert.assertThrows(SQLException.class, () -> JdbcInterpreter.getDriverByUrl("jdbc:unknown:driver", null));
        Assert.assertThrows(SQLException.class,
                () -> JdbcInterpreter.getDriverByUrl("", getClass().getClassLoader()));

        Assert.assertNotNull(JdbcInterpreter.getDriverByUrl("jdbc:ch://localhost", null), "Should have driver");
        Assert.assertNotNull(JdbcInterpreter.getDriverByUrl("jdbc:ch://localhost", getClass().getClassLoader()),
                "Should have driver");
    }

    @Test(groups = { "integration" })
    public void testGetDatabaseProduct() throws SQLException {
        try (Connection conn = DriverManager
                .getConnection("jdbc:mysql://root@" + getMySqlServer() + "?allowMultiQueries=true")) {
            Assert.assertTrue(JdbcInterpreter.getDatabaseProduct(conn.getMetaData()).contains("MySQL "));
        }
    }

    @Test(groups = { "integration" })
    public void testGetDatabaseCatalogs() throws SQLException {
        Properties props = new Properties();
        try (Connection conn = DriverManager.getConnection("jdbc:ch://" + getClickHouseServer(), props)) {
            String schema = JdbcInterpreter.getDatabaseCatalogs(conn.getMetaData(), "local-ch", null, null);
            Assert.assertTrue(schema.startsWith("\"databases\""), "Should start with catalogs");
        }

        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
            String schema = JdbcInterpreter.getDatabaseCatalogs(conn.getMetaData(), "local-duckdb", null, null);
            Assert.assertTrue(schema.startsWith("\"catalogs\""), "Should start with catalogs");
            Assert.assertTrue(schema.contains("\"schemas\""), "Should contain schemas");
        }

        try (Connection conn = DriverManager.getConnection("jdbc:sqlite::memory:")) {
            String schema = JdbcInterpreter.getDatabaseCatalogs(conn.getMetaData(), "local-sqlite", null, null);
            Assert.assertTrue(schema.startsWith("\"tables\""), "Should start with tables");
        }

        try (Connection conn = DriverManager
                .getConnection("jdbc:mysql://root@" + getMySqlServer() + "?allowMultiQueries=true")) {
            String schema = JdbcInterpreter.getDatabaseCatalogs(conn.getMetaData(), "local-mysql", null, null);
            Assert.assertTrue(schema.startsWith("\"databases\""), "Should start with databases");
        }
    }
}
