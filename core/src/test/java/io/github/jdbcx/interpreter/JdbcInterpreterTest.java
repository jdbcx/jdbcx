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
import java.sql.Statement;
import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.clickhouse.jdbc.ClickHouseDriver;

import io.github.jdbcx.BaseIntegrationTest;
import io.github.jdbcx.Utils;

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

    @Test(groups = { "unit" })
    public void testNormalizeName() {
        Assert.assertEquals(JdbcInterpreter.normalizedName(null, ""), "");
        Assert.assertEquals(JdbcInterpreter.normalizedName("", ""), "");
        Assert.assertEquals(JdbcInterpreter.normalizedName(null, "`"), "");
        Assert.assertEquals(JdbcInterpreter.normalizedName("", "`"), "");

        Assert.assertEquals(JdbcInterpreter.normalizedName("`a`", "`"), "a");
        Assert.assertEquals(JdbcInterpreter.normalizedName("\"a\"", "`"), "\"a\"");
        Assert.assertEquals(JdbcInterpreter.normalizedName("\"a\"", "`\""), "a");
    }

    @Test(groups = { "integration" })
    public void testGetDatabaseProduct() throws SQLException {
        try (Connection conn = DriverManager
                .getConnection("jdbc:mysql://root@" + getMySqlServer() + "?allowMultiQueries=true")) {
            Assert.assertTrue(JdbcInterpreter.getDatabaseProduct(conn.getMetaData()).contains("MySQL "));
        }
    }

    @Test(groups = { "integration" })
    public void testGetCurrentDatabaseCatalogAndSchema() throws SQLException {
        Properties props = new Properties();
        try (Connection conn = DriverManager.getConnection("jdbc:ch://" + getClickHouseServer(), props)) {
            String json = JdbcInterpreter.getCurrentDatabaseCatalogAndSchema(conn, conn.getMetaData());
            Assert.assertTrue(json.startsWith("\"currentDatabase\""), "Should start with currentDatabase");
        }

        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
            String json = JdbcInterpreter.getCurrentDatabaseCatalogAndSchema(conn, conn.getMetaData());
            Assert.assertTrue(json.startsWith("\"currentCatalog\""), "Should start with currentCatalog");
            Assert.assertTrue(json.indexOf("\"currentSchema\"") > 0, "Should has currentSchema");
        }

        try (Connection conn = DriverManager.getConnection("jdbc:sqlite::memory:")) {
            String json = JdbcInterpreter.getCurrentDatabaseCatalogAndSchema(conn, conn.getMetaData());
            Assert.assertEquals(json, "", "Should have no current catalog and schema");
        }

        try (Connection conn = DriverManager
                .getConnection("jdbc:mysql://root@" + getMySqlServer() + "?allowMultiQueries=true")) {
            String json = JdbcInterpreter.getCurrentDatabaseCatalogAndSchema(conn, conn.getMetaData());
            Assert.assertEquals(json, "", "Should have no current catalog and schema");
        }

        try (Connection conn = DriverManager
                .getConnection(
                        "jdbc:mysql://root@" + getMySqlServer() + "/information_schema?allowMultiQueries=true")) {
            String json = JdbcInterpreter.getCurrentDatabaseCatalogAndSchema(conn, conn.getMetaData());
            Assert.assertEquals(json, "\"currentDatabase\":\"information_schema\"", "Should have current database");
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
            Assert.assertFalse(schema.contains("\"schemas\""), "Should NOT contain schemas");
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

    @Test(groups = { "integration" })
    public void testGetDatabaseTable() throws SQLException {
        final String table = UUID.randomUUID().toString();
        Properties props = new Properties();
        try (Connection conn = DriverManager.getConnection("jdbc:ch://" + getClickHouseServer(), props);
                Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("create table if not exists `" + table
                    + "`(a UInt32, b FixedString(3), c Nullable(String), d Decimal(18,4) DEFAULT 5.12345, e DateTime64(3, 'UTC'), f Array(Decimal(18,4))) "
                    + "engine=MergeTree primary key a order by (a,b); "
                    + "ALTER TABLE `" + table + "` ADD INDEX my_index_name (a) TYPE minmax GRANULARITY 1;");
            Assert.assertEquals(
                    JdbcInterpreter.getDatabaseTable(conn.getMetaData(), Utils.getCatalogName(conn),
                            Utils.getSchemaName(conn), table),
                    "\"database\":\"default\",\"table\":\"" + table + "\",\"columns\":[{"
                            + "\"name\":\"a\",\"type\":\"UInt32\",\"nullable\":false},{\"name\":\"b\",\"type\":\"FixedString(3)\",\"nullable\":false},"
                            + "{\"name\":\"c\",\"type\":\"Nullable(String)\",\"nullable\":true},{\"name\":\"d\",\"type\":\"Decimal(18, 4)\",\"nullable\":false,\"default\":\"5.12345\"},"
                            + "{\"name\":\"e\",\"type\":\"DateTime64(3, \\u0027UTC\\u0027)\",\"nullable\":false},{\"name\":\"f\",\"type\":\"Array(Decimal(18, 4))\",\"nullable\":false}],"
                            + "\"indexes\":[{\"name\":\"my_index_name\",\"columns\":[\"a\"]}]");
            Assert.assertEquals(JdbcInterpreter.getDatabaseTable(conn, table),
                    JdbcInterpreter.getDatabaseTable(conn.getMetaData(), Utils.getCatalogName(conn),
                            Utils.getSchemaName(conn), table));
        }

        try (Connection conn = DriverManager
                .getConnection("jdbc:mysql://root@" + getMySqlServer()
                        + "/test?createDatabaseIfNotExist=true&allowMultiQueries=true");
                Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("create table if not exists `" + table
                    + "` (a INTEGER NOT NULL, b CHAR(2) NOT NULL, c VARCHAR(3) DEFAULT 'xyz', d DECIMAL(18,4) DEFAULT 5.12345, e TIMESTAMP(6) NULL, f BLOB,"
                    + "PRIMARY KEY (a,b), UNIQUE INDEX i1(a), INDEX i2(a,c)) ENGINE=InnoDB");
            Assert.assertEquals(
                    JdbcInterpreter.getDatabaseTable(conn.getMetaData(), Utils.getCatalogName(conn),
                            Utils.getSchemaName(conn), table),
                    "\"database\":\"test\",\"table\":\"" + table + "\",\"columns\":[{"
                            + "\"name\":\"a\",\"type\":\"INT\",\"nullable\":false},{\"name\":\"b\",\"type\":\"CHAR\",\"size\":2,\"nullable\":false},"
                            + "{\"name\":\"c\",\"type\":\"VARCHAR\",\"size\":3,\"nullable\":true,\"default\":\"xyz\"},"
                            + "{\"name\":\"d\",\"type\":\"DECIMAL\",\"precision\":18,\"scale\":4,\"nullable\":true,\"default\":\"5.1235\"},"
                            + "{\"name\":\"e\",\"type\":\"TIMESTAMP\",\"nullable\":true},{\"name\":\"f\",\"type\":\"BLOB\",\"nullable\":true}],"
                            + "\"primaryKey\":[\"a\",\"b\"],\"indexes\":[{\"name\":\"i1\",\"columns\":[\"a\"]},{\"name\":\"PRIMARY\",\"columns\":["
                            + "\"a\",\"b\"]},{\"name\":\"i2\",\"columns\":[\"a\",\"c\"]}]");
            Assert.assertEquals(
                    JdbcInterpreter.getDatabaseTable(conn, "`test`.`" + table + "`"),
                    JdbcInterpreter.getDatabaseTable(conn.getMetaData(), Utils.getCatalogName(conn),
                            Utils.getSchemaName(conn), table));
        }
    }

    @Test(groups = { "integration" })
    public void testGetFullQualifiedTable() throws SQLException {
        final String table = UUID.randomUUID().toString();
        Properties props = new Properties();
        try (Connection conn = DriverManager.getConnection("jdbc:ch://" + getClickHouseServer(), props);
                Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(
                    "create table if not exists `" + table + "`(a UInt32, b String) engine=MergeTree order by a");
            Assert.assertEquals(JdbcInterpreter.getFullQualifiedTable(conn, table),
                    Arrays.asList(Utils.getCatalogName(conn), Utils.getSchemaName(conn), table));
            Assert.assertEquals(JdbcInterpreter.getFullQualifiedTable(conn, "default.`" + table + "`"),
                    Arrays.asList("default", Utils.getSchemaName(conn), table)); // bug in ClickHouse JDBC driver
        }

        try (Connection conn = DriverManager
                .getConnection("jdbc:mysql://root@" + getMySqlServer()
                        + "/test?createDatabaseIfNotExist=true&allowMultiQueries=true");
                Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("create database if not exists test1; create table if not exists test1.`" + table
                    + "` (a INTEGER NOT NULL, b CHAR(1) NOT NULL, c VARCHAR(30) NULL, PRIMARY KEY (a,b)) ENGINE=InnoDB");
            Assert.assertEquals(JdbcInterpreter.getFullQualifiedTable(conn, " `test1` . `" + table + "`"),
                    Arrays.asList("test1", Utils.getSchemaName(conn), table));
        }
    }
}
