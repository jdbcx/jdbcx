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
package io.github.jdbcx.extension;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.github.jdbcx.BaseIntegrationTest;
import io.github.jdbcx.WrappedDriver;

public class DbDriverExtensionTest extends BaseIntegrationTest {
    @Test(groups = { "integration" })
    public void testQuery() throws SQLException {
        final String address = getClickHouseServer();
        try (Connection conn = DriverManager.getConnection("jdbcx:db:ch://" + address);
                Statement stmt = conn.createStatement()) {
            Assert.assertThrows(SQLException.class, () -> stmt.executeQuery("select 1"));
            try (ResultSet rs = stmt
                    .executeQuery("select '{{ sql(url='jdbc:ch://${ch.server.address}'): select 2}}'")) {
                Assert.assertTrue(rs.next(), "Should have at least one row");
                Assert.assertEquals(rs.getInt(1), 2);
                Assert.assertFalse(rs.next(), "Should have only one row");
            }
        }

        Properties props = new Properties();
        props.setProperty("jdbcx.db.ch.server.address", address);
        try (Connection conn = DriverManager.getConnection("jdbcx:ch://" + address, props);
                Statement stmt = conn.createStatement()) {
            Assert.assertThrows(SQLException.class, () -> stmt.executeQuery("select {{sql(: select 1}}"));
            try (ResultSet rs = stmt.executeQuery("select {{ sql(url='jdbc:ch://${ch.server.address}'): select 3}}")) {
                Assert.assertTrue(rs.next(), "Should have at least one row");
                Assert.assertEquals(rs.getInt(1), 3);
                Assert.assertFalse(rs.next(), "Should have only one row");
            }
        }
    }

    @Test(groups = { "private" })
    public void testGetTables() throws SQLException {
        Properties props = new Properties();
        WrappedDriver driver = new WrappedDriver();
        try (Connection conn = DriverManager.getConnection("jdbcx:", props)) {
            DatabaseMetaData metaData = conn.getMetaData();
            try (ResultSet rs = metaData.getTables("db", "ch-play", "%", null)) {
                int count = 0;
                while (rs.next()) {
                    Assert.assertNotNull(rs.getString(3));
                    count++;
                }
                Assert.assertTrue(count > 0, "Should have more than one table");
            }
        }
    }

    @Test(groups = { "private" })
    public void testConnectById() throws SQLException {
        Properties props = new Properties();
        WrappedDriver driver = new WrappedDriver();
        String[] queries = new String[] { "{{ db: select 1}}", "select {{ db: select 1}}", "{{ script: 1 }}",
                "select 1", "select {{ script: 1}}" };
        String url = "jdbcx:ch://" + getClickHouseServer();
        try (Connection conn = driver.connect(url, props);
                Statement stmt = conn.createStatement()) {
            for (String query : queries) {
                try (ResultSet rs = stmt.executeQuery(query)) {
                    Assert.assertTrue(rs.next(), "Should have at least one row");
                    Assert.assertEquals(rs.getInt(1), 1);
                    Assert.assertFalse(rs.next(), "Should have only one row");
                }
            }
        }

        try (Connection conn = DriverManager.getConnection("jdbcx:db:ch-play", props);
                Statement stmt = conn.createStatement()) {
            for (String query : queries) {
                try (ResultSet rs = stmt.executeQuery(query)) {
                    Assert.assertTrue(rs.next(), "Should have at least one row");
                    Assert.assertEquals(rs.getInt(1), 1);
                    Assert.assertFalse(rs.next(), "Should have only one row");
                }
            }
        }

        try (Connection conn = DriverManager.getConnection("jdbcx:", props);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt
                        .executeQuery("{{ db(url='jdbc:ch:https://explorer@play.clickhouse.com:443'): select 1 }}")) {
            Assert.assertTrue(rs.next(), "Should have at least one row");
            Assert.assertEquals(rs.getInt(1), 1);
            Assert.assertFalse(rs.next(), "Should have only one row");
        }

        try (Connection conn = DriverManager.getConnection("jdbcx:", props);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("{{ db(id=ch-play): select 1 }}")) {
            Assert.assertTrue(rs.next(), "Should have at least one row");
            Assert.assertEquals(rs.getInt(1), 1);
            Assert.assertFalse(rs.next(), "Should have only one row");
        }

        try (Connection conn = DriverManager.getConnection("jdbcx:", props);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("{{ db.ch-play: select 1 }}")) {
            Assert.assertTrue(rs.next(), "Should have at least one row");
            Assert.assertEquals(rs.getInt(1), 1);
            Assert.assertFalse(rs.next(), "Should have only one row");
        }

        try (Connection conn = DriverManager.getConnection("jdbcx:db:ch-play", props);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("select 1")) {
            Assert.assertTrue(rs.next(), "Should have at least one row");
            Assert.assertEquals(rs.getInt(1), 1);
            Assert.assertFalse(rs.next(), "Should have only one row");
        }
    }
}
