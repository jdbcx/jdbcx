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
package io.github.jdbcx.extension;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
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
import io.github.jdbcx.Compression;
import io.github.jdbcx.Format;
import io.github.jdbcx.Utils;
import io.github.jdbcx.WrappedDriver;
import io.github.jdbcx.executor.Stream;

public class DbDriverExtensionTest extends BaseIntegrationTest {
    @Test(groups = { "integration" })
    public void testQuery() throws SQLException {
        Properties props = new Properties();
        props.setProperty("jdbcx.db.exec.error", "warn");
        final String address = getClickHouseServer();
        try (Connection conn = DriverManager.getConnection("jdbcx:db:ch://" + address, props);
                Statement stmt = conn.createStatement()) {
            try (ResultSet rs = stmt.executeQuery("select 1")) {
                Assert.assertTrue(rs.next(), "Should have at least one row");
                Assert.assertEquals(rs.getInt(1), 1);
                Assert.assertFalse(rs.next(), "Should have only one row");
            }

            try (ResultSet rs = stmt
                    .executeQuery(
                            "select '{{ db(url='jdbc:ch://${ch.server.address}'): select ''${_}:${_.url}'', 2}}'")) {
                Assert.assertNotNull(stmt.getWarnings());
                Assert.assertTrue(rs.next(), "Should have at least one row");
                Assert.assertEquals(rs.getString(1), "select 'db:jdbc:ch://${ch.server.address}', 2");
                Assert.assertFalse(rs.next(), "Should have only one row");
            }
        }

        props = new Properties();
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

    @Test(groups = { "integration" })
    public void testConnectToExtension() throws SQLException {
        Properties props = new Properties();
        WrappedDriver driver = new WrappedDriver();
        try (Connection conn = DriverManager.getConnection("jdbcx:db", props);
                Statement stmt = conn.createStatement()) {
            // single value
            for (String query : new String[] { "select {{ db: select 1}}", "select select 1",
                    "{{ db: select select 1}}", "select {%var:a=1%}select ${a}",
                    "select {%var:a=1%}{{db: select ${a}}}" }) {
                try (ResultSet rs = stmt.executeQuery(query)) {
                    Assert.assertTrue(rs.next(), "Should have at least one row");
                    Assert.assertEquals(rs.getString(1), "select select 1");
                    Assert.assertFalse(rs.next(), "Should have only one row");
                }
            }

            // multiple values
            for (String query : new String[] { "{%var:a=1%}select {{ db: ${a} }}{{ script: [${a}, 2]}}",
                    "select {{script:[11,12]}}" }) {
                try (ResultSet rs = stmt.executeQuery(query)) {
                    Assert.assertTrue(rs.next(), "Should have at least one row");
                    Assert.assertEquals(rs.getString(1), "select 11");
                    Assert.assertTrue(rs.next(), "Should have at least two rows");
                    Assert.assertEquals(rs.getString(1), "select 12");
                    Assert.assertFalse(rs.next(), "Should have only two rows");
                }
            }
        }
    }

    @Test(groups = { "integration" })
    public void testConnectById() throws SQLException {
        Properties props = new Properties();
        props.setProperty("jdbcx.base.dir", "target/test-classes/config");
        WrappedDriver driver = new WrappedDriver();
        String url = "jdbcx:ch://" + getClickHouseServer();
        String[] queries = new String[] { "{%var:a=1%}select 0+${a}", "select 1", "{{ db: select 1}}",
                "select {{ db: select 1}}", "{{ script: 1 }}", "select {{ script: 1}}" };
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

        try (Connection conn = driver.connect("jdbcx:db:my-duckdb", props);
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
                        .executeQuery("{{ db(url='jdbc:ch://" + getClickHouseServer() + "/default'): select 1 }}")) {
            Assert.assertTrue(rs.next(), "Should have at least one row");
            Assert.assertEquals(rs.getInt(1), 1);
            Assert.assertFalse(rs.next(), "Should have only one row");
        }

        try (Connection conn = DriverManager.getConnection("jdbcx:", props);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("{{ db(id=my-sqlite): select 1 }}")) {
            Assert.assertTrue(rs.next(), "Should have at least one row");
            Assert.assertEquals(rs.getInt(1), 1);
            Assert.assertFalse(rs.next(), "Should have only one row");
        }

        try (Connection conn = DriverManager.getConnection("jdbcx:", props);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("{{ db.my-sqlite: select 1 }}")) {
            Assert.assertTrue(rs.next(), "Should have at least one row");
            Assert.assertEquals(rs.getInt(1), 1);
            Assert.assertFalse(rs.next(), "Should have only one row");
        }

        try (Connection conn = DriverManager.getConnection("jdbcx:db:my-sqlite", props);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("select 1")) {
            Assert.assertTrue(rs.next(), "Should have at least one row");
            Assert.assertEquals(rs.getInt(1), 1);
            Assert.assertFalse(rs.next(), "Should have only one row");
        }
    }

    @Test(groups = { "integration" })
    public void testWriteOutputFile() throws IOException, SQLException {
        Properties props = new Properties();
        props.setProperty("jdbcx.base.dir", "target/test-classes/config");
        WrappedDriver driver = new WrappedDriver();
        final String url = "jdbcx:ch://" + getClickHouseServer();
        final String file = Utils.createTempFile() + ".csv.gz";
        try (Connection conn = driver.connect(url, props); Statement stmt = conn.createStatement()) {
            try (ResultSet rs = stmt
                    .executeQuery(Utils.format("{{ db(output.file=%s): select 1}}", file))) {
                Assert.assertTrue(rs.next(), "Should have one result");
                Assert.assertEquals(rs.getString("file"), file);
                Assert.assertEquals(rs.getString("format"), Format.CSV.name());
                Assert.assertEquals(rs.getString("compress_algorithm"), Compression.GZIP.name());
                Assert.assertEquals(rs.getInt("compress_level"), -1);
                Assert.assertEquals(rs.getInt("compress_buffer"), 0);
                Assert.assertEquals(rs.getLong("size"), Files.size(new File(file).toPath()));
                Assert.assertFalse(rs.next(), "Should have only one result");
            }
            Assert.assertEquals(
                    Stream.readAllAsString(
                            Compression.getProvider(Compression.GZIP).decompress(new FileInputStream(file), -1, 0)),
                    "1\n1");
        }
    }
}
