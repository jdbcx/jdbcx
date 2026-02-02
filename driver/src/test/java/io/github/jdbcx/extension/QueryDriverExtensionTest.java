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
package io.github.jdbcx.extension;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.github.jdbcx.BaseIntegrationTest;
import io.github.jdbcx.WrappedDriver;

public class QueryDriverExtensionTest extends BaseIntegrationTest {
    @Test(groups = { "integration" })
    public void testPath() throws SQLException {
        Properties props = new Properties();
        WrappedDriver driver = new WrappedDriver();

        final String testQuery = "{{ query(path=target/test-classes/test-queries/sequential/*ddl.*,input.file=omitted): omitted }}";
        try (Connection conn = driver.connect("jdbcx:sqlite::memory:", props);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(testQuery)) {
            Assert.assertTrue(rs.next(), "Should have at least one row");
            final int firstConnectionId = rs.getInt("connection");
            Assert.assertEquals(rs.getString("thread"), Thread.currentThread().getName());
            Assert.assertNotEquals(firstConnectionId, conn.hashCode());
            Assert.assertNotEquals(rs.getString("source"), "");
            Assert.assertEquals(rs.getInt("group"), 1);
            Assert.assertEquals(rs.getString("query"), "create table test_queries_g1t1");
            Assert.assertEquals(rs.getLong("query_count"), 0L);
            Assert.assertEquals(rs.getLong("update_count"), 1L);
            Assert.assertEquals(rs.getLong("total_operations"), 1L);
            Assert.assertEquals(rs.getLong("affected_rows"), 0L);
            Assert.assertTrue(rs.getLong("elapsed_ms") >= 0L);

            Assert.assertTrue(rs.next(), "Should have at least two rows");
            Assert.assertEquals(rs.getString("thread"), Thread.currentThread().getName());
            Assert.assertEquals(rs.getInt("connection"), firstConnectionId);
            Assert.assertNotEquals(rs.getString("source"), "");
            Assert.assertEquals(rs.getInt("group"), 2);
            Assert.assertEquals(rs.getString("query"), "create table test_queries_g1t2");
            Assert.assertEquals(rs.getLong("query_count"), 0L);
            Assert.assertEquals(rs.getLong("update_count"), 1L);
            Assert.assertEquals(rs.getLong("total_operations"), 1L);
            Assert.assertEquals(rs.getLong("affected_rows"), 0L);
            Assert.assertTrue(rs.getLong("elapsed_ms") >= 0L);

            Assert.assertFalse(rs.next(), "Should have only 2 rows");
        }

        for (int i = 0; i < 10; i++) {
            try (Connection conn = driver.connect("jdbcx:sqlite::memory:", props);
                    Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery(
                            "{{ query(path=target/test-classes/test-queries/sequential/??ddl.*,exec.parallelism="
                                    + i + ") }}")) {
                Assert.assertTrue(rs.next(), "Should have at least one row when parallelism=" + i);
                Assert.assertTrue(rs.next(), "Should have at least two rows when parallelism=" + i);
                Assert.assertFalse(rs.next(), "Should have only 2 rows when parallelism=" + i);
            }
        }
    }

    @Test(groups = { "integration" })
    public void testInputFile() throws SQLException {
        Properties props = new Properties();
        WrappedDriver driver = new WrappedDriver();

        final String testQuery = "{{ query(input.file=target/test-classes/test-queries/sequential/1_ddl.sql): inline query is omitted }}";
        try (Connection conn = driver.connect("jdbcx:sqlite::memory:", props);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(testQuery)) {
            Assert.assertTrue(rs.next(), "Should have at least one row");
            final int firstConnectionId = rs.getInt("connection");
            Assert.assertEquals(rs.getString("thread"), Thread.currentThread().getName());
            Assert.assertNotEquals(firstConnectionId, conn.hashCode());
            Assert.assertTrue(rs.getString("source").endsWith("1_ddl.sql"));
            Assert.assertEquals(rs.getInt("group"), 1);
            Assert.assertEquals(rs.getString("query"), "create table test_queries_g1t1");
            Assert.assertEquals(rs.getLong("query_count"), 0L);
            Assert.assertEquals(rs.getLong("update_count"), 1L);
            Assert.assertEquals(rs.getLong("total_operations"), 1L);
            Assert.assertEquals(rs.getLong("affected_rows"), 0L);
            Assert.assertTrue(rs.getLong("elapsed_ms") >= 0L);

            Assert.assertTrue(rs.next(), "Should have at least two rows");
            Assert.assertEquals(rs.getString("thread"), Thread.currentThread().getName());
            Assert.assertEquals(rs.getInt("connection"), firstConnectionId);
            Assert.assertTrue(rs.getString("source").endsWith("1_ddl.sql"));
            Assert.assertEquals(rs.getInt("group"), 2);
            Assert.assertEquals(rs.getString("query"), "create table test_queries_g1t2");
            Assert.assertEquals(rs.getLong("query_count"), 0L);
            Assert.assertEquals(rs.getLong("update_count"), 1L);
            Assert.assertEquals(rs.getLong("total_operations"), 1L);
            Assert.assertEquals(rs.getLong("affected_rows"), 0L);
            Assert.assertTrue(rs.getLong("elapsed_ms") >= 0L);

            Assert.assertFalse(rs.next(), "Should have only 2 rows");
        }

        for (int i = 0; i < 10; i++) {
            try (Connection conn = driver.connect("jdbcx:sqlite::memory:", props);
                    Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery(
                            "{{ query(input.file=target/test-classes/test-queries/sequential/1_ddl.sql,exec.parallelism="
                                    + i + ") }}")) {
                Assert.assertTrue(rs.next(), "Should have at least one row when parallelism=" + i);
                Assert.assertTrue(rs.getString("source").endsWith("1_ddl.sql"));
                Assert.assertTrue(rs.next(), "Should have at least two rows when parallelism=" + i);
                Assert.assertTrue(rs.getString("source").endsWith("1_ddl.sql"));
                Assert.assertFalse(rs.next(), "Should have only 2 rows when parallelism=" + i);
            }
        }
    }

    @Test(groups = { "integration" })
    public void testInlineQuery() throws SQLException {
        Properties props = new Properties();
        WrappedDriver driver = new WrappedDriver();
        try (Connection conn = driver.connect("jdbcx:", props);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("{{ query: test }}")) {
            Assert.assertTrue(rs.next(), "Should have at least one row");
            Assert.assertEquals(rs.getString("thread"), Thread.currentThread().getName());
            Assert.assertNotEquals(rs.getInt("connection"), conn.hashCode());
            Assert.assertEquals(rs.getString("source"), "");
            Assert.assertEquals(rs.getInt("group"), 1);
            Assert.assertEquals(rs.getString("query"), "");
            Assert.assertEquals(rs.getLong("query_count"), 1L);
            Assert.assertEquals(rs.getLong("update_count"), 0L);
            Assert.assertEquals(rs.getLong("total_operations"), 1L);
            Assert.assertEquals(rs.getLong("affected_rows"), 0L);
            Assert.assertTrue(rs.getLong("elapsed_ms") >= 0L);
            Assert.assertFalse(rs.next(), "Should have only one row");
        }

        final String multiStatementQuery = "--;; 1st query\nselect 1\n--;; 2nd query \nselect 2\n"
                + "--;;  1st update \ncreate table a(b String) engine=Memory;\ninsert into a values('x'),('y')";
        try (Connection conn = driver.connect("jdbcx:ch://" + getClickHouseServer(), props);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "{{ query:\n" + multiStatementQuery + " }}")) {
            Assert.assertTrue(rs.next(), "Should have at least one row");
            final int firstConnectionId = rs.getInt("connection");
            Assert.assertEquals(rs.getString("thread"), Thread.currentThread().getName());
            Assert.assertNotEquals(firstConnectionId, conn.hashCode());
            Assert.assertEquals(rs.getString("source"), "");
            Assert.assertEquals(rs.getInt("group"), 1);
            Assert.assertEquals(rs.getString("query"), "1st query");
            Assert.assertEquals(rs.getLong("query_count"), 1L);
            Assert.assertEquals(rs.getLong("update_count"), 0L);
            Assert.assertEquals(rs.getLong("total_operations"), 1L);
            Assert.assertEquals(rs.getLong("affected_rows"), 0L);
            Assert.assertTrue(rs.getLong("elapsed_ms") >= 0L);

            Assert.assertTrue(rs.next(), "Should have at least two rows");
            Assert.assertEquals(rs.getString("thread"), Thread.currentThread().getName());
            Assert.assertEquals(rs.getInt("connection"), firstConnectionId);
            Assert.assertEquals(rs.getString("source"), "");
            Assert.assertEquals(rs.getInt("group"), 2);
            Assert.assertEquals(rs.getString("query"), "2nd query");
            Assert.assertEquals(rs.getLong("query_count"), 1L);
            Assert.assertEquals(rs.getLong("update_count"), 0L);
            Assert.assertEquals(rs.getLong("total_operations"), 1L);
            Assert.assertEquals(rs.getLong("affected_rows"), 0L);
            Assert.assertTrue(rs.getLong("elapsed_ms") >= 0L);

            Assert.assertTrue(rs.next(), "Should have at least three rows");
            Assert.assertEquals(rs.getString("thread"), Thread.currentThread().getName());
            Assert.assertEquals(rs.getInt("connection"), firstConnectionId);
            Assert.assertEquals(rs.getString("source"), "");
            Assert.assertEquals(rs.getInt("group"), 3);
            Assert.assertEquals(rs.getString("query"), "1st update");
            Assert.assertEquals(rs.getLong("query_count"), 0L);
            Assert.assertEquals(rs.getLong("update_count"), 1L);
            Assert.assertEquals(rs.getLong("total_operations"), 1L);
            Assert.assertEquals(rs.getLong("affected_rows"), 2L);
            Assert.assertTrue(rs.getLong("elapsed_ms") >= 0L);

            Assert.assertFalse(rs.next(), "Should have only 3 rows");
        }
    }

    @Test(groups = { "integration" })
    public void testDefaultQuery() throws SQLException {
        Properties props = new Properties();
        props.setProperty("jdbcx.base.dir", "target/test-classes/config");
        WrappedDriver driver = new WrappedDriver();
        try (Connection conn = driver.connect("jdbcx:ch://" + getClickHouseServer(), props)) {
            try (Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("{{ query.insert3rows }}")) {
                Assert.assertTrue(rs.next(), "Should have at least one row");
                Assert.assertTrue(rs.next(), "Should have at least two rows");
                Assert.assertFalse(rs.next(), "Should have only two rows");
            }

            try (Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("select * from abc_test order by s")) {
                Assert.assertTrue(rs.next(), "Should have at least one row");
                Assert.assertEquals(rs.getString(1), "1");
                Assert.assertTrue(rs.next(), "Should have at least two rows");
                Assert.assertEquals(rs.getString(1), "2");
                Assert.assertTrue(rs.next(), "Should have at least three rows");
                Assert.assertEquals(rs.getString(1), "3");
                Assert.assertFalse(rs.next(), "Should have only three rows");
            }
        }

        try (Connection conn = driver.connect("jdbcx:ch://" + getClickHouseServer(), props)) {
            try (Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("{{ query.insert3rows: truncate table abc_test }}")) {
                Assert.assertTrue(rs.next(), "Should have at least one row");
                Assert.assertFalse(rs.next(), "Should have only one row");
            }

            try (Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("select * from abc_test order by s")) {
                Assert.assertFalse(rs.next(), "Should have no row");
            }
        }
    }
}
