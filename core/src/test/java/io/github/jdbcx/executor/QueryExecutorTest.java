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
package io.github.jdbcx.executor;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.github.jdbcx.BaseIntegrationTest;
import io.github.jdbcx.Option;
import io.github.jdbcx.executor.jdbc.ReadOnlyResultSet;

public class QueryExecutorTest extends BaseIntegrationTest {
    @Test(groups = { "unit" })
    public void testConstructor() {
        Assert.assertNotNull(new QueryExecutor(null, null));
        Assert.assertNotNull(new QueryExecutor(null, new Properties()));
    }

    @Test(groups = { "integration" })
    public void testPath() throws SQLException {
        Properties props = new Properties();
        final String url = "jdbc:mysql://root@" + getMySqlServer() + "?allowMultiQueries=true";
        final String query = "select 1";
        try (Connection conn = DriverManager.getConnection(url)) {
            Option.INPUT_FILE.setValue(props, "target/test-classes/non-existent.file");
            props.setProperty("path", "target/test-classes/queries/*.non-existent");
            QueryExecutor exec = new QueryExecutor(null, props);
            try (ResultSet rs = new ReadOnlyResultSet(null, exec.execute(query, () -> conn, props))) {
                Assert.assertFalse(rs.next(), "Should not have any result");
            }
        }

        try (Connection conn = DriverManager.getConnection(url)) {
            props.setProperty("path", "target/test-classes/queries/*_7.sql");
            QueryExecutor exec = new QueryExecutor(null, props);
            try (ResultSet rs = new ReadOnlyResultSet(null, exec.execute(query, () -> conn, props))) {
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getString("thread"), Thread.currentThread().getName());
                Assert.assertEquals(rs.getInt("connection"), conn.hashCode());
                Assert.assertTrue(rs.getString("source").endsWith("select_7.sql"));
                Assert.assertEquals(rs.getInt("group"), 1);
                Assert.assertEquals(rs.getString("query"), "");
                Assert.assertEquals(rs.getLong("query_count"), 1L);
                Assert.assertEquals(rs.getLong("update_count"), 0L);
                Assert.assertEquals(rs.getLong("total_operations"), 1L);
                Assert.assertEquals(rs.getLong("affected_rows"), 0L);
                Assert.assertTrue(rs.getLong("elapsed_ms") >= 0L);
                Assert.assertFalse(rs.next());
            }
        }
    }

    @Test(groups = { "integration" })
    public void testInputFile() throws SQLException {
        Properties props = new Properties();
        final String query = "select 1";
        try (Connection conn = DriverManager
                .getConnection("jdbc:mysql://root@" + getMySqlServer() + "?allowMultiQueries=true")) {
            Option.INPUT_FILE.setValue(props, "target/test-classes/non-existent.file");
            Assert.assertThrows(IllegalArgumentException.class,
                    () -> new QueryExecutor(null, props).execute("query", () -> conn, props));

            Option.INPUT_FILE.setValue(props, "target/test-classes/queries/select_7.sql");
            QueryExecutor exec = new QueryExecutor(null, props);
            try (ResultSet rs = new ReadOnlyResultSet(null, exec.execute(query, () -> conn, props))) {
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getString("thread"), Thread.currentThread().getName());
                Assert.assertEquals(rs.getInt("connection"), conn.hashCode());
                Assert.assertTrue(rs.getString("source").endsWith("select_7.sql"));
                Assert.assertEquals(rs.getInt("group"), 1);
                Assert.assertEquals(rs.getString("query"), "");
                Assert.assertEquals(rs.getLong("query_count"), 1L);
                Assert.assertEquals(rs.getLong("update_count"), 0L);
                Assert.assertEquals(rs.getLong("total_operations"), 1L);
                Assert.assertEquals(rs.getLong("affected_rows"), 0L);
                Assert.assertTrue(rs.getLong("elapsed_ms") >= 0L);
                Assert.assertFalse(rs.next());
            }
        }
    }

    @Test(groups = { "integration" })
    public void testInlineQuery() throws SQLException {
        Properties props = new Properties();

        final String query = "--;; my query\nselect 1; select 2 n union select 3 order by 1; select 4 n union select 5 union select 6 order by 1";
        try (Connection conn = DriverManager
                .getConnection("jdbc:mysql://root@" + getMySqlServer() + "?allowMultiQueries=true")) {
            QueryExecutor exec = new QueryExecutor(null, props);
            try (ResultSet rs = new ReadOnlyResultSet(null, exec.execute(query, () -> conn, props))) {
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getString("thread"), Thread.currentThread().getName());
                Assert.assertEquals(rs.getInt("connection"), conn.hashCode());
                Assert.assertTrue(rs.getString("source").endsWith(""));
                Assert.assertEquals(rs.getInt("group"), 1);
                Assert.assertEquals(rs.getString("query"), "my query");
                Assert.assertEquals(rs.getLong("query_count"), 3L);
                Assert.assertEquals(rs.getLong("update_count"), 0L);
                Assert.assertEquals(rs.getLong("total_operations"), 3L);
                Assert.assertEquals(rs.getLong("affected_rows"), 0L);
                Assert.assertTrue(rs.getLong("elapsed_ms") >= 0L);
                Assert.assertFalse(rs.next());
            }
        }
    }

    @Test(groups = { "integration" })
    public void testInlineUpdate() throws SQLException {
        Properties props = new Properties();

        final String query = "--;; my update\ncreate table if not exists test_multi_update_counts(i INTEGER); insert into test_multi_update_counts values(1); "
                + "insert into test_multi_update_counts values(2),(3); insert into test_multi_update_counts values(4),(5),(6)";
        try (Connection conn = DriverManager
                .getConnection("jdbc:mysql://root@" + getMySqlServer() + "/mysql?allowMultiQueries=true")) {
            QueryExecutor exec = new QueryExecutor(null, props);
            try (ResultSet rs = new ReadOnlyResultSet(null, exec.execute(query, () -> conn, props))) {
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getString("thread"), Thread.currentThread().getName());
                Assert.assertEquals(rs.getInt("connection"), conn.hashCode());
                Assert.assertTrue(rs.getString("source").endsWith(""));
                Assert.assertEquals(rs.getInt("group"), 1);
                Assert.assertEquals(rs.getString("query"), "my update");
                Assert.assertEquals(rs.getLong("query_count"), 0L);
                Assert.assertEquals(rs.getLong("update_count"), 4L);
                Assert.assertEquals(rs.getLong("total_operations"), 4L);
                Assert.assertEquals(rs.getLong("affected_rows"), 6L);
                Assert.assertTrue(rs.getLong("elapsed_ms") >= 0L);
                Assert.assertFalse(rs.next());
            }
        }
    }
}
