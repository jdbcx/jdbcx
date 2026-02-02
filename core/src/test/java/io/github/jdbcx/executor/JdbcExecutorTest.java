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
package io.github.jdbcx.executor;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.github.jdbcx.BaseIntegrationTest;
import io.github.jdbcx.Option;

public class JdbcExecutorTest extends BaseIntegrationTest {
    @Test(groups = { "unit" })
    public void testConstructor() {
        Assert.assertNotNull(new JdbcExecutor(null, null));
        Assert.assertNotNull(new JdbcExecutor(null, new Properties()));
    }

    @Test(groups = { "integration" })
    public void testInputFile() throws SQLException {
        Properties props = new Properties();
        final String query = "select 1";
        try (Connection conn = DriverManager
                .getConnection("jdbc:mysql://root@" + getMySqlServer() + "?allowMultiQueries=true")) {
            Option.INPUT_FILE.setValue(props, "target/test-classes/non-existent.file");
            Assert.assertThrows(IllegalArgumentException.class,
                    () -> new JdbcExecutor(null, props).execute("query", conn, props));

            Option.INPUT_FILE.setValue(props, "target/test-classes/queries/select_7.sql");
            JdbcExecutor exec = new JdbcExecutor(null, props);
            try (ResultSet rs = (ResultSet) exec.execute(query, conn, props)) {
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getInt(1), 7);
                Assert.assertFalse(rs.next());
            }
        }
    }

    @Test(groups = { "integration" })
    public void testGetFirstResult() throws SQLException {
        try (Connection conn = DriverManager
                .getConnection("jdbc:mysql://root@" + getMySqlServer() + "/mysql?allowMultiQueries=true")) {
            long[] stats = new long[] { 0L, 0L, 0L };
            try (Statement stmt = conn.createStatement()) {
                Object obj = JdbcExecutor.getFirstResult(stmt, "/* nothing to execute */", stats);
                Assert.assertEquals(stats, new long[] { 0L, 1L, 0L });
                Assert.assertEquals(obj, Long.valueOf(0L));
            }

            stats = new long[] { 0L, 0L, 0L };
            try (Statement stmt = conn.createStatement()) {
                Object obj = JdbcExecutor.getFirstResult(stmt, "drop table if exists get_first_result", stats);
                Assert.assertEquals(stats, new long[] { 0L, 1L, 0L });
                Assert.assertEquals(obj, Long.valueOf(0L));
            }

            stats = new long[] { 0L, 0L, 0L };
            try (Statement stmt = conn.createStatement()) {
                Object obj = JdbcExecutor.getFirstResult(stmt, "drop table if exists get_first_result;select 1", stats);
                Assert.assertEquals(stats, new long[] { 0L, 1L, 0L });
                Assert.assertEquals(obj, Long.valueOf(0L));
            }

            stats = new long[] { 0L, 0L, 0L };
            try (Statement stmt = conn.createStatement()) {
                Object obj = JdbcExecutor.getFirstResult(stmt, "select 2", stats);
                Assert.assertEquals(stats, new long[] { 1L, 0L, 0L });
                Assert.assertTrue(((ResultSet) obj).next(), "Should return result set");
                Assert.assertEquals(((ResultSet) obj).getInt(1), 2);
                Assert.assertFalse(((ResultSet) obj).next(), "Should return only one result");
            }

            stats = new long[] { 0L, 0L, 0L };
            try (Statement stmt = conn.createStatement()) {
                Object obj = JdbcExecutor.getFirstResult(stmt, "select 1; select 2", stats);
                Assert.assertEquals(stats, new long[] { 1L, 0L, 0L });
                Assert.assertTrue(((ResultSet) obj).next(), "Should return result set");
                Assert.assertEquals(((ResultSet) obj).getInt(1), 1);
                Assert.assertFalse(((ResultSet) obj).next(), "Should return only one result");
            }
        }
    }

    @Test(groups = { "integration" })
    public void testGetFirstQueryResult() throws SQLException {
        try (Connection conn = DriverManager
                .getConnection("jdbc:mysql://root@" + getMySqlServer() + "/mysql?allowMultiQueries=true")) {
            long[] stats = new long[] { 0L, 0L, 0L };
            try (Statement stmt = conn.createStatement();
                    ResultSet rs = JdbcExecutor.getFirstQueryResult(stmt, "/* nothing to execute */", stats)) {
                Assert.assertEquals(stats, new long[] { 0L, 1L, 0L });
                Assert.assertFalse(rs.next(), "Should have no result");
            }

            stats = new long[] { 0L, 0L, 0L };
            try (Statement stmt = conn.createStatement();
                    ResultSet rs = JdbcExecutor.getFirstQueryResult(stmt, "drop table if exists get_first_result",
                            stats)) {
                Assert.assertEquals(stats, new long[] { 0L, 1L, 0L });
                Assert.assertFalse(rs.next(), "Should have no result");
            }

            stats = new long[] { 0L, 0L, 0L };
            try (Statement stmt = conn.createStatement();
                    ResultSet rs = JdbcExecutor.getFirstQueryResult(stmt,
                            "drop table if exists get_first_result; select 1", stats)) {
                Assert.assertEquals(stats, new long[] { 1L, 1L, 0L });
                Assert.assertTrue(rs.next(), "Should have one result");
                Assert.assertEquals(rs.getInt(1), 1);
                Assert.assertFalse(rs.next(), "Should have exact one result");
            }

            stats = new long[] { 0L, 0L, 0L };
            try (Statement stmt = conn.createStatement();
                    ResultSet rs = JdbcExecutor.getFirstQueryResult(stmt,
                            "select 1; drop table if exists get_first_result", stats)) {
                Assert.assertEquals(stats, new long[] { 1L, 0L, 0L });
                Assert.assertTrue(rs.next(), "Should have one result");
                Assert.assertEquals(rs.getInt(1), 1);
                Assert.assertFalse(rs.next(), "Should have exact one result");
            }

            stats = new long[] { 0L, 0L, 0L };
            try (Statement stmt = conn.createStatement();
                    ResultSet rs = JdbcExecutor.getFirstQueryResult(stmt,
                            "drop table if exists get_first_result1; drop table if exists get_first_result2", stats)) {
                Assert.assertEquals(stats, new long[] { 0L, 2L, 0L });
                Assert.assertFalse(rs.next(), "Should have no result");
            }
        }
    }

    @Test(groups = { "integration" })
    public void testGetFirstUpdateCount() throws SQLException {
        try (Connection conn = DriverManager
                .getConnection("jdbc:mysql://root@" + getMySqlServer() + "/mysql?allowMultiQueries=true")) {
            long[] stats = new long[] { 0L, 0L, 0L };
            try (Statement stmt = conn.createStatement()) {
                Assert.assertEquals(
                        JdbcExecutor.getFirstUpdateCount(stmt, "/* nothing to execute */", stats), 0L);
                Assert.assertEquals(stats, new long[] { 0L, 1L, 0L });
            }

            stats = new long[] { 0L, 0L, 0L };
            try (Statement stmt = conn.createStatement()) {
                Assert.assertEquals(
                        JdbcExecutor.getFirstUpdateCount(stmt, "drop table if exists get_first_result", stats), 0L);
                Assert.assertEquals(stats, new long[] { 0L, 1L, 0L });
            }

            stats = new long[] { 0L, 0L, 0L };
            try (Statement stmt = conn.createStatement()) {
                Assert.assertEquals(
                        JdbcExecutor.getFirstUpdateCount(stmt, "select 1", stats), 0L);
                Assert.assertEquals(stats, new long[] { 1L, 0L, 0L });
            }

            stats = new long[] { 0L, 0L, 0L };
            try (Statement stmt = conn.createStatement()) {
                Assert.assertEquals(
                        JdbcExecutor.getFirstUpdateCount(stmt, "select 1; drop table if exists get_first_result",
                                stats),
                        0L);
                Assert.assertEquals(stats, new long[] { 1L, 1L, 0L });
            }
        }
    }

    @Test(groups = { "integration" })
    public void testGetLastResult() throws SQLException {
        try (Connection conn = DriverManager
                .getConnection("jdbc:mysql://root@" + getMySqlServer() + "/mysql?allowMultiQueries=true")) {
            long[] stats = new long[] { 0L, 0L, 0L };
            try (Statement stmt = conn.createStatement()) {
                Object obj = JdbcExecutor.getLastResult(stmt, "/* nothing to execute */", stats);
                Assert.assertEquals(stats, new long[] { 0L, 1L, 0L });
                Assert.assertEquals(obj, Long.valueOf(0L));
            }

            stats = new long[] { 0L, 0L, 0L };
            try (Statement stmt = conn.createStatement()) {
                Object obj = JdbcExecutor.getLastResult(stmt, "drop table if exists get_first_result", stats);
                Assert.assertEquals(stats, new long[] { 0L, 1L, 0L });
                Assert.assertEquals(obj, Long.valueOf(0L));
            }

            stats = new long[] { 0L, 0L, 0L };
            try (Statement stmt = conn.createStatement()) {
                Object obj = JdbcExecutor.getLastResult(stmt, "drop table if exists get_first_result;select 1", stats);
                Assert.assertEquals(stats, new long[] { 1L, 1L, 0L });
                Assert.assertTrue(((ResultSet) obj).next(), "Should return result set");
                Assert.assertEquals(((ResultSet) obj).getInt(1), 1);
                Assert.assertFalse(((ResultSet) obj).next(), "Should return only one result");
            }

            stats = new long[] { 0L, 0L, 0L };
            try (Statement stmt = conn.createStatement()) {
                Object obj = JdbcExecutor.getLastResult(stmt, "select 2", stats);
                Assert.assertEquals(stats, new long[] { 1L, 0L, 0L });
                Assert.assertTrue(((ResultSet) obj).next(), "Should return result set");
                Assert.assertEquals(((ResultSet) obj).getInt(1), 2);
                Assert.assertFalse(((ResultSet) obj).next(), "Should return only one result");
            }

            stats = new long[] { 0L, 0L, 0L };
            try (Statement stmt = conn.createStatement()) {
                Object obj = JdbcExecutor.getLastResult(stmt, "select 1; select 2", stats);
                Assert.assertEquals(stats, new long[] { 2L, 0L, 0L });
                Assert.assertTrue(((ResultSet) obj).next(), "Should return result set");
                Assert.assertEquals(((ResultSet) obj).getInt(1), 2);
                Assert.assertFalse(((ResultSet) obj).next(), "Should return only one result");
            }
        }
    }

    @Test(groups = { "integration" })
    public void testGetLastQueryResult() throws SQLException {
        try (Connection conn = DriverManager
                .getConnection("jdbc:mysql://root@" + getMySqlServer() + "/mysql?allowMultiQueries=true")) {
            long[] stats = new long[] { 0L, 0L, 0L };
            try (Statement stmt = conn.createStatement();
                    ResultSet rs = JdbcExecutor.getLastQueryResult(stmt, "/* nothing to execute */", stats)) {
                Assert.assertEquals(stats, new long[] { 0L, 1L, 0L });
                Assert.assertFalse(rs.next(), "Should have no result");
            }

            stats = new long[] { 0L, 0L, 0L };
            try (Statement stmt = conn.createStatement();
                    ResultSet rs = JdbcExecutor.getLastQueryResult(stmt, "drop table if exists get_first_result",
                            stats)) {
                Assert.assertEquals(stats, new long[] { 0L, 1L, 0L });
                Assert.assertFalse(rs.next(), "Should have no result");
            }

            stats = new long[] { 0L, 0L, 0L };
            try (Statement stmt = conn.createStatement();
                    ResultSet rs = JdbcExecutor.getLastQueryResult(stmt,
                            "drop table if exists get_first_result; select 1", stats)) {
                Assert.assertEquals(stats, new long[] { 1L, 1L, 0L });
                Assert.assertTrue(rs.next(), "Should have one result");
                Assert.assertEquals(rs.getInt(1), 1);
                Assert.assertFalse(rs.next(), "Should have exact one result");
            }

            stats = new long[] { 0L, 0L, 0L };
            try (Statement stmt = conn.createStatement();
                    ResultSet rs = JdbcExecutor.getLastQueryResult(stmt,
                            "select 1; drop table if exists get_first_result", stats)) {
                Assert.assertEquals(stats, new long[] { 1L, 1L, 0L });
                Assert.assertTrue(rs.next(), "Should have one result");
                Assert.assertEquals(rs.getInt(1), 1);
                Assert.assertFalse(rs.next(), "Should have exact one result");
            }

            stats = new long[] { 0L, 0L, 0L };
            try (Statement stmt = conn.createStatement();
                    ResultSet rs = JdbcExecutor.getLastQueryResult(stmt,
                            "drop table if exists get_first_result1; drop table if exists get_first_result2", stats)) {
                Assert.assertEquals(stats, new long[] { 0L, 2L, 0L });
                Assert.assertFalse(rs.next(), "Should have no result");
            }
        }
    }

    @Test(groups = { "integration" })
    public void testGetLastUpdateCount() throws SQLException {
        try (Connection conn = DriverManager
                .getConnection("jdbc:mysql://root@" + getMySqlServer() + "/mysql?allowMultiQueries=true")) {
            long[] stats = new long[] { 0L, 0L, 0L };
            try (Statement stmt = conn.createStatement()) {
                Assert.assertEquals(
                        JdbcExecutor.getLastUpdateCount(stmt, "/* nothing to execute */", stats), 0L);
                Assert.assertEquals(stats, new long[] { 0L, 1L, 0L });
            }

            stats = new long[] { 0L, 0L, 0L };
            try (Statement stmt = conn.createStatement()) {
                Assert.assertEquals(
                        JdbcExecutor.getLastUpdateCount(stmt, "drop table if exists get_first_result", stats), 0L);
                Assert.assertEquals(stats, new long[] { 0L, 1L, 0L });
            }

            stats = new long[] { 0L, 0L, 0L };
            try (Statement stmt = conn.createStatement()) {
                Assert.assertEquals(
                        JdbcExecutor.getLastUpdateCount(stmt, "select 1", stats), 0L);
                Assert.assertEquals(stats, new long[] { 1L, 0L, 0L });
            }

            stats = new long[] { 0L, 0L, 0L };
            try (Statement stmt = conn.createStatement()) {
                Assert.assertEquals(
                        JdbcExecutor.getLastUpdateCount(stmt, "drop table if exists get_first_result; select 1",
                                stats),
                        0L);
                Assert.assertEquals(stats, new long[] { 1L, 1L, 0L });
            }
        }
    }

    @Test(groups = { "integration" })
    public void testGetMergedQueryResult() throws SQLException {
        try (Connection conn = DriverManager
                .getConnection("jdbc:mysql://root@" + getMySqlServer() + "/mysql?allowMultiQueries=true")) {
            long[] stats = new long[] { 0L, 0L, 0L };
            try (Statement stmt = conn.createStatement();
                    ResultSet rs = JdbcExecutor.getMergedQueryResult(stmt, "/* nothing to execute */", stats)) {
                Assert.assertEquals(stats, new long[] { 0L, 1L, 0L });
                Assert.assertFalse(rs.next(), "Should have no result");
            }

            stats = new long[] { 0L, 0L, 0L };
            try (Statement stmt = conn.createStatement();
                    ResultSet rs = JdbcExecutor.getMergedQueryResult(stmt, "drop table if exists get_first_result",
                            stats)) {
                Assert.assertEquals(stats, new long[] { 0L, 1L, 0L });
                Assert.assertFalse(rs.next(), "Should have no result");
            }

            stats = new long[] { 0L, 0L, 0L };
            try (Statement stmt = conn.createStatement();
                    ResultSet rs = JdbcExecutor.getMergedQueryResult(stmt,
                            "drop table if exists get_first_result; select 1", stats)) {
                Assert.assertEquals(stats, new long[] { 1L, 1L, 0L });
                Assert.assertTrue(rs.next(), "Should have one result");
                Assert.assertEquals(rs.getInt(1), 1);
                Assert.assertFalse(rs.next(), "Should have exact one result");
            }

            stats = new long[] { 0L, 0L, 0L };
            try (Statement stmt = conn.createStatement();
                    ResultSet rs = JdbcExecutor.getMergedQueryResult(stmt,
                            "select 1; drop table if exists get_first_result; select 2", stats)) {
                Assert.assertEquals(stats, new long[] { 2L, 1L, 0L });
                Assert.assertTrue(rs.next(), "Should have at least one result");
                Assert.assertEquals(rs.getInt(1), 1);
                Assert.assertTrue(rs.next(), "Should have one more result");
                Assert.assertEquals(rs.getInt(1), 2);
                Assert.assertFalse(rs.next(), "Should have just two results");
            }

            stats = new long[] { 0L, 0L, 0L };
            try (Statement stmt = conn.createStatement();
                    ResultSet rs = JdbcExecutor.getMergedQueryResult(stmt,
                            "drop table if exists get_first_result1; drop table if exists get_first_result2", stats)) {
                Assert.assertEquals(stats, new long[] { 0L, 2L, 0L });
                Assert.assertFalse(rs.next(), "Should have no result");
            }
        }
    }

    @Test(groups = { "integration" })
    public void testGetMergedUpdateCount() throws SQLException {
        try (Connection conn = DriverManager
                .getConnection("jdbc:mysql://root@" + getMySqlServer() + "/mysql?allowMultiQueries=true")) {
            long[] stats = new long[] { 0L, 0L, 0L };
            try (Statement stmt = conn.createStatement()) {
                Assert.assertEquals(
                        JdbcExecutor.getMergedUpdateCount(stmt, "/* nothing to execute */", stats), 0L);
                Assert.assertEquals(stats, new long[] { 0L, 1L, 0L });
            }

            stats = new long[] { 0L, 0L, 0L };
            try (Statement stmt = conn.createStatement()) {
                Assert.assertEquals(
                        JdbcExecutor.getMergedUpdateCount(stmt, "drop table if exists get_first_result", stats), 0L);
                Assert.assertEquals(stats, new long[] { 0L, 1L, 0L });
            }

            stats = new long[] { 0L, 0L, 0L };
            try (Statement stmt = conn.createStatement()) {
                Assert.assertEquals(
                        JdbcExecutor.getMergedUpdateCount(stmt, "select 1", stats), 0L);
                Assert.assertEquals(stats, new long[] { 1L, 0L, 0L });
            }

            stats = new long[] { 0L, 0L, 0L };
            try (Statement stmt = conn.createStatement()) {
                Assert.assertEquals(
                        JdbcExecutor.getMergedUpdateCount(stmt, "drop table if exists get_first_result; select 1",
                                stats),
                        0L);
                Assert.assertEquals(stats, new long[] { 1L, 1L, 0L });
            }
        }
    }

    @Test(groups = { "integration" })
    public void testMultiQueryResults() throws SQLException {
        Properties props = new Properties();

        final String query = "select 1; select 2 n union select 3 order by 1; select 4 n union select 5 union select 6 order by 1";
        try (Connection conn = DriverManager
                .getConnection("jdbc:mysql://root@" + getMySqlServer() + "?allowMultiQueries=true")) {
            JdbcExecutor exec = new JdbcExecutor(null, props);
            try (ResultSet rs = (ResultSet) exec.execute(query, conn, props)) {
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getInt(1), 1);
                Assert.assertFalse(rs.next());
            }

            JdbcExecutor.OPTION_RESULT.setValue(props, JdbcExecutor.RESULT_FIRST);
            try (ResultSet rs = (ResultSet) exec.execute(query, conn, props)) {
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getInt(1), 1);
                Assert.assertFalse(rs.next());
            }

            JdbcExecutor.OPTION_RESULT.setValue(props, JdbcExecutor.RESULT_FIRST_QUERY);
            try (ResultSet rs = (ResultSet) exec.execute(query, conn, props)) {
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getInt(1), 1);
                Assert.assertFalse(rs.next());
            }

            JdbcExecutor.OPTION_RESULT.setValue(props, JdbcExecutor.RESULT_FIRST_UPDATE);
            Assert.assertEquals(exec.execute(query, conn, props), 0L);

            JdbcExecutor.OPTION_RESULT.setValue(props, JdbcExecutor.RESULT_LAST);
            try (ResultSet rs = (ResultSet) exec.execute(query, conn, props)) {
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getInt(1), 4);
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getInt(1), 5);
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getInt(1), 6);
                Assert.assertFalse(rs.next());
            }

            JdbcExecutor.OPTION_RESULT.setValue(props, JdbcExecutor.RESULT_LAST_QUERY);
            try (ResultSet rs = (ResultSet) exec.execute(query, conn, props)) {
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getInt(1), 4);
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getInt(1), 5);
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getInt(1), 6);
                Assert.assertFalse(rs.next());
            }

            JdbcExecutor.OPTION_RESULT.setValue(props, JdbcExecutor.RESULT_LAST_UPDATE);
            Assert.assertEquals(exec.execute(query, conn, props), 0L);

            JdbcExecutor.OPTION_RESULT.setValue(props, JdbcExecutor.RESULT_MERGED_QUERIES);
            try (ResultSet rs = (ResultSet) exec.execute(query, conn, props)) {
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getInt(1), 1);
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getInt(1), 2);
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getInt(1), 3);
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getInt(1), 4);
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getInt(1), 5);
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getInt(1), 6);
                Assert.assertFalse(rs.next());
            }

            JdbcExecutor.OPTION_RESULT.setValue(props, JdbcExecutor.RESULT_MERGED_UPDATES);
            Assert.assertEquals(exec.execute(query, conn, props), 0L);

            JdbcExecutor.OPTION_RESULT.setValue(props, JdbcExecutor.RESULT_SUMMARY);
            try (ResultSet rs = (ResultSet) exec.execute(query, conn, props)) {
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getInt(1), 1);
                Assert.assertEquals(rs.getString(2), "query");
                Assert.assertEquals(rs.getLong(3), 1L);
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getInt(1), 2);
                Assert.assertEquals(rs.getString(2), "query");
                Assert.assertEquals(rs.getLong(3), 2L);
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getInt(1), 3);
                Assert.assertEquals(rs.getString(2), "query");
                Assert.assertEquals(rs.getLong(3), 3L);
                Assert.assertFalse(rs.next());
            }
        }
    }

    @Test(groups = { "integration" })
    public void testMultiUpdateCounts() throws SQLException {
        Properties props = new Properties();

        final String query = "create table if not exists test_multi_update_counts(i INTEGER); insert into test_multi_update_counts values(1); "
                + "insert into test_multi_update_counts values(2),(3); insert into test_multi_update_counts values(4),(5),(6)";
        try (Connection conn = DriverManager
                .getConnection("jdbc:mysql://root@" + getMySqlServer() + "/mysql?allowMultiQueries=true")) {
            JdbcExecutor exec = new JdbcExecutor(null, props);
            Assert.assertEquals(exec.execute(query, conn, props), 0L);

            JdbcExecutor.OPTION_RESULT.setValue(props, JdbcExecutor.RESULT_FIRST);
            Assert.assertEquals(exec.execute(query, conn, props), 0L);
            JdbcExecutor.OPTION_RESULT.setValue(props, JdbcExecutor.RESULT_FIRST_QUERY);
            Assert.assertEquals(((ResultSet) exec.execute(query, conn, props)).next(), false);
            JdbcExecutor.OPTION_RESULT.setValue(props, JdbcExecutor.RESULT_FIRST_UPDATE);
            Assert.assertEquals(exec.execute(query, conn, props), 0L);

            JdbcExecutor.OPTION_RESULT.setValue(props, JdbcExecutor.RESULT_LAST);
            Assert.assertEquals(exec.execute(query, conn, props), 3L);
            JdbcExecutor.OPTION_RESULT.setValue(props, JdbcExecutor.RESULT_LAST_QUERY);
            Assert.assertEquals(((ResultSet) exec.execute(query, conn, props)).next(), false);
            JdbcExecutor.OPTION_RESULT.setValue(props, JdbcExecutor.RESULT_LAST_UPDATE);
            Assert.assertEquals(exec.execute(query, conn, props), 3L);

            JdbcExecutor.OPTION_RESULT.setValue(props, JdbcExecutor.RESULT_MERGED_QUERIES);
            Assert.assertEquals(((ResultSet) exec.execute(query, conn, props)).next(), false);
            JdbcExecutor.OPTION_RESULT.setValue(props, JdbcExecutor.RESULT_MERGED_UPDATES);
            Assert.assertEquals(exec.execute(query, conn, props), 6L);

            JdbcExecutor.OPTION_RESULT.setValue(props, JdbcExecutor.RESULT_SUMMARY);
            try (ResultSet rs = (ResultSet) exec.execute(query, conn, props)) {
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getInt(1), 1);
                Assert.assertEquals(rs.getString(2), "update");
                Assert.assertEquals(rs.getLong(3), 0L);
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getInt(1), 2);
                Assert.assertEquals(rs.getString(2), "update");
                Assert.assertEquals(rs.getLong(3), 1L);
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getInt(1), 3);
                Assert.assertEquals(rs.getString(2), "update");
                Assert.assertEquals(rs.getLong(3), 2L);
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getInt(1), 4);
                Assert.assertEquals(rs.getString(2), "update");
                Assert.assertEquals(rs.getLong(3), 3L);
                Assert.assertFalse(rs.next());
            }
        }
    }

    @Test(groups = { "integration" })
    public void testMultiMixedResults() throws SQLException {
        Properties props = new Properties();

        final String query = "create table if not exists test_multi_mixed_results(i INTEGER); select * from test_multi_mixed_results; "
                + "insert into test_multi_mixed_results values(1),(2); select * from test_multi_mixed_results order by i; "
                + "delete from test_multi_mixed_results";
        try (Connection conn = DriverManager
                .getConnection("jdbc:mysql://root@" + getMySqlServer() + "/mysql?allowMultiQueries=true")) {
            JdbcExecutor exec = new JdbcExecutor(null, props);
            Assert.assertEquals(exec.execute(query, conn, props), 0L);

            JdbcExecutor.OPTION_RESULT.setValue(props, JdbcExecutor.RESULT_FIRST);
            Assert.assertEquals(exec.execute(query, conn, props), 0L);
            JdbcExecutor.OPTION_RESULT.setValue(props, JdbcExecutor.RESULT_FIRST_QUERY);
            Assert.assertEquals(((ResultSet) exec.execute(query, conn, props)).next(), false);
            JdbcExecutor.OPTION_RESULT.setValue(props, JdbcExecutor.RESULT_FIRST_UPDATE);
            Assert.assertEquals(exec.execute(query, conn, props), 0L);

            JdbcExecutor.OPTION_RESULT.setValue(props, JdbcExecutor.RESULT_LAST);
            Assert.assertEquals(exec.execute(query, conn, props), 2L);
            JdbcExecutor.OPTION_RESULT.setValue(props, JdbcExecutor.RESULT_LAST_QUERY);
            try (ResultSet rs = (ResultSet) exec.execute(query, conn, props)) {
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getInt(1), 1);
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getInt(1), 2);
                Assert.assertFalse(rs.next());
            }
            JdbcExecutor.OPTION_RESULT.setValue(props, JdbcExecutor.RESULT_LAST_UPDATE);
            Assert.assertEquals(exec.execute(query, conn, props), 2L);

            JdbcExecutor.OPTION_RESULT.setValue(props, JdbcExecutor.RESULT_MERGED_QUERIES);
            try (ResultSet rs = (ResultSet) exec.execute(query, conn, props)) {
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getInt(1), 1);
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getInt(1), 2);
                Assert.assertFalse(rs.next());
            }
            JdbcExecutor.OPTION_RESULT.setValue(props, JdbcExecutor.RESULT_MERGED_UPDATES);
            Assert.assertEquals(exec.execute(query, conn, props), 4L);

            JdbcExecutor.OPTION_RESULT.setValue(props, JdbcExecutor.RESULT_SUMMARY);
            try (ResultSet rs = (ResultSet) exec.execute(query, conn, props)) {
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getInt(1), 1);
                Assert.assertEquals(rs.getString(2), "update");
                Assert.assertEquals(rs.getLong(3), 0L);
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getInt(1), 2);
                Assert.assertEquals(rs.getString(2), "query");
                Assert.assertEquals(rs.getLong(3), 0L);
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getInt(1), 3);
                Assert.assertEquals(rs.getString(2), "update");
                Assert.assertEquals(rs.getLong(3), 2L);
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getInt(1), 4);
                Assert.assertEquals(rs.getString(2), "query");
                Assert.assertEquals(rs.getLong(3), 2L);
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getInt(1), 5);
                Assert.assertEquals(rs.getString(2), "update");
                Assert.assertEquals(rs.getLong(3), 2L);
                Assert.assertFalse(rs.next());
            }
        }
    }
}
