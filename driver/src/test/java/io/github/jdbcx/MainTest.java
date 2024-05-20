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
package io.github.jdbcx;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

public class MainTest {
    static String[][] toArray(List<List<String[]>> list) {
        List<String[]> result = new LinkedList<>();
        for (List<String[]> l : list) {
            result.addAll(l);
        }
        return result.toArray(new String[0][2]);
    }

    @Test(groups = { "unit" })
    public void testCloseConnection() throws SQLException {
        Main.closeQuietly(null);
        Main.closeQuietly(new ByteArrayInputStream(new byte[1]));

        try (Connection conn = DriverManager.getConnection("jdbcx:sqlite::memory:", null)) {
            Assert.assertFalse(conn.isClosed());
            Main.closeQuietly(conn);
            Assert.assertTrue(conn.isClosed());
        }
    }

    @Test(groups = { "unit" })
    public void testGetOrCreateConnection() throws SQLException {
        Assert.assertNull(Main.getOrCreateConnection(null, null, null, -1, null, false));
        Assert.assertNull(Main.getOrCreateConnection(null, null, null, 0, null, true));

        Connection conn = Main.getOrCreateConnection("jdbcx:sqlite::memory:", null, null, 1000, "select 2",
                false);
        Assert.assertNotNull(conn);
        for (int i = 0; i < 3; i++) {
            Assert.assertFalse(conn.isClosed());
            final Properties props;
            if (i % 3 == 1) {
                props = System.getProperties();
            } else {
                props = new Properties();
            }
            Assert.assertEquals(conn,
                    Main.getOrCreateConnection("jdbcx:sqlite::memory:", props, conn, 1000,
                            "select 2", false));
        }

        for (int i = 0; i < 100; i++) {
            Assert.assertFalse(conn.isClosed());
            conn.close();
            Assert.assertTrue(conn.isClosed());
            Connection newConn = Main.getOrCreateConnection("jdbcx:sqlite::memory:", null, conn, 1000,
                    "select 3",
                    false);
            Assert.assertNotEquals(conn, newConn);
            Assert.assertTrue(conn.isClosed());
            Assert.assertFalse(newConn.isClosed());
            conn = newConn;
        }
    }

    @Test(groups = { "unit" })
    public void testSplitTasks() {
        Assert.assertEquals(toArray(Main.splitTasks(Collections.emptyList(), 1)), new String[0][]);
        Assert.assertEquals(toArray(Main.splitTasks(Collections.emptyList(), 2)), new String[0][]);

        Assert.assertEquals(toArray(Main.splitTasks(Collections.singletonList(new String[] { "n", "q" }), 1)),
                new String[][] { { "n", "q" } });
        Assert.assertEquals(toArray(Main.splitTasks(Collections.singletonList(new String[] { "n", "q" }), 2)),
                new String[][] { { "n", "q" } });

        Assert.assertEquals(
                toArray(Main.splitTasks(
                        Arrays.asList(new String[] { "n1", "q1" }, new String[] { "n2", "q2" }),
                        1)),
                new String[][] { { "n1", "q1" }, { "n2", "q2" } });
        Assert.assertEquals(
                toArray(Main.splitTasks(
                        Arrays.asList(new String[] { "n1", "q1" }, new String[] { "n2", "q2" }),
                        2)),
                new String[][] { { "n1", "q1" }, { "n2", "q2" } });

        Assert.assertEquals(
                toArray(Main.splitTasks(
                        Arrays.asList(new String[] { "n1", "q1" }, new String[] { "n2", "q2" },
                                new String[] { "n3", "q3" }),
                        1)),
                new String[][] { { "n1", "q1" }, { "n2", "q2" }, { "n3", "q3" } });
        Assert.assertEquals(
                toArray(Main.splitTasks(
                        Arrays.asList(new String[] { "n1", "q1" }, new String[] { "n2", "q2" },
                                new String[] { "n3", "q3" }),
                        2)),
                new String[][] { { "n1", "q1" }, { "n3", "q3" }, { "n2", "q2" } });
    }

    @Test(groups = { "unit" })
    public void testExecuteQueries() throws IOException, SQLException {
        final String url = "jdbcx:sqlite::memory:";
        Assert.assertFalse(Main.executeQueries(url, false, 1000, null, null, null, null,
                false));
        Assert.assertFalse(Main.executeQueries(url, false, 1000, null,
                Collections.emptyList(), null,
                null, false));

        Assert.assertTrue(Main.executeQueries(url, false, 1000, null,
                Collections.singletonList(new String[] { "first", "select 1" }), null, null, false));
        Assert.assertTrue(Main.executeQueries(url, false, 1000, null,
                Collections.singletonList(
                        new String[] { "first",
                                "SELECT name FROM sqlite_master WHERE type='invalid_table'" }),
                null, null, false));
        Assert.assertThrows(SQLException.class,
                () -> Main.executeQueries(url, false, 1000, null,
                        Collections.singletonList(new String[] { "first", "select '" }), null,
                        null, false));

        Assert.assertTrue(Main.executeQueries(url, false, 1000, null,
                Arrays.asList(new String[] { "first", "select 1" }, new String[] { "2nd", "select 2" }),
                null, null,
                false));
        Assert.assertTrue(Main.executeQueries(url, false, 1000, null,
                Arrays.asList(new String[] { "first",
                        "SELECT name FROM sqlite_master WHERE type='invalid_table'" },
                        new String[] { "2nd", "select 2" }),
                null, null, false));
        Assert.assertTrue(Main.executeQueries(url, false, 1000, null,
                Arrays.asList(new String[] { "ddl",
                        "create table if not exists test_execute_queries(a VARCHAR(30))" },
                        new String[] { "1st",
                                "SELECT name FROM sqlite_master WHERE type='test_execute_queries'" },
                        new String[] { "2nd", "insert into test_execute_queries values('x')" },
                        new String[] { "3rd", "select a from test_execute_queries" }),
                null, null, true));
    }

    @Test(groups = { "private" })
    public void testExecuteQueriesInParallel() throws Exception {
        System.setProperty("verbose", "true");
        // Main.main(new String[] { "jdbcx:sqlite::memory:",
        // "@target/test-classes/test-queries/sequential" });

        System.setProperty("tasks", "2");
        Main.main(new String[] { "jdbcx:sqlite::memory:", "@target/test-classes/test-queries/parallel" });
    }
}
