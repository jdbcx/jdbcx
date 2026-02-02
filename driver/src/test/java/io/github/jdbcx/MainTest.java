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
package io.github.jdbcx;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

public class MainTest {
    static final String DEFAULT_CONNECTION_URL = "jdbcx:sqlite::memory:";

    static String[][] toArray(List<List<String[]>> list) {
        List<String[]> result = new LinkedList<>();
        for (List<String[]> l : list) {
            result.addAll(l);
        }
        return result.toArray(new String[0][2]);
    }

    @Test(groups = { "unit" })
    public void testCustomArguments() {
        Properties connProps = new Properties();
        connProps.setProperty("x", "1");
        connProps.setProperty("y", "false");
        Properties props = new Properties();
        props.setProperty("x", "y");
        String[][] queries = new String[][] { { "q1", "select 'q'" }, { "q 1", "select 2" } };
        Main.Arguments args = new Main.Arguments(connProps, 10L, 9L, "12.3", Compression.BROTLI, 8,
                7, Format.ARROW_STREAM, props, 6, 5, "4321", Arrays.asList(queries), "select 2", 3, true);
        Assert.assertEquals(args.connectionProps, connProps);
        Assert.assertEquals(args.loopCount, 10L);
        Assert.assertEquals(args.loopInterval, 9L);
        Assert.assertEquals(args.outputFile, "12.3");
        Assert.assertEquals(args.outputFormat, Format.ARROW_STREAM);
        Assert.assertEquals(args.outputParams, props);
        Assert.assertEquals(args.outputCompression, Compression.BROTLI);
        Assert.assertEquals(args.compressionLevel, 8);
        Assert.assertEquals(args.compressionBuffer, 7);
        Assert.assertEquals(args.tasks, 6);
        Assert.assertEquals(args.taskCheckInterval, 5);
        Assert.assertEquals(args.url, "4321");
        Assert.assertEquals(args.queries.toArray(new String[2][]), queries);
        Assert.assertEquals(args.validationQuery, "select 2");
        Assert.assertEquals(args.validationTimeout, 3);
        Assert.assertTrue(args.verbose);
    }

    @Test(groups = { "unit" })
    public void testDefaultArguments() throws IOException {
        Main.Arguments args = new Main.Arguments(new String[] { "new" });
        Assert.assertEquals(args.connectionProps, new Properties());
        Assert.assertEquals(args.loopCount, 1L);
        Assert.assertEquals(args.loopInterval, 0L);
        Assert.assertEquals(args.outputFile, "");
        Assert.assertEquals(args.outputFormat, Format.TSV);
        Assert.assertEquals(args.outputParams, new Properties());
        Assert.assertEquals(args.outputCompression, Compression.NONE);
        Assert.assertEquals(args.compressionLevel, -1);
        Assert.assertEquals(args.compressionBuffer, 0);
        Assert.assertEquals(args.tasks, 1);
        Assert.assertEquals(args.taskCheckInterval, 10);
        Assert.assertEquals(args.url, "new");
        Assert.assertEquals(args.queries, Collections.emptyList());
        Assert.assertEquals(args.validationQuery, "");
        Assert.assertEquals(args.validationTimeout, 3);
        Assert.assertEquals(args.verbose, false);
    }

    @Test(groups = { "unit" })
    public void testNewArguments() throws IOException {
        Main.Arguments args = new Main.Arguments(new String[1]);
        Main.Arguments newArgs = new Main.Arguments(args, Collections.singletonList(new String[] { "1", "q1" }));
        Assert.assertTrue(args.connectionProps == newArgs.connectionProps);
        Assert.assertTrue(args.loopCount == newArgs.loopCount);
        Assert.assertTrue(args.loopInterval == newArgs.loopInterval);
        Assert.assertTrue(args.outputFile == newArgs.outputFile);
        Assert.assertTrue(args.outputFormat == newArgs.outputFormat);
        Assert.assertTrue(args.outputParams == newArgs.outputParams);
        Assert.assertTrue(args.outputCompression == newArgs.outputCompression);
        Assert.assertTrue(args.compressionLevel == newArgs.compressionLevel);
        Assert.assertTrue(args.compressionBuffer == newArgs.compressionBuffer);
        Assert.assertTrue(args.tasks == newArgs.tasks);
        Assert.assertTrue(args.taskCheckInterval == newArgs.taskCheckInterval);
        Assert.assertTrue(args.url == newArgs.url);
        Assert.assertTrue(args.validationQuery == newArgs.validationQuery);
        Assert.assertTrue(args.validationTimeout == newArgs.validationTimeout);
        Assert.assertTrue(args.verbose == newArgs.verbose);

        Assert.assertNotEquals(args.queries, newArgs.queries);
        Assert.assertEquals(newArgs.queries.toArray(new String[1][]), new String[][] { { "1", "q1" } });
    }

    @Test(groups = { "unit" })
    public void testExtractQueries() throws IOException {
        Assert.assertEquals(
                Main.Arguments.extractQueries(new String[] { "url", "", "@?iles", "select 1", "@1.sql" })
                        .toArray(new String[0][]),
                new String[][] { { "Query #1", "select 1" } });
    }

    @Test(groups = { "unit" })
    public void testGetOrCreateConnection() throws SQLException {
        Assert.assertNull(Main.getOrCreateConnection(null, null, null, -1, null));
        Assert.assertNull(Main.getOrCreateConnection(null, null, null, 0, null));

        Connection conn = Main.getOrCreateConnection(DEFAULT_CONNECTION_URL, new Properties(), null, 1000, "select 2");
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
                    Main.getOrCreateConnection(DEFAULT_CONNECTION_URL, props, conn, 1000, "select 2"));
        }

        for (int i = 0; i < 100; i++) {
            Assert.assertFalse(conn.isClosed());
            conn.close();
            Assert.assertTrue(conn.isClosed());
            Connection newConn = Main.getOrCreateConnection(DEFAULT_CONNECTION_URL, new Properties(), conn, 1000,
                    "select 3");
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
        final String url = DEFAULT_CONNECTION_URL;
        final Main.Arguments args = new Main.Arguments(new String[] { url });
        Assert.assertTrue(Main.executeQueries(args, null));
        Assert.assertTrue(Main.executeQueries(new Main.Arguments(args, Collections.emptyList()), null));

        Assert.assertTrue(Main.executeQueries(
                new Main.Arguments(args, Collections.singletonList(new String[] { "first", "select 1" })), null));
        Assert.assertTrue(Main.executeQueries(new Main.Arguments(args, Collections.singletonList(
                new String[] { "first",
                        "SELECT name FROM sqlite_master WHERE type='invalid_table'" })),
                null));
        Assert.assertThrows(SQLException.class,
                () -> Main.executeQueries(
                        new Main.Arguments(args, Collections.singletonList(new String[] { "first", "select '" })),
                        null));

        Assert.assertTrue(Main.executeQueries(
                new Main.Arguments(args, Arrays.asList(new String[] { "first", "select 1" }, new String[] { "2nd",
                        "select 2" })),
                null));
        Assert.assertTrue(Main.executeQueries(new Main.Arguments(args, Arrays.asList(new String[] { "first",
                "SELECT name FROM sqlite_master WHERE type='invalid_table'" },
                new String[] { "2nd", "select 2" })), null));
        Assert.assertTrue(Main.executeQueries(new Main.Arguments(args, Arrays.asList(new String[] { "ddl",
                "create table if not exists test_execute_queries(a VARCHAR(30))" },
                new String[] { "1st",
                        "SELECT name FROM sqlite_master WHERE type='test_execute_queries'" },
                new String[] { "2nd", "insert into test_execute_queries values('x')" },
                new String[] { "3rd", "select a from test_execute_queries" })), null));
    }

    @Test(groups = { "unit" })
    public void testExecuteQueriesInParallel() throws Exception {
        System.setProperty("verbose", "true");
        Assert.assertEquals(
                Main.process(new String[] { DEFAULT_CONNECTION_URL, "@target/test-classes/test-queries/sequential" }),
                0);

        System.setProperty("tasks", "2");
        Assert.assertEquals(
                Main.process(new String[] { DEFAULT_CONNECTION_URL, "@target/test-classes/test-queries/parallel" }),
                0);
    }

    @Test(groups = { "unit" })
    public void testProcess() throws Exception {
        Assert.assertEquals(Main.process(null), 0);
        Assert.assertEquals(Main.process(new String[0]), 0);
        Assert.assertEquals(Main.process(new String[] { "-h" }), 0);
        Assert.assertEquals(Main.process(new String[] { "--help" }), 0);
        Assert.assertEquals(Main.process(new String[] { "help" }), 0);

        Assert.assertEquals(Main.process(new String[] { "keygen" }), 0);

        System.setProperty(ConfigManager.OPTION_KEY_FILE.getName(), "target/test-classes/test.key");
        Assert.assertEquals(Main.process(new String[] { "decrypt", "zFjzko6vtUk8KRN3GSL1dyFIV4K6ffG5zGl/+tIuLw==" }),
                0);
        Assert.assertEquals(Main.process(new String[] { "encrypt", "123" }), 0);

        Assert.assertEquals(Main.process(new String[] { "encrypt", "123", "me" }), 0);
        Assert.assertEquals(
                Main.process(new String[] { "decrypt", "hs2/iUATM4TNnj1dz5ZBTTlqHNGDEg032N6DS2pxcg==", "me" }), 0);

        Assert.assertEquals(Main.process(new String[] { "jdbcx:", "{{ script: 1}}" }), 0);
    }

    @Test(groups = { "unit" })
    public void testGenerateToken() throws Exception {
        Assert.assertThrows(IllegalArgumentException.class, () -> Main.generateToken(null, false));
        Assert.assertThrows(IllegalArgumentException.class, () -> Main.generateToken("", false));
        Assert.assertThrows(IllegalArgumentException.class, () -> Main.generateToken("issuer=a", false));
        Assert.assertThrows(IllegalArgumentException.class, () -> Main.generateToken("subject=a", false));

        Assert.assertEquals(Main.generateToken("issuer=a;subject=b", false), 0);
        Assert.assertEquals(Main.generateToken("issuer=a;subject=b;expires=3", false), 0);
        Assert.assertEquals(Main.generateToken("issuer=a;subject=b;expires=-1;role=guest", false), 0);
        Assert.assertEquals(Main.generateToken("issuer=a;subject=b;expires=-1;role=guest,admin;host=localhost", false),
                0);
    }
}
