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
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.github.jdbcx.BaseIntegrationTest;
import io.github.jdbcx.Constants;

public class ShellDriverExtensionTest extends BaseIntegrationTest {
    @Test(groups = { "private" })
    public void testQuerySpecialCases() throws SQLException {
        Properties props = new Properties();
        try (Connection conn = DriverManager.getConnection("jdbcx:ch://" + getClickHouseServer(), props);
                Statement stmt = conn.createStatement()) {
            try (ResultSet rs = stmt // should be -V
                    .executeQuery("select '{{ shell(result.string.escape=true): ~/tests/prqlc9 -v }}'")) {
                SQLWarning warning = stmt.getWarnings();
                Assert.assertNotNull(warning, "Should have warning");
                Assert.assertNull(warning.getNextException(), "Should not have next exception");
                Assert.assertNull(warning.getNextWarning(), "Should not have next warning");
            }

            String query = "{% vars: v1=~/tests/prqlc8, v2=~/tests/prqlc9 %}\n"
                    + "select  '{{ shell(result.string.trim=true, result.string.replace=true): ${v1} -V }}' v1,\n"
                    + "'{{ shell(result.string.trim=true, result.string.replace=true): ${v2} -v }}' v2,\n"
                    + "'{{ prql(cli.path=${v1}): from t }}' v1_result,\n"
                    + "'{{ prql(cli.path=${v2}): from t }}' v2_result";
            try (ResultSet rs = stmt.executeQuery(query)) {
                SQLWarning warning = stmt.getWarnings();
                Assert.assertNotNull(warning, "Should have warning");
                Assert.assertNull(warning.getNextException(), "Should not have next exception");
                Assert.assertNull(warning.getNextWarning(), "Should not have next warning");
            }

            // empty query
            Assert.assertThrows(SQLException.class, () -> stmt.executeQuery(
                    "{% var: ch-server=node1, test-client=node2 %}\n{% shell: bash -c 'echo ${ch-server}' %}"));
        }
    }

    @Test(groups = { "integration" })
    public void testQuery() throws SQLException {
        final String query = "select '{{ shell: echo 12 }}'";
        final String address = getClickHouseServer();
        Properties props = new Properties();
        props.setProperty("jdbcx.shell.result.string.trim", Constants.TRUE_EXPR);
        try (Connection conn = DriverManager.getConnection("jdbcx:ch://" + address, props);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(query)) {
            Assert.assertTrue(rs.next(), "Should have at least one row");
            Assert.assertEquals(rs.getString(1), "12");
            Assert.assertFalse(rs.next(), "Should have only one row");
        }

        try (Connection conn = DriverManager.getConnection("jdbcx:shell:ch://" + address, props);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("echo select 12")) {
            Assert.assertTrue(rs.next(), "Should have at least one row");
            Assert.assertEquals(rs.getString(1), "12");
            Assert.assertFalse(rs.next(), "Should have only one row");
        }
    }
}
