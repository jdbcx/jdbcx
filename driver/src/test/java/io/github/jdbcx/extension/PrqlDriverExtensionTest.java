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
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.github.jdbcx.WrappedDriver;

public class PrqlDriverExtensionTest {
    @Test(groups = { "private" })
    public void testQuery() throws SQLException {
        final String query = "from `SYS.SYSTABLES`\n"
                + "select {{ script: 'TABLE' }}{{ shell: echo 'NAME' }}";
        WrappedDriver d = new WrappedDriver();
        Properties props = new Properties();
        props.setProperty("jdbcx.config.path", "~/jdbcx-config.properties");
        props.setProperty("jdbcx.prql.cli.path", "~/tests/prqlc8");
        // props.setProperty("jdbcx.prql.cli.path", "~/.cargo/bin/prqlc");
        // props.setProperty("jdbcx.prql.compile.target", "mssql");
        // try (Connection conn =
        // DriverManager.getConnection("jdbcx:prql:sqlite::memory:", props);
        // Statement stmt = conn.createStatement();
        // ResultSet rs = stmt.executeQuery("from `SYS.SYSTABLES`\ntake 3")) {
        // Assert.assertTrue(rs.next(), "Should have at least one row");
        // Assert.assertTrue(rs.getString(1).length() > 0, "Should have content");
        // }
        props.setProperty("jdbcx.prql.exec.error", "throw");
        props.setProperty("jdbcx.script.exec.error", "throw");
        props.setProperty("jdbcx.shell.exec.error", "throw");
        try (Connection conn = DriverManager.getConnection("jdbcx:prql:sqlite::memory:", props);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(query)) {
            Assert.assertTrue(rs.next(), "Should have at least one row");
            Assert.assertTrue(rs.getString(1).length() > 0, "Should have content");
        }

        try (Connection conn = DriverManager.getConnection("jdbcx:sqlite::memory:", props);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt
                        .executeQuery("select '{% prql: from `SYS.SYSTABLES` %}' as v, TABLENAME from SYS.SYSTABLES")) {
            Assert.assertTrue(rs.next(), "Should have at least one row");
            Assert.assertEquals(rs.getString(1), "");
            Assert.assertTrue(rs.getString(2).length() > 0, "Should have content");
        }

        try (Connection conn = DriverManager.getConnection("jdbcx:sqlite::memory:", props);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt
                        .executeQuery(
                                "select '{{ sql(id=ch-dev): select 'select ''' || version() || ''' local_version, version() remote_version' }}")) {
            Assert.assertTrue(rs.next(), "Should have at least one row");
            Assert.assertEquals(rs.getString(1), "");
            Assert.assertTrue(rs.getString(2).length() > 0, "Should have content");
        }
    }
}
