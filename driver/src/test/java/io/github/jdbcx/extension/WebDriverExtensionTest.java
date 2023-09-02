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

import io.github.jdbcx.BaseIntegrationTest;

public class WebDriverExtensionTest extends BaseIntegrationTest {
    @Test(groups = { "integration" })
    public void testTemplateEscaping() throws SQLException {
        Properties props = new Properties();
        String url = "http://" + getClickHouseServer();
        // Too bad that ClickHouse will somehow unescape the query...
        String query = "{{ web(base.url='" + url + "',request.template=\"select '1${}3'\", exec.dryrun=true): \"2\"}}";
        try (Connection conn = DriverManager.getConnection("jdbcx:ch:" + url, props);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(query)) {
            Assert.assertTrue(rs.next(), "Should have at least one row");
            Assert.assertEquals(rs.getString("request"), "select '1\"2\"3'");
            Assert.assertFalse(rs.next(), "Should have only one row");
        }

        props.setProperty("jdbcx.web.request.escape.target", "\"");
        props.setProperty("jdbcx.web.request.escape.char", "\\");
        try (Connection conn = DriverManager.getConnection("jdbcx:ch:" + url, props);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(query)) {
            Assert.assertTrue(rs.next(), "Should have at least one row");
            Assert.assertEquals(rs.getString("request"), "select '1\\\"2\\\"3'");
            Assert.assertFalse(rs.next(), "Should have only one row");
        }
    }

    @Test(groups = { "private" })
    public void testCustomUrlTemplate() throws SQLException {
        Properties props = new Properties();
        props.setProperty("jdbcx.web.exec.error", "throw");
        try (Connection conn = DriverManager.getConnection("jdbcx:ch://" + getClickHouseServer(), props);
                Statement stmt = conn.createStatement()) {
            Assert.assertThrows(SQLException.class,
                    () -> stmt.executeQuery("{{ web.ch-altinity(query='?decompress=1'): select 1 }}"));

            try (ResultSet rs = stmt.executeQuery("{{web.makersuite: Hi, \"there\"!}}")) {
                Assert.assertTrue(rs.next(), "Should have at least one row");
                Assert.assertNotNull(rs.getString(1), "Should have answer returned from server");
                Assert.assertFalse(rs.next(), "Should have only one row");
            }
        }
    }
}
