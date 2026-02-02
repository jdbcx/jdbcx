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
import java.sql.Statement;
import java.sql.SQLException;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import io.github.jdbcx.BaseIntegrationTest;
import io.github.jdbcx.WrappedDriver;

public class McpDriverExtensionTest extends BaseIntegrationTest {
    @DataProvider(name = "multipleResults")
    public Object[][] getMultipleResults() {
        return new Object[][] {
                { "{{ mcp }}" },
                { "{{ mcp.everything }}" },
                { "{{ mcp(cmd=npx, args=-y @modelcontextprotocol/server-everything) }}" },
                { "{{ mcp(cmd=npx, args=-y @modelcontextprotocol/server-everything, target=info) }}" },
                { "{{ mcp(cmd=npx, args=-y @modelcontextprotocol/server-everything, target=capability) }}" },
                { "{{ mcp(cmd=npx, args=-y @modelcontextprotocol/server-everything, target=prompt) }}" },
                { "{{ mcp(cmd=npx, args=-y @modelcontextprotocol/server-everything, target=resource) }}" },
                { "{{ mcp(cmd=npx, args=-y @modelcontextprotocol/server-everything, target=tool) }}" },
                { "{{ mcp(cmd=npx, args=-y @modelcontextprotocol/server-everything, target=resource_template) }}" }
                // { "{{ mcp(cmd=npx, args=-y @modelcontextprotocol/server-everything,
                // prompt=simple-prompt) }}" },
                // { "{{ mcp(cmd=npx, args=-y @modelcontextprotocol/server-everything,
                // target=prompt): {\"name\":\"args-prompt\",
                // \"arguments\":{\"city\":\"Shanghai\"}} }}" }
        };
    }

    @DataProvider(name = "singleResults")
    public Object[][] getSingleResults() {
        return new Object[][] {
                { "{{ mcp(cmd=npx, args=-y @modelcontextprotocol/server-everything, prompt=simple-prompt) }}" },
                { "{{ mcp(cmd=npx, args=-y @modelcontextprotocol/server-everything, target=prompt): simple-prompt }}" },
                { "{{ mcp(cmd=npx, args=-y @modelcontextprotocol/server-everything, target=prompt): {\"name\":\"simple-prompt\"} }}" },
                { "{{ mcp(cmd=npx, args=-y @modelcontextprotocol/server-everything, resource=demo://resource/static/document/extension.md) }}" },
                { "{{ mcp(cmd=npx, args=-y @modelcontextprotocol/server-everything, resource=non-existing): demo://resource/static/document/extension.md }}" },
                { "{{ mcp(cmd=npx, args=-y @modelcontextprotocol/server-everything, target=resource): {\"uri\":\"demo://resource/static/document/extension.md\"} }}" },
                { "{{ mcp(cmd=npx, args=-y @modelcontextprotocol/server-everything, tool=echo): {\"message\":\"me\"} }}" },
                { "{{ mcp.everything(tool=echo): {\"message\":\"me\"} }}" }
        };
    }

    @Test(dataProvider = "multipleResults", groups = { "integration" })
    public void testMultipleResults(String query) throws SQLException {
        skipTestsIfJdkIsOlderThan(17);

        Properties props = new Properties();
        props.setProperty("jdbcx.base.dir", "target/test-classes/config");
        WrappedDriver driver = new WrappedDriver();
        try (Connection conn = DriverManager.getConnection("jdbcx:", props);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(query)) {
            int count = 0;
            while (rs.next()) {
                Assert.assertNotNull(rs.getString(1));
                count++;
            }
            Assert.assertTrue(count > 1, "Should have more than one row for query: " + query);
        }
    }

    @Test(dataProvider = "singleResults", groups = { "integration" })
    public void testSingleResults(String query) throws SQLException {
        skipTestsIfJdkIsOlderThan(17);

        Properties props = new Properties();
        props.setProperty("jdbcx.base.dir", "target/test-classes/config");
        WrappedDriver driver = new WrappedDriver();
        try (Connection conn = DriverManager.getConnection("jdbcx:", props);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(query)) {
            int count = 0;
            while (rs.next()) {
                Assert.assertNotNull(rs.getString(1));
                count++;
            }
            Assert.assertTrue(count == 1, "Should have more one row for query: " + query);
        }
    }
}
