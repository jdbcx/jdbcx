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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.function.Predicate;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.github.jdbcx.BaseIntegrationTest;
import io.github.jdbcx.ResourceManager;
import io.github.jdbcx.Row;
import io.github.jdbcx.executor.jdbc.ReadOnlyResultSet;

// npx -y @modelcontextprotocol/inspector npx @modelcontextprotocol/server-everything
public class McpExecutorTest extends BaseIntegrationTest {
    static final class ClientManager implements ResourceManager {
        final List<AutoCloseable> clients;

        ClientManager() {
            clients = Collections.synchronizedList(new LinkedList<>());
        }

        @Override
        public <T extends AutoCloseable> T add(T resource) {
            if (!clients.contains(resource)) {
                clients.add(resource);
            }
            return resource;
        }

        @Override
        public <T extends AutoCloseable> T get(Class<T> clazz) {
            return get(clazz, null);
        }

        @Override
        public <T extends AutoCloseable> T get(Class<T> clazz, Predicate<T> filter) {
            if (clazz != null) {
                for (AutoCloseable resource : clients) {
                    if (clazz.isInstance(resource)) {
                        T obj = clazz.cast(resource);
                        if (filter == null || filter.test(obj)) {
                            return obj;
                        }
                    }
                }
            }
            return null;
        }
    }

    @Test(groups = { "unit" })
    public void testConstructor() {
        Assert.assertNotNull(new McpExecutor(null, null));
        Assert.assertNotNull(new McpExecutor(null, new Properties()));
    }

    @Test(groups = { "integration" })
    public void testClientManager() throws Exception {
        skipTestsIfJdkIsOlderThan(17);

        Properties props = new Properties();
        McpExecutor.OPTION_SERVER_CMD.setValue(props, "npx");
        McpExecutor.OPTION_SERVER_ARGS.setValue(props, "-y @modelcontextprotocol/server-everything");
        McpExecutor exec = new McpExecutor(null, props);

        ClientManager manager = new ClientManager();
        Assert.assertEquals(manager.clients, Collections.emptyList());
        props.setProperty(McpExecutor.OPTION_SERVER_TARGET.getName(), "prompt");
        int count = 0;
        for (Row row : exec.execute("", props, manager).rows()) {
            count++;
        }
        Assert.assertTrue(count > 1, "Should have more than one row");
        Assert.assertEquals(manager.clients.size(), 1);

        props.setProperty(McpExecutor.OPTION_SERVER_TARGET.getName(), "tool");
        count = 0;
        for (Row row : exec.execute("", props, manager).rows()) {
            count++;
        }
        Assert.assertTrue(count > 1, "Should have more than one row");
        Assert.assertEquals(manager.clients.size(), 1);

        manager.clients.get(0).close();
        manager.clients.clear();
        props.setProperty(McpExecutor.OPTION_SERVER_TARGET.getName(), "resource");
        count = 0;
        for (Row row : exec.execute("", props, manager).rows()) {
            count++;
        }
        Assert.assertTrue(count > 1, "Should have more than one row");
        Assert.assertEquals(manager.clients.size(), 1);
    }

    @Test(groups = { "integration" })
    public void testGetPrompts() throws SQLException {
        skipTestsIfJdkIsOlderThan(17);

        Properties props = new Properties();
        McpExecutor.OPTION_SERVER_CMD.setValue(props, "npx");
        McpExecutor.OPTION_SERVER_ARGS.setValue(props, "-y @modelcontextprotocol/server-everything");
        McpExecutor exec = new McpExecutor(null, props);

        // list prompts
        props.setProperty(McpExecutor.OPTION_SERVER_TARGET.getName(), "prompt");
        int count = 0;
        for (Row row : exec.execute("", props, null).rows()) {
            count++;
        }
        Assert.assertTrue(count > 1, "Should have more than one row");

        // prompt target
        props.setProperty(McpExecutor.OPTION_SERVER_TARGET.getName(), "prompt");
        // or McpExecutor.OPTION_SERVER_TARGET.setValue(props, "PROMPT")
        Assert.assertThrows(SQLException.class, () -> exec.execute("{}", props, null));
        Assert.assertThrows(SQLException.class, () -> exec.execute("{\"name\":null}", props, null));
        count = 0;
        for (Row row : exec.execute("{\"name\": \"simple-prompt\"}", props, null).rows()) {
            count++;
        }
        Assert.assertEquals(count, 1);
        count = 0;
        for (Row row : exec
                .execute("{\"name\": \"args-prompt\", \"arguments\":{\"city\":\"Shanghai\"}}",
                        props, null)
                .rows()) {
            count++;
        }
        Assert.assertEquals(count, 1);
        props.remove(McpExecutor.OPTION_SERVER_TARGET.getName());

        // prompt in request body
        McpExecutor.OPTION_SERVER_PROMPT.setValue(props, "a");
        count = 0;
        for (Row row : exec.execute(" simple-prompt ", props, null).rows()) {
            count++;
        }
        Assert.assertEquals(count, 1);

        // prompt argument
        McpExecutor.OPTION_SERVER_PROMPT.setValue(props, "simple-prompt");
        count = 0;
        for (Row row : exec.execute("", props, null).rows()) {
            count++;
        }
        Assert.assertEquals(count, 1);

        // prompt argument mixed with request body
        McpExecutor.OPTION_SERVER_PROMPT.setValue(props, "args-prompt");
        Assert.assertThrows(SQLException.class, () -> exec.execute(" non-exitsing-prompt", props, null));
        Assert.assertThrows(SQLException.class, () -> exec.execute("args-prompt", props, null));
        count = 0;
        for (Row row : exec.execute("{\"city\":\"Chengdu\", \"state\":\"Sichuan\"}", props, null).rows()) {
            count++;
        }
        Assert.assertEquals(count, 1);
    }

    @Test(groups = { "integration" })
    public void testGetResources() throws SQLException {
        skipTestsIfJdkIsOlderThan(17);

        Properties props = new Properties();
        McpExecutor.OPTION_SERVER_CMD.setValue(props, "npx");
        McpExecutor.OPTION_SERVER_ARGS.setValue(props, "-y @modelcontextprotocol/server-everything");
        McpExecutor exec = new McpExecutor(null, props);

        // list resources
        McpExecutor.OPTION_SERVER_TARGET.setValue(props, McpServerTarget.resource.name());
        int count = 0;
        for (Row row : exec.execute("", props, null).rows()) {
            count++;
        }
        Assert.assertTrue(count > 1, "Should have more than one row");

        final String resource_url = "demo://resource/static/document/extension.md";
        // resource in request body
        count = 0;
        for (Row row : exec.execute("{\"uri\": \"" + resource_url + "\"}", props, null).rows()) {
            count++;
        }
        Assert.assertEquals(count, 1);
        props.remove(McpExecutor.OPTION_SERVER_TARGET.getName());

        // resource argument
        McpExecutor.OPTION_SERVER_RESOURCE.setValue(props, "test://static/resource/non-existing");
        Assert.assertThrows(SQLException.class, () -> exec.execute("", props, null));

        McpExecutor.OPTION_SERVER_RESOURCE.setValue(props, resource_url);
        count = 0;
        for (Row row : exec.execute("", props, null).rows()) {
            count++;
        }
        Assert.assertEquals(count, 1);
        count = 0;
        for (Row row : exec.execute("{\"uri\":null}", props, null).rows()) {
            count++;
        }
        Assert.assertEquals(count, 1);

        // resource argument mixed with request body
        McpExecutor.OPTION_SERVER_RESOURCE.setValue(props, "test://static/resource/non-existing");
        count = 0;
        for (Row row : exec.execute("{\"uri\": \"" + resource_url + "\"}", props, null).rows()) {
            count++;
        }
        Assert.assertEquals(count, 1);
    }

    @Test(groups = { "integration" })
    public void testUseTool() throws SQLException {
        skipTestsIfJdkIsOlderThan(17);

        Properties props = new Properties();
        McpExecutor.OPTION_SERVER_TOOL.setValue(props, "echo");
        McpExecutor.OPTION_SERVER_CMD.setValue(props, "npx");
        McpExecutor.OPTION_SERVER_ARGS.setValue(props, "-y @modelcontextprotocol/server-everything");
        McpExecutor exec = new McpExecutor(null, props);
        for (Row row : exec.execute("{\"message\":\"hello\"}", props, null).rows()) {
            Assert.assertEquals(row.fields().size(), 5);
            Assert.assertEquals(row.value("contentType"), row.value(0));
            Assert.assertEquals(row.value("mimeType"), row.value(1));
            Assert.assertEquals(row.value("priority"), row.value(2));
            Assert.assertEquals(row.value("audience"), row.value(3));
            Assert.assertEquals(row.value("content"), row.value(4));

            Assert.assertEquals(row.value(0).asString(), "text");
            Assert.assertEquals(row.value(1).asString(), "text/plain");
            Assert.assertEquals(row.value(4).asString(), "Echo: hello");
        }
    }

    @Test(groups = { "integration" })
    public void testServerTargets() throws SQLException {
        skipTestsIfJdkIsOlderThan(17);

        Properties props = new Properties();
        McpExecutor exec = new McpExecutor(null, props);
        McpExecutor.OPTION_SERVER_CMD.setValue(props, "npx");
        McpExecutor.OPTION_SERVER_ARGS.setValue(props, "-y @modelcontextprotocol/server-everything");

        // capabilities
        McpExecutor.OPTION_SERVER_TARGET.setValue(props, McpServerTarget.capability.name());
        int count = 0;
        try (ResultSet rs = new ReadOnlyResultSet(null, exec.execute(null, props, null))) {
            while (rs.next()) {
                Assert.assertNotNull(rs.getString("type"), "Type should not be null");
                Assert.assertNotNull(rs.getString("experimental"), "Experimental should not be null");
                Assert.assertNotNull(rs.getString("logging"), "Logging should not be null");
                Assert.assertNotNull(rs.getString("prompts"), "Prompts should not be null");
                Assert.assertNotNull(rs.getString("resources"), "Resources should not be null");
                Assert.assertNotNull(rs.getString("roots"), "Roots should not be null");
                Assert.assertNotNull(rs.getString("sampling"), "Sampling should not be null");
                Assert.assertNotNull(rs.getString("tools"), "Tools should not be null");

                Assert.assertEquals(rs.getString("type"), rs.getString(1));
                Assert.assertEquals(rs.getString("experimental"), rs.getString(2));
                Assert.assertEquals(rs.getString("logging"), rs.getString(3));
                Assert.assertEquals(rs.getString("prompts"), rs.getString(4));
                Assert.assertEquals(rs.getString("resources"), rs.getString(5));
                Assert.assertEquals(rs.getString("roots"), rs.getString(6));
                Assert.assertEquals(rs.getString("sampling"), rs.getString(7));
                Assert.assertEquals(rs.getString("tools"), rs.getString(8));
                count++;
            }
        }
        Assert.assertEquals(count, 2);

        // info
        McpExecutor.OPTION_SERVER_TARGET.setValue(props, McpServerTarget.info.name());
        count = 0;
        try (ResultSet rs = new ReadOnlyResultSet(null, exec.execute(null, props, null))) {
            while (rs.next()) {
                Assert.assertNotNull(rs.getString("type"), "Type should not be null");
                Assert.assertNotNull(rs.getString("name"), "Name should not be null");
                Assert.assertNotNull(rs.getString("version"), "Version should not be null");

                Assert.assertEquals(rs.getString("type"), rs.getString(1));
                Assert.assertEquals(rs.getString("name"), rs.getString(2));
                Assert.assertEquals(rs.getString("version"), rs.getString(3));
                count++;
            }
        }
        Assert.assertEquals(count, 2);

        // prompts
        McpExecutor.OPTION_SERVER_TARGET.setValue(props, McpServerTarget.prompt.name());
        count = 0;
        try (ResultSet rs = new ReadOnlyResultSet(null, exec.execute(null, props, null))) {
            while (rs.next()) {
                Assert.assertNotNull(rs.getString("name"), "Tool name should not be null");
                Assert.assertNotNull(rs.getString("description"), "Description should not be null");
                Assert.assertNotNull(rs.getString("arguments"), "Arguments should not be null");

                Assert.assertEquals(rs.getString("name"), rs.getString(1));
                Assert.assertEquals(rs.getString("description"), rs.getString(2));
                Assert.assertEquals(rs.getString("arguments"), rs.getString(3));
                count++;
            }
        }
        Assert.assertTrue(count > 0, "Should have more than one prompt");

        // resources
        McpExecutor.OPTION_SERVER_TARGET.setValue(props, McpServerTarget.resource.name());
        count = 0;
        try (ResultSet rs = new ReadOnlyResultSet(null, exec.execute(null, props, null))) {
            while (rs.next()) {
                Assert.assertNotNull(rs.getString("name"), "Tool name should not be null");
                Assert.assertNotNull(rs.getString("description"), "Description should not be null");
                Assert.assertNotNull(rs.getString("uri"), "URI should not be null");
                Assert.assertNotNull(rs.getString("mimeType"), "MIME type should not be null");
                Assert.assertNotNull(rs.getString("annotations"), "Annotations type should not be null");

                Assert.assertEquals(rs.getString("name"), rs.getString(1));
                Assert.assertEquals(rs.getString("description"), rs.getString(2));
                Assert.assertEquals(rs.getString("uri"), rs.getString(3));
                Assert.assertEquals(rs.getString("mimeType"), rs.getString(4));
                Assert.assertEquals(rs.getString("annotations"), rs.getString(5));
                count++;
            }
        }
        Assert.assertTrue(count > 0, "Should have more than one resource");

        // resource templates
        McpExecutor.OPTION_SERVER_TARGET.setValue(props, McpServerTarget.resource_template.name());
        count = 0;
        try (ResultSet rs = new ReadOnlyResultSet(null, exec.execute(null, props, null))) {
            while (rs.next()) {
                Assert.assertNotNull(rs.getString("name"), "Tool name should not be null");
                Assert.assertNotNull(rs.getString("description"), "Description should not be null");
                Assert.assertNotNull(rs.getString("uriTemplate"), "URI template should not be null");
                Assert.assertNotNull(rs.getString("mimeType"), "MIME type should not be null");
                Assert.assertNotNull(rs.getString("annotations"), "Annotations type should not be null");

                Assert.assertEquals(rs.getString("name"), rs.getString(1));
                Assert.assertEquals(rs.getString("description"), rs.getString(2));
                Assert.assertEquals(rs.getString("uriTemplate"), rs.getString(3));
                Assert.assertEquals(rs.getString("mimeType"), rs.getString(4));
                Assert.assertEquals(rs.getString("annotations"), rs.getString(5));
                count++;
            }
        }
        Assert.assertTrue(count > 0, "Should have more than one resource template");

        // tools
        McpExecutor.OPTION_SERVER_TARGET.setValue(props, McpServerTarget.tool.name());
        count = 0;
        try (ResultSet rs = new ReadOnlyResultSet(null, exec.execute(null, props, null))) {
            while (rs.next()) {
                Assert.assertNotNull(rs.getString("name"), "Tool name should not be null");
                Assert.assertNotNull(rs.getString("description"), "Description should not be null");
                Assert.assertNotNull(rs.getString("inputSchema"), "Input schema should not be null");

                Assert.assertEquals(rs.getString("name"), rs.getString(1));
                Assert.assertEquals(rs.getString("description"), rs.getString(2));
                Assert.assertEquals(rs.getString("inputSchema"), rs.getString(3));
                count++;
            }
        }
        Assert.assertTrue(count > 0, "Should have more than one tool");
    }
}
