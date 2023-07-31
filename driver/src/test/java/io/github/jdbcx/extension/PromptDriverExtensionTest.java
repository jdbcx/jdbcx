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

import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.github.jdbcx.BaseIntegrationTest;
import io.github.jdbcx.Utils;

public class PromptDriverExtensionTest extends BaseIntegrationTest {
    @Test(groups = { "private" })
    public void testGoogleMakerSuite() throws Exception {
        final String template = "{\"prompt\": {\"messages\": [{\"content\":\"${my.prompt}\"}]},\"temperature\": 0.1}";
        final String query = Utils.format("select '{{ prompt(google.api=generateMessage, google.model=chat-bison-001, "
                + "request.template='%s', my.prompt='Hi there!') }}'", template);
        Properties props = new Properties();
        // jdbcx.prompt.provider=google
        // jdbcx.prompt.google.api.key=<secret>
        // #jdbcx.prompt.google.api=generateMessage
        // #jdbcx.prompt.google.model=chat-bison-001
        // jdbcx.prompt.result.json.path=candidates[].content
        // #jdbcx.prompt.proxy=<host>:<port>
        props.load(new FileInputStream(Utils.normalizePath("~/Backup/secrets/maker-suite.properties")));
        try (Connection conn = DriverManager.getConnection("jdbcx:ch://" + getClickHouseServer(), props);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(query)) {
            Assert.assertTrue(rs.next(), "Should have at least one row");
            Assert.assertTrue(rs.getString(1).length() > 0, "Should have answer");
            Assert.assertFalse(rs.next(), "Should have only one row");
        }
    }

    @Test(groups = { "private" })
    public void testLocalGpt() throws Exception {
        final String template = "{ \"model\": \"gpt-3.5-turbo\", \"temperature\": 0, \"messages\": [" +
                "{\"role\": \"system\", \"content\": \"You are a calculator. " +
                "You can solve any expression and provide the result as a short and precise answer, " +
                "showing only the result number, nothing else.\"},\n" +
                "{\"role\": \"user\", \"content\": \"${my.prompt}\"}\n" +
                "]}";
        final String query = Utils.format("select '{{ prompt(my.prompt='10 + 2') }}'", template);
        Properties props = new Properties();
        props.setProperty("jdbcx.prompt.base.url", "http://localhost:4891/v1/chat/completions");
        props.setProperty("jdbcx.prompt.request.headers", "Content-Type=application/json");
        props.setProperty("jdbcx.prompt.request.template", template);
        props.setProperty("jdbcx.prompt.result.json.path", "choices[0].message.content");
        try (Connection conn = DriverManager.getConnection("jdbcx:ch://" + getClickHouseServer(), props);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(query)) {
            Assert.assertTrue(rs.next(), "Should have at least one row");
            Assert.assertEquals(rs.getString(1), "12");
            Assert.assertFalse(rs.next(), "Should have only one row");
        }
    }
}
