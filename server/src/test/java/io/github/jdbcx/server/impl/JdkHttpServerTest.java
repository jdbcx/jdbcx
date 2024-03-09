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
package io.github.jdbcx.server.impl;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import io.github.jdbcx.Option;
import io.github.jdbcx.server.BaseBridgeServerTest;

public class JdkHttpServerTest extends BaseBridgeServerTest {
    public JdkHttpServerTest() {
        super();

        Properties props = new Properties();
        Option.CONFIG_PATH.setJdbcxValue(props, "target/test-classes/test-config.properties");
        Option.SERVER_URL.setJdbcxValue(props, getServerUrl());
        JdkHttpServer.OPTION_DATASOURCE_CONFIG.setJdbcxValue(props, "/test-datasource.properties");
        server = new JdkHttpServer(props);
    }

    @BeforeClass(groups = { "integration" })
    protected void startServer() {
        server.start();
    }

    @AfterClass(groups = { "integration" })
    protected void stopServer() {
        server.stop();
    }

    @Test(groups = { "integration" })
    public void testAny() throws Exception {
        Properties props = new Properties();
        // props.setProperty("jdbcx.base.dir", "target/test-classes/config");

        try (Connection conn = DriverManager.getConnection("jdbcx:ch://" + getClickHouseServer(), props);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("select * from {{ bridge.db.my-duckdb: select 1}}")) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getInt(1), 1);
            Assert.assertFalse(rs.next());
        }

        try (Connection conn = DriverManager.getConnection("jdbcx:duckdb:", props);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("select * from {{ bridge.db.my-sqlite: select 1}}")) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getInt(1), 1);
            Assert.assertFalse(rs.next());
        }
    }
}
