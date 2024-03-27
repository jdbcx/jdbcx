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

import java.util.Properties;

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
        // nothing
    }
}
