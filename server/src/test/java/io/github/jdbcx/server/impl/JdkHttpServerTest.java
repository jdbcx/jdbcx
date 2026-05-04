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
package io.github.jdbcx.server.impl;

import java.util.Properties;
import java.util.concurrent.ThreadPoolExecutor;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import io.github.jdbcx.Constants;
import io.github.jdbcx.Option;
import io.github.jdbcx.server.BaseBridgeServerTest;

public class JdkHttpServerTest extends BaseBridgeServerTest {
    public JdkHttpServerTest() {
        super();

        Properties props = new Properties();
        Option.CONFIG_PATH.setJdbcxValue(props, "target/test-classes/test-config.properties");
        Option.SERVER_URL.setJdbcxValue(props, getServerUrl());
        JdkHttpServer.OPTION_DATASOURCE_CONFIG.setJdbcxValue(props, "target/test-classes/test-datasource.properties");
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

    @Test(groups = { "unit" })
    public void testThreadPoolAndHikariCPExplicitThreads() {
        Properties props = new Properties();
        JdkHttpServer.OPTION_DATASOURCE_CONFIG.setJdbcxValue(props, "target/test-classes/test-datasource.properties");
        JdkHttpServer.OPTION_THREADS.setJdbcxValue(props, "5");
        Option.SERVER_PORT.setJdbcxValue(props, "0");
        Option.SERVER_URL.setJdbcxValue(props, "http://localhost:0/");

        JdkHttpServer s = new JdkHttpServer(props);
        try {
            Assert.assertEquals(s.getDatasource().getMaximumPoolSize(), 10,
                    "maxPoolSize should be threads*2");
            Assert.assertEquals(s.getDatasource().getMinimumIdle(), 5,
                    "minimumIdle should match threads");

            ThreadPoolExecutor pool = (ThreadPoolExecutor) s.getPool();
            Assert.assertEquals(pool.getCorePoolSize(), 5,
                    "core pool size should match threads");
            Assert.assertEquals(pool.getMaximumPoolSize(), 10,
                    "max pool size should be threads*2");
            Assert.assertTrue(pool.getQueue().remainingCapacity() > 0,
                    "queue should be bounded");
        } finally {
            s.stop();
        }
    }

    @Test(groups = { "unit" })
    public void testHikariCPUnlimitedThreads() {
        Properties props = new Properties();
        JdkHttpServer.OPTION_DATASOURCE_CONFIG.setJdbcxValue(props, "target/test-classes/test-datasource.properties");
        JdkHttpServer.OPTION_THREADS.setJdbcxValue(props, "0");
        Option.SERVER_PORT.setJdbcxValue(props, "0");
        Option.SERVER_URL.setJdbcxValue(props, "http://localhost:0/");

        JdkHttpServer s = new JdkHttpServer(props);
        try {
            int maxPoolSize = s.getDatasource().getMaximumPoolSize();
            Assert.assertTrue(
                    maxPoolSize == Constants.DETECTED_PROCESSORS * 2 || maxPoolSize == JdkHttpServer.MAX_DB_POOL_SIZE,
                    "Should use HikariCP default or unconfigured max pool size when threads <= 0, but was: "
                            + maxPoolSize);
        } finally {
            s.stop();
        }
    }

    @Test(groups = { "unit" })
    public void testHikariCPNegativeThreads() {
        Properties props = new Properties();
        JdkHttpServer.OPTION_DATASOURCE_CONFIG.setJdbcxValue(props, "target/test-classes/test-datasource.properties");
        JdkHttpServer.OPTION_THREADS.setJdbcxValue(props, "-1");
        Option.SERVER_PORT.setJdbcxValue(props, "0");
        Option.SERVER_URL.setJdbcxValue(props, "http://localhost:0/");

        JdkHttpServer s = new JdkHttpServer(props);
        try {
            int maxPoolSize = s.getDatasource().getMaximumPoolSize();
            Assert.assertTrue(
                    maxPoolSize == Constants.DETECTED_PROCESSORS * 2 || maxPoolSize == JdkHttpServer.MAX_DB_POOL_SIZE,
                    "Should use HikariCP default or unconfigured max pool size when threads <= 0, but was: "
                            + maxPoolSize);
        } finally {
            s.stop();
        }
    }
}
