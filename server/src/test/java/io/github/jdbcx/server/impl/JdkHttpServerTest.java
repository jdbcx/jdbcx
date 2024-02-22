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
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import io.github.jdbcx.Option;
import io.github.jdbcx.Utils;
import io.github.jdbcx.WrappedDriver;
import io.github.jdbcx.server.BaseBridgeServerTest;

public class JdkHttpServerTest extends BaseBridgeServerTest {
    public JdkHttpServerTest() {
        super();

        Properties props = new Properties();
        Option.SERVER_URL.setJdbcxValue(props, getServerUrl());
        JdkHttpServer.OPTION_DATASOURCE_CONFIG.setJdbcxValue(props, "/datasource.properties");
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

    // @Test(groups = { "integration" })
    // public void testAny() throws Exception {
    //     super.testWriteConfig();
    // }

    @Test(groups = { "integration" })
    public void testConnect() throws Exception {
        Properties props = new Properties();
        WrappedDriver d = new WrappedDriver();

        try (Connection conn = d.connect("jdbc:ch://" + getClickHouseServer(), props);
                Statement stmt = conn.createStatement();) {
            try (ResultSet rs = stmt
                    .executeQuery("select * from url('" + getServerUrl() + "?q=select%201','LineAsString')")) {
                Assert.assertEquals(rs.getMetaData().getColumnCount(), 1);
                Assert.assertTrue(rs.next());
                Assert.assertTrue(rs.getString(1).length() > 0);
                Assert.assertFalse(rs.next());
            }

            try (ResultSet rs = stmt
                    .executeQuery(
                            "select * from url('" + getServerUrl() + "?m=d&q=select+1+as+r','CSVWithNames')")) {
                Assert.assertEquals(rs.getMetaData().getColumnCount(), 1);
                Assert.assertEquals(rs.getMetaData().getColumnName(1), "r");
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getString(1), "1");
                Assert.assertFalse(rs.next());
            }

            int count = 70000;
            String query = "select *, b::string as c from (select generate_series a, a*random() b from generate_series("
                    + count + ",1,-1))";
            try (ResultSet rs = stmt.executeQuery(
                    "select * from url('" + getServerUrl() + "?m=d&q=" + Utils.encode(query) + "','CSVWithNames')")) {
                Assert.assertEquals(rs.getMetaData().getColumnCount(), 3);
                Assert.assertEquals(rs.getMetaData().getColumnName(1), "a");
                Assert.assertEquals(rs.getMetaData().getColumnName(2), "b");
                Assert.assertEquals(rs.getMetaData().getColumnName(3), "c");
                while (rs.next()) {
                    Assert.assertEquals(rs.getInt(1), count--);
                    Assert.assertEquals(rs.getString(2), rs.getString(3));
                }
                Assert.assertEquals(count, 0);
            }
        }
    }
}
