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
package io.github.jdbcx.server;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.github.jdbcx.BaseIntegrationTest;
import io.github.jdbcx.Constants;
import io.github.jdbcx.Utils;
import io.github.jdbcx.WrappedDriver;
import io.github.jdbcx.executor.Stream;
import io.github.jdbcx.executor.WebExecutor;
import io.github.jdbcx.server.impl.JdkHttpServer;

public abstract class BaseBridgeServerTest extends BaseIntegrationTest {
    protected JdkHttpServer server;

    @Test(groups = { "integration" })
    public void testConnect() throws Exception {
        // Properties props = new Properties();
        // WrappedDriver d = new WrappedDriver();

        // try (Connection conn = d.connect("jdbc:ch://" + getClickHouseServer(),
        // props);
        // Statement stmt = conn.createStatement();
        // ResultSet rs = stmt.executeQuery("select * from url('" + getServerUrl() + "',
        // 'LineAsString')")) {
        // Assert.assertFalse(rs.next());
        // }
    }

    @Test(groups = { "integration" })
    public void testDefaultUrl() throws Exception {
        final String url = getServerUrl();

        Properties config = new Properties();
        Map<?, ?> headers = new HashMap<>();
        WebExecutor web = new WebExecutor(config);
        WebExecutor.OPTION_FOLLOW_REDIRECT.setValue(config, Constants.FALSE_EXPR);
        HttpURLConnection conn = web.openConnection(new URL(url), config, headers);
        Assert.assertEquals(conn.getResponseCode(), HttpURLConnection.HTTP_NOT_FOUND);

        // try again using POST
        conn = web.openConnection(new URL(url), config, headers);
        conn.setRequestMethod("POST");
        conn.setDoOutput(true);
        try (OutputStream out = conn.getOutputStream()) {
            Stream.writeAll(out, "");
        }
        Assert.assertEquals(conn.getResponseCode(), HttpURLConnection.HTTP_NOT_FOUND);
    }

    @Test(groups = { "integration" })
    public void testNonExistentUrl() throws Exception {
        final String url = getServerUrl();

        Properties config = new Properties();
        Map<?, ?> headers = new HashMap<>();
        WebExecutor web = new WebExecutor(config);
        WebExecutor.OPTION_FOLLOW_REDIRECT.setValue(config, Constants.FALSE_EXPR);
        HttpURLConnection conn = web.openConnection(new URL(url + "non-exist-query.id"), config, headers);
        Assert.assertEquals(conn.getResponseCode(), HttpURLConnection.HTTP_NOT_FOUND);

        // try again using POST
        conn = web.openConnection(new URL(url), config, headers);
        conn.setRequestProperty(BridgeServer.HEADER_QUERY_ID, "non-exist-query.id");
        conn.setRequestMethod("POST");
        conn.setDoOutput(false);
        Assert.assertEquals(conn.getResponseCode(), HttpURLConnection.HTTP_NOT_FOUND);
    }

    @Test(groups = { "integration" })
    public void testSubmitQuery() throws Exception {
        final String url = getServerUrl();

        Properties config = new Properties();
        Map<String, String> headers = new HashMap<>();
        headers.put(BridgeServer.HEADER_ACCEPT, "application/bson");
        headers.put(BridgeServer.HEADER_ACCEPT_ENCODING, "xz,lz4,gzip");
        WebExecutor web = new WebExecutor(config);
        WebExecutor.OPTION_FOLLOW_REDIRECT.setValue(config, Constants.FALSE_EXPR);
        HttpURLConnection conn = web.openConnection(new URL(url + "?q=select+1"), config, headers);
        Assert.assertNull(conn.getHeaderField(BridgeServer.HEADER_CONTENT_TYPE),
                "Should not have content type header");
        Assert.assertNull(conn.getHeaderField(BridgeServer.HEADER_CONTENT_ENCODING),
                "Should not have content encoding header");
        Assert.assertEquals(conn.getResponseCode(), HttpURLConnection.HTTP_OK);
        Assert.assertTrue(Stream.readAllAsString(conn.getInputStream()).length() > 0);

        // try again using POST
        conn = web.openConnection(new URL(url), config, headers);
        conn.setRequestMethod("POST");
        conn.setDoOutput(true);
        try (OutputStream out = conn.getOutputStream()) {
            Stream.writeAll(out, "select 1");
        }
        Assert.assertNull(conn.getHeaderField(BridgeServer.HEADER_CONTENT_TYPE),
                "Should not have content type header");
        Assert.assertNull(conn.getHeaderField(BridgeServer.HEADER_CONTENT_ENCODING),
                "Should not have content encoding header");
        Assert.assertEquals(conn.getResponseCode(), HttpURLConnection.HTTP_OK);
        Assert.assertTrue(Stream.readAllAsString(conn.getInputStream()).length() > 0);
    }

    @Test(groups = { "integration" })
    public void testSubmitAndRedirectQuery() throws Exception {
        final String url = getServerUrl();

        Properties config = new Properties();
        Map<String, String> headers = new HashMap<>();
        headers.put(BridgeServer.HEADER_ACCEPT, "application/bson");
        headers.put(BridgeServer.HEADER_ACCEPT_ENCODING, "xz,lz4,gzip");
        WebExecutor web = new WebExecutor(config);
        WebExecutor.OPTION_FOLLOW_REDIRECT.setValue(config, Constants.FALSE_EXPR);
        HttpURLConnection conn = web.openConnection(new URL(url + "?m=r&q=select+1"), config, headers);
        String queryUrl = conn.getHeaderField(BridgeServer.HEADER_LOCATION);
        Assert.assertNull(conn.getHeaderField(BridgeServer.HEADER_CONTENT_TYPE),
                "Should not have content type header");
        Assert.assertNull(conn.getHeaderField(BridgeServer.HEADER_CONTENT_ENCODING),
                "Should not have content encoding header");
        Assert.assertEquals(conn.getResponseCode(), HttpURLConnection.HTTP_MOVED_TEMP);
        Assert.assertEquals(Stream.readAllAsString(conn.getInputStream()), queryUrl);

        // try again using POST
        conn = web.openConnection(new URL(url + "?m=r"), config, headers);
        conn.setRequestMethod("POST");
        conn.setDoOutput(true);
        try (OutputStream out = conn.getOutputStream()) {
            Stream.writeAll(out, "select 1");
        }
        queryUrl = conn.getHeaderField(BridgeServer.HEADER_LOCATION);
        Assert.assertNull(conn.getHeaderField(BridgeServer.HEADER_CONTENT_TYPE),
                "Should not have content type header");
        Assert.assertNull(conn.getHeaderField(BridgeServer.HEADER_CONTENT_ENCODING),
                "Should not have content encoding header");
        Assert.assertEquals(conn.getResponseCode(), HttpURLConnection.HTTP_MOVED_TEMP);
        Assert.assertEquals(Stream.readAllAsString(conn.getInputStream()), queryUrl);
    }

    @Test(groups = { "integration" })
    public void testJdbcQuery() throws Exception {
        Properties props = new Properties();
        WrappedDriver d = new WrappedDriver();

        String uuid = UUID.randomUUID().toString();
        try (Connection conn = d.connect("jdbc:ch://" + getClickHouseServer(), props);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt
                        .executeQuery("select * from url('" + getServerUrl() + "?q=" + uuid + "','LineAsString')")) {
            final String qid = server.findIdOfCachedQuery(uuid);
            Assert.assertNotNull(qid, "Query should have been cached");
            Assert.assertEquals(rs.getMetaData().getColumnCount(), 1);
            Assert.assertTrue(rs.next());
            String queryUrl = rs.getString(1);
            Assert.assertTrue(queryUrl.startsWith("http"),
                    Utils.format("The returned URL should starts with http, but we got [%s]", queryUrl));
            Assert.assertTrue(rs.getString(1).indexOf(qid) > 0,
                    Utils.format("The returned URL should contacts query id [%s], but we got [%s]", qid, queryUrl));
            Assert.assertFalse(rs.next());
        }

        // try again using POST
        uuid = UUID.randomUUID().toString();
        try (Connection conn = d.connect("jdbcx:ch://" + getClickHouseServer(), props);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt
                        .executeQuery("select * from url('{{web(url.template=" + getServerUrl() + "):select '" + uuid
                                + "' r}}','TSVWithNames')")) {
            // final String qid = server.findIdOfCachedQuery(uuid);
            // Assert.assertNotNull(qid, "Query should have been cached");
            Assert.assertEquals(rs.getMetaData().getColumnCount(), 1);
            Assert.assertEquals(rs.getMetaData().getColumnName(1), "r");
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getString(1), uuid);
            // Assert.assertTrue(rs.getString(1).indexOf(qid) > 0);
            Assert.assertFalse(rs.next());
        }
    }
}
