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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.github.jdbcx.Compression;
import io.github.jdbcx.Format;
import io.github.jdbcx.Result;

public class QueryInfoTest {
    static class TestResource implements AutoCloseable {
        final AtomicBoolean closed = new AtomicBoolean(false);

        @Override
        public void close() {
            closed.compareAndSet(false, true);
        }
    }

    static class TroublesomeResource implements AutoCloseable {
        @Override
        public void close() {
            throw new IllegalAccessError();
        }
    }

    @Test(groups = { "unit" })
    public void testConstructor() {
        try (QueryInfo info = new QueryInfo(null, null, null, null, null, null, null, null)) {
            Assert.assertTrue(info.qid.length() > 0);
            Assert.assertEquals(info.query, "");
            Assert.assertEquals(info.txid, "");
            Assert.assertEquals(info.format, Format.TSV);
            Assert.assertEquals(info.compress, Compression.NONE);
            Assert.assertEquals(info.token, "");
            Assert.assertEquals(info.user, "");
            Assert.assertEquals(info.client, "");
            Assert.assertNull(info.getResult());
            Assert.assertNull(info.getResources());

            Assert.assertThrows(IllegalArgumentException.class, () -> info.setResult(null));
            Assert.assertNull(info.getResult());
            Assert.assertThrows(IllegalArgumentException.class, () -> info.setResources((AutoCloseable[]) null));
            Assert.assertNull(info.getResources());
            Assert.assertThrows(IllegalArgumentException.class, () -> info.setResources());
            Assert.assertNull(info.getResources());
            Assert.assertThrows(IllegalArgumentException.class, () -> info.setResources((AutoCloseable) null));
            Assert.assertNull(info.getResources());
            Assert.assertThrows(IllegalArgumentException.class, () -> info.setResources(null, null));
            Assert.assertNull(info.getResources());
        }

        final String id1 = UUID.randomUUID().toString();
        final String id2 = UUID.randomUUID().toString();
        try (QueryInfo info = new QueryInfo(id1, "select 5", id2, Format.AVRO_BINARY, Compression.SNAPPY, "233",
                "zhicwu", "JDBCX/1.0")) {
            Assert.assertEquals(info.qid, id1);
            Assert.assertEquals(info.query, "select 5");
            Assert.assertEquals(info.txid, id2);
            Assert.assertEquals(info.format, Format.AVRO_BINARY);
            Assert.assertEquals(info.compress, Compression.SNAPPY);
            Assert.assertEquals(info.token, "233");
            Assert.assertEquals(info.user, "zhicwu");
            Assert.assertEquals(info.client, "JDBCX/1.0");
            Assert.assertNull(info.getResult());
            Assert.assertNull(info.getResources());

            info.setResult(Result.of("123"));
            Assert.assertNotNull(info.getResult());
            Assert.assertEquals(info.getResult().get(), "123");
            Assert.assertThrows(IllegalArgumentException.class, () -> info.setResult(null));
            Assert.assertThrows(IllegalStateException.class, () -> info.setResult(Result.of("321")));
            Assert.assertNotNull(info.getResult());
            Assert.assertEquals(info.getResult().get(), "123");

            AutoCloseable res1 = new TestResource();
            AutoCloseable res2 = new TroublesomeResource();
            AutoCloseable res3 = new TestResource();
            info.setResources(null, res1, null, null, res2, null, null, null, res3, null);
            Assert.assertThrows(IllegalArgumentException.class, () -> info.setResources((AutoCloseable[]) null));
            Assert.assertThrows(IllegalArgumentException.class, () -> info.setResources());
            Assert.assertThrows(IllegalStateException.class, () -> info.setResources(res1));
            Assert.assertEquals(info.getResources(), new AutoCloseable[] { res1, res2, res3 });
        }
    }

    @Test(groups = { "unit" })
    public void testClose() throws SQLException {
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite::memory:");
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("select 1")) {
            Result<?> res = Result.of(rs);
            QueryInfo info = new QueryInfo(null, null, null, null, null, null, null, null);
            info.setResult(res);
            Assert.assertFalse(rs.isClosed());
            Assert.assertFalse(stmt.isClosed());
            Assert.assertFalse(conn.isClosed());

            info.close();
            Assert.assertTrue(rs.isClosed());
            Assert.assertFalse(stmt.isClosed());
            Assert.assertFalse(conn.isClosed());

            info.setResources(rs, stmt, conn);
            info.close();
            Assert.assertTrue(rs.isClosed());
            Assert.assertTrue(stmt.isClosed());
            Assert.assertTrue(conn.isClosed());
        }
    }
}
