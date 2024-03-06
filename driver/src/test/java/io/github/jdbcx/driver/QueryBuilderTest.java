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
package io.github.jdbcx.driver;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.github.jdbcx.Compression;
import io.github.jdbcx.Format;
import io.github.jdbcx.JdbcDialect;
import io.github.jdbcx.Option;
import io.github.jdbcx.QueryContext;
import io.github.jdbcx.ResultMapper;
import io.github.jdbcx.WrappedDriver;

public class QueryBuilderTest {
    static class TestJdbcDialect implements JdbcDialect {
        @Override
        public Compression getPreferredCompression() {
            return Compression.ZSTD;
        }

        @Override
        public Format getPreferredFormat() {
            return Format.AVRO_JSON;
        }

        @Override
        public boolean supports(Compression compress) {
            return compress != null && compress != Compression.GZIP;
        }

        @Override
        public boolean supports(Format format) {
            return format != null && format != Format.PARQUET;
        }

        @Override
        public ResultMapper getMapper() {
            return null;
        }
    }

    @Test(groups = { "unit" })
    public void testGetEncodings() {
        final TestJdbcDialect dialect = new TestJdbcDialect();
        Assert.assertEquals(QueryBuilder.getEncodings(dialect, null), "zstd;identity");

        Properties config = new Properties();
        Assert.assertEquals(QueryBuilder.getEncodings(dialect, config), "zstd;identity");
        Option.SERVER_COMPRESSION.setValue(config, "GZIP");
        Assert.assertEquals(QueryBuilder.getEncodings(dialect, config), "zstd");
    }

    @Test(groups = { "unit" })
    public void testGetMimeTypes() {
        final TestJdbcDialect dialect = new TestJdbcDialect();
        Assert.assertEquals(QueryBuilder.getMimeTypes(dialect, null), "application/vnd.apache.avro+json;text/csv");

        Properties config = new Properties();
        Assert.assertEquals(QueryBuilder.getMimeTypes(dialect, config), "application/vnd.apache.avro+json;text/csv");
        Option.SERVER_FORMAT.setValue(config, "PARQUET");
        Assert.assertEquals(QueryBuilder.getMimeTypes(dialect, config), "application/vnd.apache.avro+json");
    }

    @Test(groups = { "unit" })
    public void testBuild() throws SQLException {
        String url = "jdbcx:sqlite::memory:";
        Properties props = new Properties();
        try (WrappedConnection conn = (WrappedConnection) new WrappedDriver().connect(url, props);
                WrappedStatement stmt = conn.createStatement()) {
            Assert.assertTrue(conn instanceof WrappedConnection, "Should return wrapped connection");
            try (WrappedConnection c = (WrappedConnection) conn; QueryContext context = QueryContext.newContext()) {
                ParsedQuery pq = QueryParser.parse("select {{ rhino: ['1','2','3']}}", null, props);
                QueryBuilder builder = new QueryBuilder(context, pq, conn.manager, stmt.queryResult);
                Assert.assertEquals(builder.build(), Arrays.asList("select 1", "select 2", "select 3"));
            }

            try (WrappedConnection c = (WrappedConnection) conn; QueryContext context = QueryContext.newContext()) {
                ParsedQuery pq = QueryParser
                        .parse("select {{ rhino: ['1','2','3']}} + {{ rhino: ['4', '5']}}", null, props);
                QueryBuilder builder = new QueryBuilder(context, pq, conn.manager, stmt.queryResult);
                Assert.assertEquals(builder.build(), Arrays.asList("select 1 + 4", "select 2 + 4", "select 3 + 4",
                        "select 1 + 5", "select 2 + 5", "select 3 + 5"));
            }

            try (WrappedConnection c = (WrappedConnection) conn; QueryContext context = QueryContext.newContext()) {
                ParsedQuery pq = QueryParser
                        .parse("select {{ rhino: ['1','2','3']}} + {{ rhino: ['4', '5']}} as {{ rhino: 'a' }}", null,
                                props);
                QueryBuilder builder = new QueryBuilder(context, pq, conn.manager, stmt.queryResult);
                Assert.assertEquals(builder.build(),
                        Arrays.asList("select 1 + 4 as a", "select 2 + 4 as a", "select 3 + 4 as a",
                                "select 1 + 5 as a", "select 2 + 5 as a", "select 3 + 5 as a"));
            }

            try (WrappedConnection c = (WrappedConnection) conn; QueryContext context = QueryContext.newContext()) {
                ParsedQuery pq = QueryParser
                        .parse("select {{ rhino: [1,2] }} + {{ rhino: [3,4,5] }} + {{rhino:[1,2]}}", null, props);
                QueryBuilder builder = new QueryBuilder(context, pq, conn.manager, stmt.queryResult);
                Assert.assertEquals(builder.build(), Arrays.asList("select 1 + 3 + 1", "select 2 + 3 + 2",
                        "select 1 + 4 + 1", "select 2 + 4 + 2", "select 1 + 5 + 1", "select 2 + 5 + 2"));
            }
        }
    }
}
