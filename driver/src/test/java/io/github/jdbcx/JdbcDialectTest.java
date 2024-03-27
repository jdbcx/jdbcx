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
package io.github.jdbcx;

import org.testng.Assert;
import org.testng.annotations.Test;

public class JdbcDialectTest {
    static class TestDefaultJdbcDialect implements JdbcDialect {
        @Override
        public ResultMapper getMapper() {
            return null;
        }
    }

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
    public void testDefaultImplmentation() {
        JdbcDialect dialect = new TestDefaultJdbcDialect();
        Assert.assertFalse(dialect.supports((Compression) null), "Should NOT support null compression");
        Assert.assertFalse(dialect.supports((Format) null), "Should NOT support null format");
        for (Compression c : Compression.values()) {
            Assert.assertTrue(dialect.supports(c), "Should support " + c);
        }
        for (Format f : Format.values()) {
            Assert.assertTrue(dialect.supports(f), "Should support " + f);
        }
        Assert.assertEquals(dialect.getPreferredCompression(), Compression.NONE);
        Assert.assertEquals(dialect.getPreferredFormat(), Format.CSV);
    }

    @Test(groups = { "unit" })
    public void testGetEncodings() {
        final TestJdbcDialect dialect = new TestJdbcDialect();
        Assert.assertEquals(dialect.getEncodings(null), "zstd");

        Assert.assertEquals(dialect.getEncodings(Compression.NONE), "identity;zstd");
        Assert.assertEquals(dialect.getEncodings(Compression.GZIP), "zstd");
    }

    @Test(groups = { "unit" })
    public void testGetMimeTypes() {
        final TestJdbcDialect dialect = new TestJdbcDialect();
        Assert.assertEquals(dialect.getMimeTypes(null), "application/vnd.apache.avro+json");

        Assert.assertEquals(dialect.getMimeTypes(Format.CSV), "text/csv;application/vnd.apache.avro+json");
        Assert.assertEquals(dialect.getMimeTypes(Format.PARQUET), "application/vnd.apache.avro+json");
    }
}
