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
package io.github.jdbcx.server;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.github.jdbcx.Compression;
import io.github.jdbcx.Format;
import io.github.jdbcx.QueryMode;
import io.github.jdbcx.Result;

public class RequestTest {
    @Test(groups = { "unit" })
    public void testConstructor() {
        Request request = new Request(null, null, null, null, null, null, null, null, null, null, null, null, null,
                null);
        Assert.assertFalse(request.hasQueryId());
        Assert.assertFalse(request.isMutation());
        Assert.assertFalse(request.isTransactional());
        Assert.assertEquals(request.getMethod(), "");
        Assert.assertEquals(request.getQueryMode(), QueryMode.SUBMIT);
        Assert.assertEquals(request.getQuery(), "");
        Assert.assertEquals(request.getCompression(), Compression.NONE);
        Assert.assertEquals(request.getFormat(), Format.TSV);
        String qid = request.getQueryId();
        Assert.assertTrue(qid.length() > 0);
        Assert.assertEquals(request.getQueryId(), qid);
        Assert.assertNull(request.getImplementation(Object.class));
        Assert.assertEquals(request.getTenant(), "");

        request = new Request("HEAD", QueryMode.MUTATION, "a=1&b=2", "123", "321", "tx1", Format.AVRO_JSON,
                Compression.SNAPPY, null, null, null, "tt", null, request);
        Assert.assertTrue(request.hasQueryId());
        Assert.assertTrue(request.isMutation());
        Assert.assertTrue(request.isTransactional());
        Assert.assertEquals(request.getMethod(), "HEAD");
        Assert.assertEquals(request.getQueryMode(), QueryMode.MUTATION);
        Assert.assertEquals(request.getRawParameters(), "a=1&b=2");
        Assert.assertEquals(request.getQuery(), "321");
        Assert.assertEquals(request.getQueryId(), "123");
        Assert.assertEquals(request.getCompression(), Compression.SNAPPY);
        Assert.assertEquals(request.getFormat(), Format.AVRO_JSON);
        Assert.assertNotNull(request.getImplementation(Request.class));
        Assert.assertEquals(request.getTenant(), "tt");
    }

    @Test(groups = { "unit" })
    public void testResult() {
        Request request = new Request(null, null, null, null, null, null, null, null, null, null, null, null, null,
                null);
        Assert.assertFalse(request.hasResult(), "Should not have result");
        Assert.assertNull(request.getQueryInfo().getResult(), "Should not have result");
        Assert.assertEquals(request.getResultState(), 0);

        request.getQueryInfo().setResult(Result.of(1L));
        Assert.assertTrue(request.hasResult(), "Should have result");
        Assert.assertNotNull(request.getQueryInfo().getResult(), "Should have result");
        Assert.assertEquals(request.getResultState(), 1);

        request.getQueryInfo().getResult().rows();
        Assert.assertTrue(request.hasResult(), "Should still have result");
        Assert.assertNotNull(request.getQueryInfo().getResult(), "Should still have result");
        Assert.assertEquals(request.getResultState(), -1);
    }

    @Test(groups = { "unit" })
    public void testToUrl() {
        final String baseUrl = "http://localhost:1234/";
        Request request = new Request(null, null, null, null, null, null, null, null, null, null, null, null, null,
                null);
        Assert.assertEquals(request.toUrl(baseUrl),
                baseUrl + request.getQueryId() + request.getFormat().fileExtension());
        Assert.assertEquals(request.toUrl(""), request.getQueryId() + request.getFormat().fileExtension());
        Assert.assertEquals(request.toUrl("/"), "/" + request.getQueryId() + request.getFormat().fileExtension());
        request = new Request(null, QueryMode.DIRECT, null, null, null, null, Format.BSON, Compression.LZ4, null, null,
                null, null, null, null);
        Assert.assertEquals(request.toUrl(baseUrl),
                baseUrl + request.getQueryId() + request.getFormat().fileExtension()
                        + request.getCompression().fileExtension());
        request = new Request("POST", QueryMode.MUTATION, "a=1", "123", "321", "567", Format.BSON, Compression.LZ4,
                null, null, null, null, null, null);
        Assert.assertEquals(request.toUrl(baseUrl), baseUrl + "123.bson.lz4");
        Assert.assertEquals(request.toUrl("http://localhost/"), "http://localhost/123.bson.lz4");
    }
}
