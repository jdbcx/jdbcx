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

import org.testng.Assert;
import org.testng.annotations.Test;

import io.github.jdbcx.Compression;
import io.github.jdbcx.Format;

public class RequestTest {
    @Test(groups = { "unit" })
    public void testConstructor() {
        Request request = new Request(null, null, null, null, null, null, null, null);
        Assert.assertFalse(request.hasQueryId());
        Assert.assertFalse(request.isMutation());
        Assert.assertFalse(request.isTransactional());
        Assert.assertEquals(request.getMethod(), "");
        Assert.assertEquals(request.getQueryMode(), QueryMode.SUBMIT_QUERY);
        Assert.assertEquals(request.getQuery(), "");
        Assert.assertEquals(request.getCompression(), Compression.NONE);
        Assert.assertEquals(request.getFormat(), Format.TSV);
        String qid = request.getQueryId();
        Assert.assertTrue(qid.length() > 0);
        Assert.assertEquals(request.getQueryId(), qid);
        Assert.assertNull(request.getUserObject(Object.class));

        request = new Request("HEAD", QueryMode.MUTATION, "123", "321", "tx1", Format.AVROB, Compression.SNAPPY,
                request);
        Assert.assertTrue(request.hasQueryId());
        Assert.assertTrue(request.isMutation());
        Assert.assertTrue(request.isTransactional());
        Assert.assertEquals(request.getMethod(), "HEAD");
        Assert.assertEquals(request.getQueryMode(), QueryMode.MUTATION);
        Assert.assertEquals(request.getQuery(), "321");
        Assert.assertEquals(request.getQueryId(), "123");
        Assert.assertEquals(request.getCompression(), Compression.SNAPPY);
        Assert.assertEquals(request.getFormat(), Format.AVROB);
        Assert.assertNotNull(request.getUserObject(Request.class));
    }

    @Test(groups = { "unit" })
    public void testToUrl() {
        Request request = new Request(null, null, null, null, null, null, null, null);
        Assert.assertEquals(request.toUrl(), request.getQueryId() + request.getFormat().fileExtension());
        Assert.assertEquals(request.toUrl(""), request.getQueryId() + request.getFormat().fileExtension());
        Assert.assertEquals(request.toUrl("/"), "/" + request.getQueryId() + request.getFormat().fileExtension());
        request = new Request(null, QueryMode.DIRECT_QUERY, null, null, null, Format.BSON, Compression.LZ4, null);
        Assert.assertEquals(request.toUrl(),
                request.getQueryId() + request.getFormat().fileExtension() + request.getCompression().fileExtension());
        request = new Request("POST", QueryMode.MUTATION, "123", "321", "567", Format.BSON, Compression.LZ4, null);
        Assert.assertEquals(request.toUrl(), "123.bson.lz4");
        Assert.assertEquals(request.toUrl("http://localhost/"), "http://localhost/123.bson.lz4");
    }
}
