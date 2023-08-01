/*
 * Copyright 2022-2023, Zhichun Wu
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
package io.github.jdbcx.cache;

import java.util.HashMap;
import java.util.Map;

import com.github.benmanes.caffeine.cache.Cache;

import org.testng.Assert;
import org.testng.annotations.Test;

public class CaffeineCacheTest {
    @Test(groups = { "unit" })
    public void testAutoClose() {
        io.github.jdbcx.Cache<String, AutoClosableResource> cache = io.github.jdbcx.cache.CaffeineCache.create(2, 0,
                AutoClosableResource::new);
        AutoClosableResource res1 = cache.get("1");
        Assert.assertEquals(res1.key, "1");
        Assert.assertEquals(res1.closed.get(), false);

        AutoClosableResource res2 = cache.get("2");
        Assert.assertEquals(res2.key, "2");
        Assert.assertEquals(res2.closed.get(), false);
        Assert.assertEquals(res1.closed.get(), false);

        AutoClosableResource res3 = cache.get("3");
        Assert.assertEquals(res3.key, "3");
        Assert.assertEquals(res3.closed.get(), false);
        Cache<String, AutoClosableResource> c = (Cache<String, AutoClosableResource>) cache.unwrap(Cache.class);
        c.cleanUp();
        Assert.assertEquals(res3.closed.get(), false);
        Assert.assertEquals(res2.closed.get() || res1.closed.get(), true);
        Assert.assertEquals(c.asMap().size(), 2);
    }

    @Test(groups = { "unit" })
    public void testCache() {
        int capacity = 3;
        io.github.jdbcx.Cache<String, String> cache = io.github.jdbcx.Cache.create(capacity, 1L, (k) -> k);
        Assert.assertNotNull(cache);

        Cache<String, String> c = (Cache<String, String>) cache.unwrap(Cache.class);
        Assert.assertNotNull(c);
        Assert.assertEquals(c.estimatedSize(), 0L);

        Map<String, String> m = new HashMap<>();
        m.put("A", "A");
        m.put("B", "B");
        m.put("C", "C");
        Assert.assertEquals(cache.get("A"), "A");
        Assert.assertEquals(c.asMap().size(), 1);
        Assert.assertEquals(cache.get("B"), "B");
        Assert.assertEquals(c.asMap().size(), 2);
        Assert.assertEquals(cache.get("C"), "C");
        Assert.assertEquals(c.asMap().size(), 3);
        Assert.assertEquals(c.asMap(), m);

        try {
            Thread.sleep(1500L);
        } catch (InterruptedException e) {
            Assert.fail("Sleep was interrupted", e);
        }
        c.cleanUp();

        Assert.assertEquals(cache.get("D"), "D");
        Assert.assertEquals(c.asMap().size(), 1);
        Assert.assertNotEquals(c.asMap(), m);
        m.clear();
        m.put("D", "D");
        Assert.assertEquals(c.asMap(), m);
    }
}
