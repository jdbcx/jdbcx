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
package io.github.jdbcx.cache;

import java.util.HashMap;
import java.util.Map;

import io.github.jdbcx.Cache;

import org.testng.Assert;
import org.testng.annotations.Test;

public class JdkLruCacheTest {
    @Test(groups = { "unit" })
    public void testAutoClose() {
        Cache<String, AutoClosableResource> cache = JdkLruCache.create(2, AutoClosableResource::new);
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
        Assert.assertEquals(res2.closed.get(), false);
        Assert.assertEquals(res1.closed.get(), true);
    }

    @Test(groups = { "unit" })
    public void testCache() {
        int capacity = 3;
        Cache<String, String> cache = JdkLruCache.create(capacity, (k) -> k);
        Assert.assertNotNull(cache);

        Map<String, String> map = (Map<String, String>) cache.unwrap(Map.class);
        Assert.assertNotNull(map);
        Assert.assertEquals(map.size(), 0);

        Map<String, String> m = new HashMap<>();
        m.put("A", "A");
        m.put("B", "B");
        m.put("C", "C");
        Assert.assertEquals(cache.get("A"), "A");
        Assert.assertEquals(map.size(), 1);
        Assert.assertEquals(cache.get("B"), "B");
        Assert.assertEquals(map.size(), 2);
        Assert.assertEquals(cache.get("C"), "C");
        Assert.assertEquals(map.size(), 3);
        Assert.assertEquals(map, m);
        Assert.assertEquals(cache.get("D"), "D");
        Assert.assertEquals(map.size(), 3);
        Assert.assertNotEquals(map, m);
        m.remove("A");
        m.put("D", "D");
        Assert.assertEquals(map, m);
    }

    @Test(groups = { "unit" })
    public void testInvalidate() {
        Cache<String, String> cache = JdkLruCache.create(3, (k) -> k);
        Map<String, String> map = (Map<String, String>) cache.unwrap(Map.class);
        Assert.assertEquals(map.size(), 0);
        Assert.assertEquals(cache.get("test"), "test");
        Assert.assertTrue(map.containsKey("test"), "Should have the entry");
        cache.invalidate("test");
        Assert.assertEquals(map.size(), 0L);
        Assert.assertFalse(map.containsKey("test"), "Should not have the entry");
    }
}
