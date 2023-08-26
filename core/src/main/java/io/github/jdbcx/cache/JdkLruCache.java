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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;

import io.github.jdbcx.Cache;
import io.github.jdbcx.Checker;

/**
 * This is a simple thread-safe LRU (Least Recently Used) cache implementation
 * based on LinkedHashMap. Although it may not be as efficient as the one
 * provided by Caffeine or Guava libraries, it does not require any additional
 * dependencies.
 */
public class JdkLruCache<K, V> implements Cache<K, V> {
    static class LruCacheMap<K, V> extends LinkedHashMap<K, V> { // NOSONAR
        private final int capacity;

        protected LruCacheMap(int capacity) {
            super(capacity, 0.75f, true);

            this.capacity = capacity;
        }

        @Override
        protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
            if (size() > capacity) {
                V value = eldest != null ? eldest.getValue() : null;
                if (value instanceof AutoCloseable) {
                    try {
                        ((AutoCloseable) value).close();
                    } catch (Exception e) {
                        // ignore
                    }
                }
                return true;
            }

            return false;
        }
    }

    /**
     * Creates a cache with given capacity and load function.
     *
     * @param <K>      type of the key
     * @param <V>      type of the value
     * @param capacity capacity
     * @param loadFunc load function
     * @return cache
     */
    public static <K, V> Cache<K, V> create(int capacity, Function<K, V> loadFunc) {
        return new JdkLruCache<>(new LruCacheMap<>(capacity), Checker.nonNull(loadFunc, Function.class));
    }

    private final Map<K, V> cache;
    private final Function<K, V> loadFunc;

    protected JdkLruCache(Map<K, V> cache, Function<K, V> loadFunc) {
        this.cache = Collections.synchronizedMap(cache);
        this.loadFunc = loadFunc;
    }

    @Override
    public V get(K key) {
        return cache.computeIfAbsent(key, loadFunc);
    }

    @Override
    public void invalidate(K key) {
        cache.remove(key);
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
        return Checker.nonNull(clazz, Class.class.getName()).cast(cache);
    }
}
