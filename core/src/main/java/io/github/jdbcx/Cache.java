/*
 * Copyright 2022-2025, Zhichun Wu
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

import java.util.function.Function;

import io.github.jdbcx.cache.CaffeineCache;
import io.github.jdbcx.cache.JdkLruCache;

/**
 * This interface defines a simple in-memory cache with a specified capacity.
 */
public interface Cache<K, V> {
    /**
     * Default cache size.
     */
    static final int DEFAULT_CACHE_SIZE = 50;

    /**
     * Creates a cache with a specific capacity and a load function.
     *
     * @param <K>           type of the key
     * @param <V>           type of the value
     * @param capacity      capacity of the cache, a negative number or zero is
     *                      considered as {@link #DEFAULT_CACHE_SIZE} for the cache
     * @param expireSeconds seconds the cache entry will expire since last access,
     *                      zero or negative number means never expire
     * @param loadFunc      non-null load function
     * @return in-memory cache
     */
    static <K, V> Cache<K, V> create(int capacity, long expireSeconds, Function<K, V> loadFunc) {
        if (capacity <= 0) {
            capacity = DEFAULT_CACHE_SIZE;
        }

        Cache<K, V> cache;
        try {
            cache = CaffeineCache.create(capacity, expireSeconds, loadFunc);
        } catch (Throwable e) { // NOSONAR
            cache = JdkLruCache.create(capacity, loadFunc);
        }
        return cache;
    }

    /**
     * Gets value from cache if it exists.
     *
     * @param key key, usually not null
     * @return value, usually not null
     */
    V get(K key);

    /**
     * Invalidates any cached value for the {@code key}.
     *
     * @param key key, usually not null
     */
    void invalidate(K key);

    /**
     * Unwraps the inner cache object, providing additional access and the ability
     * to manipulate it directly.
     *
     * @param <T>   type of the cache
     * @param clazz non-null class of the cache
     * @return non-null inner cache object
     * @throws NullPointerException when {@code clazz} is null
     */
    <T> T unwrap(Class<T> clazz);
}
