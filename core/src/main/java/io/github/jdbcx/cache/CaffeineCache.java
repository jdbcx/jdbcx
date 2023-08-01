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

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;

import io.github.jdbcx.Cache;
import io.github.jdbcx.Checker;

/**
 * This cache is based on the Caffeine implementation. However, it's important
 * to note that it does not strictly adhere to the LRU (Least Recently Used)
 * cache eviction policy.
 */
public class CaffeineCache<K, V> implements Cache<K, V>, RemovalListener<K, V> {
    private final com.github.benmanes.caffeine.cache.Cache<K, V> cache;
    private final Function<K, V> loadFunc;

    /**
     * Creates a cache with given capacity, seconds to expire(after access), and
     * load function.
     *
     * @param <K>           type of the key
     * @param <V>           type of the value
     * @param capacity      capacity of the cache
     * @param expireSeconds seconds to expire after access
     * @param loadFunc      load function
     * @return cache
     */
    public static <K, V> Cache<K, V> create(int capacity, long expireSeconds, Function<K, V> loadFunc) {
        return new CaffeineCache<>(capacity, expireSeconds, loadFunc);
    }

    @SuppressWarnings("unchecked")
    protected CaffeineCache(int capacity, long expireSeconds, Function<K, V> loadFunc) {
        Caffeine<K, V> builder = (Caffeine<K, V>) Caffeine.newBuilder().maximumSize(capacity);
        if (expireSeconds > 0L) {
            builder.expireAfterAccess(expireSeconds, TimeUnit.SECONDS);
        }
        this.cache = builder.removalListener(this).build();
        this.loadFunc = Objects.requireNonNull(loadFunc, "Non-null load function is required");
    }

    @Override
    public void onRemoval(K key, V value, RemovalCause cause) {
        if (value instanceof AutoCloseable) {
            try {
                ((AutoCloseable) value).close();
            } catch (Exception e) {
                // ignore
            }
        }
    }

    @Override
    public V get(K key) {
        return cache.get(key, loadFunc);
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
        return Checker.nonNull(clazz, Class.class.getName()).cast(cache);
    }
}
