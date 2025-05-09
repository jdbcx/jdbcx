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

import java.util.function.Predicate;

public interface ResourceManager {
    /**
     * Attaches a resource to this connection. If successfully attached, the
     * resource will be closed automatically when this connection is closed.
     *
     * @param resource The closeable resource to attach to this connection.
     * @return The same closeable resource being attached to this connection.
     */
    default <T extends AutoCloseable> T add(T resource) {
        return resource;
    }

    /**
     * Gets the first attached resource that is an instance of the given class
     * from the list of resources attached to this connection. Same as
     * {@code get(clazz, null)}.
     *
     * @param <T>   The type of the closeable resource to retrieve.
     * @param clazz The {@link Class} object representing the type of the resource
     *              to retrieve.
     * @return The first attached resource that matches the given type, or
     *         {@code null} if no matching resource is found or if the default
     *         implementation is used.
     */
    default <T extends AutoCloseable> T get(Class<T> clazz) {
        return get(clazz, null);
    }

    /**
     * Gets the first attached resource that satisfies the given class and filter
     * from the list of resources attached to this connection.
     *
     * @param <T>    The type of the closeable resource to retrieve.
     * @param clazz  The {@link Class} object representing the type of the resource
     *               to retrieve.
     * @param filter The predicate to apply to attached resources to find a match.
     * @return The first attached resource of type {@code T} that satisfies the
     *         filter, or {@code null} if no matching resource is found, no
     *         resources are attached, or if the default implementation is used.
     */
    default <T extends AutoCloseable> T get(Class<T> clazz, Predicate<T> filter) {
        return null;
    }
}
