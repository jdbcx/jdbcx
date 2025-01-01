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

import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * This class represents a deferred value. It holds a reference to a
 * {@link Supplier} or {@link Future} to retrieve value only when {@link #get()}
 * or {@link #getOptional()} was called.
 */
public final class DeferredValue<T> implements Supplier<T> {
    public static final CompletableFuture<Void> NULL_FUTURE = CompletableFuture.completedFuture(null);
    public static final Supplier<?> NULL_SUPPLIER = () -> null;

    /**
     * Wraps a future object.
     *
     * @param <T>    type of the value
     * @param future future object, could be {@code null}
     * @return deferred value of a future object
     */
    public static <T> DeferredValue<T> of(CompletableFuture<T> future) {
        return of(future, 0L);
    }

    /**
     * Wraps a future object.
     *
     * @param <T>     type of the value
     * @param future  future object, could be {@code null}
     * @param timeout timeout in milliseconds, zero or negative number means no
     *                timeout
     * @return deferred vaue of a future object
     */
    @SuppressWarnings("unchecked")
    public static <T> DeferredValue<T> of(CompletableFuture<T> future, long timeout) {
        final CompletableFuture<T> f = future != null ? future : (CompletableFuture<T>) NULL_FUTURE;
        final long t = timeout < 0L ? 0L : timeout;

        final Supplier<T> supplier = () -> {
            try {
                return t > 0L ? f.get(t, TimeUnit.MILLISECONDS) : f.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                f.cancel(false);
                throw new CompletionException(e);
            } catch (CancellationException e) {
                f.cancel(true);
                throw new CompletionException(e);
            } catch (ExecutionException | TimeoutException e) {
                throw new CompletionException(e);
            }
        };
        return new DeferredValue<>(supplier, null);
    }

    /**
     * Wraps return value from a supplier function.
     *
     * @param <T>      type of the value
     * @param supplier supplier function, could be {@code null}
     * @return deferred value of return value from supplier function
     */
    @SuppressWarnings("unchecked")
    public static <T> DeferredValue<T> of(Supplier<T> supplier) {
        return new DeferredValue<>(supplier != null ? supplier : (Supplier<T>) NULL_SUPPLIER, null);
    }

    /**
     * Wraps given value as a deferred value.
     *
     * @param <T>   type of the value
     * @param value value to wrap
     * @param clazz class of the value
     * @return deferred value
     */
    @SuppressWarnings("unchecked")
    public static <T> DeferredValue<T> of(T value, Class<T> clazz) { // NOSONAR
        return new DeferredValue<>((Supplier<T>) NULL_SUPPLIER, Optional.ofNullable(value));
    }

    @SuppressWarnings("unchecked")
    public static <T> DeferredValue<T> of(Optional<T> value) { // NOSONAR
        return new DeferredValue<>((Supplier<T>) NULL_SUPPLIER, Checker.nonNull(value, "Value"));
    }

    private final Supplier<T> supplier;
    private final AtomicReference<Optional<T>> value;

    private DeferredValue(Supplier<T> supplier, Optional<T> value) {
        this.supplier = supplier;
        this.value = new AtomicReference<>(value);
    }

    @Override
    public T get() {
        return getOptional().orElse(null);
    }

    /**
     * Gets optional value, which may or may not be null.
     *
     * @return optional value
     */
    public Optional<T> getOptional() {
        Optional<T> v = value.get();
        if (v == null && !value.compareAndSet(null, v = Optional.ofNullable(supplier.get()))) { // NOSONAR
            v = value.get();
        }

        return v != null ? v : Optional.empty(); // NOSONAR
    }
}
