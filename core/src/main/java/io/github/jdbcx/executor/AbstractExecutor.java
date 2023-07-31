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
package io.github.jdbcx.executor;

import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import io.github.jdbcx.Constants;
import io.github.jdbcx.Executor;
import io.github.jdbcx.Option;

abstract class AbstractExecutor implements Executor {
    static final ExecutorService newThreadPool(Object owner, int maxThreads, int maxRequests) {
        return newThreadPool(owner, maxThreads, 0, maxRequests, 0L, true);
    }

    static final ExecutorService newThreadPool(Object owner, int coreThreads, int maxThreads, int maxRequests,
            long keepAliveTimeoutMs, boolean allowCoreThreadTimeout) {
        final BlockingQueue<Runnable> queue;
        if (coreThreads < Constants.MIN_CORE_THREADS) {
            coreThreads = Constants.MIN_CORE_THREADS;
        }
        if (maxRequests > 0) {
            queue = new ArrayBlockingQueue<>(maxRequests);
            if (maxThreads <= coreThreads) {
                maxThreads = coreThreads * 2;
            }
        } else {
            queue = new LinkedBlockingQueue<>();
            if (maxThreads != coreThreads) {
                maxThreads = coreThreads;
            }
        }
        if (keepAliveTimeoutMs <= 0L) {
            keepAliveTimeoutMs = allowCoreThreadTimeout ? 1000L : 0L;
        }

        ThreadPoolExecutor pool = new ThreadPoolExecutor(coreThreads, maxThreads, keepAliveTimeoutMs,
                TimeUnit.MILLISECONDS, queue, new CustomThreadFactory(owner), new ThreadPoolExecutor.AbortPolicy());
        if (allowCoreThreadTimeout) {
            pool.allowCoreThreadTimeOut(true);
        }
        return pool;
    }

    protected static final ExecutorService executor;
    protected static final ScheduledExecutorService scheduler;

    static {
        int coreThreads = 2 * Runtime.getRuntime().availableProcessors() + 1;
        if (coreThreads < Constants.MIN_CORE_THREADS) {
            coreThreads = Constants.MIN_CORE_THREADS;
        }

        executor = newThreadPool("JdbcxWorker-", coreThreads, coreThreads, 0, 0, false);
        scheduler = Executors.newSingleThreadScheduledExecutor(new CustomThreadFactory("JdbcxScheduler-"));
    }

    protected final String defaultErrorHandling;
    protected final int defaultParallelism;
    protected final int defaultTimeout;

    protected AbstractExecutor(Properties props) {
        this.defaultErrorHandling = Option.EXEC_ERROR.getValue(props);
        this.defaultParallelism = Integer.parseInt(Option.EXEC_PARALLELISM.getValue(props));
        this.defaultTimeout = Integer.parseInt(Option.EXEC_TIMEOUT.getValue(props));
    }

    public final CompletableFuture<Void> runAsync(Runnable runnable) {
        if (defaultParallelism <= 0) {
            try {
                runnable.run();
                return CompletableFuture.completedFuture(null);
            } catch (CompletionException e) {
                // CompletableFuture.failedFuture(e) requires JDK 9+
                CompletableFuture<Void> future = new CompletableFuture<>();
                future.completeExceptionally(e.getCause());
                return future;
            }
        }
        return CompletableFuture.runAsync(runnable, executor);
    }

    public final <T> CompletableFuture<T> supplyAsync(Supplier<T> supplier) {
        if (defaultParallelism <= 0) {
            return CompletableFuture.completedFuture(supplier.get());
        }
        return CompletableFuture.supplyAsync(supplier, executor);
    }

    public final int getDefaultTimeout() {
        return defaultTimeout;
    }

    public final boolean ignoreError() {
        return Option.ERROR_HANDLING_IGNORE.equals(defaultErrorHandling);
    }

    public final boolean throwExceptionOnError() {
        return Option.ERROR_HANDLING_THROW.equals(defaultErrorHandling);
    }

    public final boolean warnOnError() {
        return Option.ERROR_HANDLING_WARN.equals(defaultErrorHandling);
    }

    public final boolean returnOnError() {
        return Option.ERROR_HANDLING_RETURN.equals(defaultErrorHandling);
    }
}
