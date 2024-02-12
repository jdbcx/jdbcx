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
package io.github.jdbcx.executor;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import io.github.jdbcx.Checker;
import io.github.jdbcx.Constants;
import io.github.jdbcx.Executor;
import io.github.jdbcx.Logger;
import io.github.jdbcx.Option;
import io.github.jdbcx.Utils;

abstract class AbstractExecutor implements Executor {
    static final void cancelAllTasks(Logger log, CompletableFuture<?>[] tasks) {
        if (tasks == null) {
            return;
        }

        for (CompletableFuture<?> future : tasks) {
            if (future == null) {
                // do nothing
            } else if (future.isDone()) {
                try {
                    log.debug("Task completed with result [%s]", future.get());
                } catch (Throwable e) { // NOSONAR
                    log.debug("Task completed with exception", e);
                }
            } else {
                log.debug("Request to cancel the running task...");
                future.cancel(true);
            }
        }
    }

    static final long checkTimeout(Logger log, long startTimeMs, long timeoutMs, CompletableFuture<?>... tasks)
            throws TimeoutException {
        if (startTimeMs <= 0L) {
            return timeoutMs;
        }

        long elapsed = System.currentTimeMillis() - startTimeMs;
        log.debug("current timeout is %d ms, elapsed %d since %d", timeoutMs, elapsed, startTimeMs);

        if (elapsed >= timeoutMs) {
            cancelAllTasks(log, tasks);
            throw new TimeoutException(Utils.format("Timed out after waiting for %d ms", elapsed));
        }
        return timeoutMs - elapsed;
    }

    static final Object waitForTask(Logger log, CompletableFuture<?> future, long startTime, long timeoutMs)
            throws IOException, TimeoutException {
        if (future == null) {
            return null;
        }

        log.debug("Waiting %d ms for task [%s] to complete", timeoutMs, future);
        try {
            // respect timeout setting, even if future.isDone()
            return timeoutMs <= 0L ? future.get()
                    : future.get(checkTimeout(log, startTime, timeoutMs), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new InterruptedIOException(e.getMessage());
        } catch (CancellationException e) {
            throw new InterruptedIOException(e.getMessage());
        } catch (ExecutionException e) {
            Throwable t = e.getCause();
            if (t instanceof TimeoutException) {
                throw (TimeoutException) t;
            } else {
                throw new IOException(t);
            }
        } catch (TimeoutException e) {
            future.cancel(true);
            if (e.getMessage() != null) {
                throw e;
            } else {
                throw new TimeoutException(Utils.format("Timed out after waiting for more than %d ms", timeoutMs));
            }
        }
    }

    static final TimeoutException handleTimeout(Logger log, int timeout, CompletableFuture<?>... tasks) {
        log.error("Execution timed out after waiting for %d ms", timeout);
        cancelAllTasks(log, tasks);
        return new TimeoutException(
                Utils.format("The execution was interrupted due to a timeout after waiting for %d ms.", timeout));
    }

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

    private static final ExecutorService executor;
    private static final ScheduledExecutorService scheduler;

    static {
        int coreThreads = 2 * Runtime.getRuntime().availableProcessors() + 1;
        if (coreThreads < Constants.MIN_CORE_THREADS) {
            coreThreads = Constants.MIN_CORE_THREADS;
        }

        executor = newThreadPool("JdbcxWorker-", coreThreads, coreThreads, 0, 0, false);
        scheduler = Executors.newSingleThreadScheduledExecutor(new CustomThreadFactory("JdbcxScheduler-"));
    }

    protected final boolean defaultDryRun;
    protected final String defaultErrorHandling;
    protected final Charset defaultInputCharset;
    protected final Charset defaultOutputCharset;
    protected final int defaultParallelism;
    protected final int defaultTimeout;
    protected final Path defaultWorkDir;

    protected AbstractExecutor(Properties props) {
        String inputCharset = Option.INPUT_CHARSET.getValue(props);
        String outputCharset = Option.OUTPUT_CHARSET.getValue(props);
        String workDir = Option.WORK_DIRECTORY.getValue(props);

        this.defaultDryRun = Boolean.parseBoolean(Option.EXEC_DRYRUN.getValue(props));
        this.defaultErrorHandling = Option.EXEC_ERROR.getValue(props);
        this.defaultInputCharset = Checker.isNullOrBlank(inputCharset) ? Charset.forName(inputCharset)
                : Constants.DEFAULT_CHARSET;
        this.defaultOutputCharset = Checker.isNullOrBlank(outputCharset) ? Charset.forName(outputCharset)
                : Constants.DEFAULT_CHARSET;
        this.defaultParallelism = Integer.parseInt(Option.EXEC_PARALLELISM.getValue(props));
        this.defaultTimeout = Integer.parseInt(Option.EXEC_TIMEOUT.getValue(props));
        if (Checker.isNullOrEmpty(workDir)) {
            this.defaultWorkDir = Paths.get(Constants.CURRENT_DIR);
        } else {
            this.defaultWorkDir = Utils.getPath(workDir, true);
        }
    }

    protected final CompletableFuture<Void> run(Runnable runnable, int parallelism) {
        if (parallelism <= 0) {
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

        return runAsync(runnable);
    }

    protected final CompletableFuture<Void> runAsync(Runnable runnable) {
        return CompletableFuture.runAsync(runnable, executor);
    }

    protected final <T> CompletableFuture<T> supply(Supplier<T> supplier, Properties props) {
        int parallelism = Integer.parseInt(Option.EXEC_PARALLELISM.getValue(props));
        return supply(supplier, parallelism);
    }

    protected final <T> CompletableFuture<T> supply(Supplier<T> supplier, int parallelism) {
        if (parallelism <= 0) {
            try {
                return CompletableFuture.completedFuture(supplier.get());
            } catch (CompletionException e) {
                // CompletableFuture.failedFuture(e) requires JDK 9+
                CompletableFuture<T> future = new CompletableFuture<>();
                future.completeExceptionally(e.getCause());
                return future;
            }
        }
        return supplyAsync(supplier);
    }

    protected final <T> CompletableFuture<T> supplyAsync(Supplier<T> supplier) {
        return CompletableFuture.supplyAsync(supplier, executor);
    }

    /**
     * Schedules the {@code task} to run only when {@code current} run has been
     * completed/cancelled or does not exist.
     *
     * @param current  current task, which could be {@code null} or in running
     *                 status
     * @param task     scheduled task to run
     * @param interval interval between each run
     * @return future object representing the scheduled task, could be {@code null}
     *         when no scheduler available
     */
    protected final ScheduledFuture<?> schedule(ScheduledFuture<?> current, Runnable task, long interval) {
        if (scheduler == null || task == null || (current != null && !current.isDone() && !current.isCancelled())) {
            return null;
        }
        return interval < 1L ? scheduler.schedule(task, 0L, TimeUnit.MILLISECONDS)
                : scheduler.scheduleAtFixedRate(task, 0L, interval, TimeUnit.MILLISECONDS);
    }

    public final boolean getDefaultDryRun() {
        return defaultDryRun;
    }

    public final Charset getDefaultInputCharset() {
        return defaultInputCharset;
    }

    public final Charset getDefaultOutputCharset() {
        return defaultOutputCharset;
    }

    public final int getDefaultParallelism() {
        return defaultParallelism;
    }

    public final int getDefaultTimeout() {
        return defaultTimeout;
    }

    public final Path getDefaultWorkDirectory() {
        return defaultWorkDir;
    }

    public boolean getDryRun(Properties props) {
        String value = props != null ? props.getProperty(Option.EXEC_DRYRUN.getName()) : null;
        return value != null ? Boolean.parseBoolean(value) : defaultDryRun;
    }

    public Charset getInputCharset(Properties props) {
        String value = props != null ? props.getProperty(Option.INPUT_CHARSET.getName()) : null;
        return value != null ? Charset.forName(value) : defaultInputCharset;
    }

    public Charset getOutputCharset(Properties props) {
        String value = props != null ? props.getProperty(Option.OUTPUT_CHARSET.getName()) : null;
        return value != null ? Charset.forName(value) : defaultOutputCharset;
    }

    public int getParallelism(Properties props) {
        String value = props != null ? props.getProperty(Option.EXEC_PARALLELISM.getName()) : null;
        return value != null ? Integer.parseInt(value) : defaultParallelism;
    }

    public int getTimeout(Properties props) {
        String value = props != null ? props.getProperty(Option.EXEC_TIMEOUT.getName()) : null;
        return value != null ? Integer.parseInt(value) : defaultTimeout;
    }

    public Path getWorkDirectory(Properties props) {
        final Path workDir;
        String value = props != null ? props.getProperty(Option.WORK_DIRECTORY.getName()) : null;
        if (value != null) {
            if (Checker.isNullOrEmpty(value)) {
                workDir = Paths.get(Constants.CURRENT_DIR);
            } else {
                workDir = Utils.getPath(value, true);
            }
        } else {
            workDir = defaultWorkDir;
        }
        return workDir;
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
