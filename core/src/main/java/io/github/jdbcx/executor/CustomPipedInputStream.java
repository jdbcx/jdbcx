/*
 * Copyright 2022-2026, Zhichun Wu
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
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import io.github.jdbcx.Logger;
import io.github.jdbcx.LoggerFactory;
import io.github.jdbcx.Utils;

final class CustomPipedInputStream extends PipedInputStream {
    private static final Logger log = LoggerFactory.getLogger(CustomPipedInputStream.class);

    private final long timeout;
    private final AtomicReference<CompletableFuture<?>> ref;

    CustomPipedInputStream(PipedOutputStream src, int pipeSize, long timeout) throws IOException {
        super(src, pipeSize);

        this.timeout = timeout;
        this.ref = new AtomicReference<>();
    }

    CustomPipedInputStream attach(CompletableFuture<?> future) {
        if (!ref.compareAndSet(null, future)) {
            throw new IllegalStateException("Piped input stream is occupied");
        }
        return this;
    }

    @Override
    public void close() throws IOException {
        // must close the input stream first to avoid dead-lock
        try {
            super.close();
        } catch (Throwable t) { // NOSONAR
            final CompletableFuture<?> future = ref.getAndSet(null);
            if (future != null) {
                log.debug("Failed to close [%s] due to [%s], going to check status of task [%s]",
                        this, t.getMessage(), future);
                if (future.isDone()) {
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
            throw t;
        }

        final CompletableFuture<?> future = ref.getAndSet(null);
        if (future != null) {
            try {
                if (timeout > 0L) {
                    future.get(timeout, TimeUnit.MILLISECONDS);
                } else {
                    future.get();
                }
            } catch (InterruptedException e) { // NOSONAR
                Thread.interrupted();
                throw new InterruptedIOException(e.getMessage());
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof IOException) {
                    throw (IOException) cause;
                } else {
                    throw new IOException(cause);
                }
            } catch (TimeoutException e) {
                if (e.getMessage() == null) {
                    e = new TimeoutException(Utils.format("Timed out after waiting for %d ms", timeout));
                }
                throw new IOException(e);
            }
        }
    }
}
