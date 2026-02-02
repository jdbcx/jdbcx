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

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import io.github.jdbcx.Checker;
import io.github.jdbcx.Logger;
import io.github.jdbcx.LoggerFactory;

final class CustomThreadFactory implements ThreadFactory {
    private static final Logger log = LoggerFactory.getLogger(CustomThreadFactory.class);

    private static final UncaughtExceptionHandler hanlder = (t, e) -> log.warn("Uncaught exception from thread [%s]", t,
            e);

    private final boolean daemon;
    private final int priority;

    private final ThreadGroup group; // NOSONAR
    private final String namePrefix;
    private final AtomicInteger threadNumber;

    public CustomThreadFactory(Object owner) {
        this(owner, true, Thread.NORM_PRIORITY);
    }

    public CustomThreadFactory(Object owner, boolean daemon, int priority) {
        String prefix = null;
        if (owner instanceof String) {
            prefix = ((String) owner).trim();
        } else if (owner != null) {
            prefix = new StringBuilder().append(owner.getClass().getSimpleName()).append('@').append(owner.hashCode())
                    .toString();
        }
        this.daemon = daemon;
        this.priority = Checker.between(priority, "Priority", Thread.MIN_PRIORITY, Thread.MAX_PRIORITY);

        SecurityManager s = System.getSecurityManager();
        group = s != null ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
        namePrefix = !Checker.isNullOrBlank(prefix) ? prefix
                : new StringBuilder().append(getClass().getSimpleName()).append('@').append(hashCode())
                        .append('-').toString();
        threadNumber = new AtomicInteger(1);
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread t = new Thread(group, r, namePrefix + threadNumber.getAndIncrement(), 0);
        if (daemon != t.isDaemon()) {
            t.setDaemon(daemon);
        }
        if (priority != t.getPriority()) {
            t.setPriority(priority);
        }
        t.setUncaughtExceptionHandler(hanlder);
        return t;
    }
}
