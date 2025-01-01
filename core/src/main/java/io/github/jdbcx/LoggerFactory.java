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

import java.util.ServiceLoader;

import io.github.jdbcx.logging.JdkLoggerFactory;
import io.github.jdbcx.logging.Slf4jLoggerFactory;

/**
 * Unified factory class to get logger.
 */
public abstract class LoggerFactory {
    private static final LoggerFactory instance;

    static {
        LoggerFactory factory = null;
        for (LoggerFactory f : ServiceLoader.load(LoggerFactory.class, LoggerFactory.class.getClassLoader())) {
            if (f != null) {
                factory = f;
                break;
            }
        }

        if (factory == null) {
            try {
                if (org.slf4j.LoggerFactory.getILoggerFactory() != null) {
                    factory = new Slf4jLoggerFactory(); // NOSONAR
                }
            } catch (Throwable ignore) { // NOSONAR
                factory = new JdkLoggerFactory(); // NOSONAR
            }
        }

        if (factory == null) {
            throw new IllegalArgumentException("No LoggerFactory found");
        }
        instance = factory;
    }

    /**
     * Gets logger for the given class. Same as {@code getInstance().get(clazz)}.
     *
     * @param clazz class
     * @return logger for the given class
     */
    public static Logger getLogger(Class<?> clazz) {
        return instance.get(clazz);
    }

    /**
     * Gets logger for the given name. Same as {@code getInstance().get(name)}.
     *
     * @param name name
     * @return logger for the given name
     */
    public static Logger getLogger(String name) {
        return instance.get(name);
    }

    /**
     * Gets instance of the factory for creating logger.
     *
     * @return factory for creating logger
     */
    public static LoggerFactory getInstance() {
        return instance;
    }

    /**
     * Gets logger for the given class.
     *
     * @param clazz class
     * @return logger for the given class
     */
    public Logger get(Class<?> clazz) {
        if (clazz == null) {
            throw new IllegalArgumentException("Non-null class required");
        }
        return get(clazz.getName());
    }

    /**
     * Gets logger for the given name.
     *
     * @param name name
     * @return logger for the given name
     */
    public abstract Logger get(String name);
}
