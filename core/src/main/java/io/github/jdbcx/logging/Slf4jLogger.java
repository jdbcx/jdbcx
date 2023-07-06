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
package io.github.jdbcx.logging;

import java.util.function.Supplier;

import io.github.jdbcx.LogMessage;
import io.github.jdbcx.Logger;

/**
 * Adaptor for slf4j logger.
 */
public class Slf4jLogger implements Logger {
    private final org.slf4j.Logger logger;

    /**
     * Default constructor.
     *
     * @param logger non-null SLF4J logger
     */
    public Slf4jLogger(org.slf4j.Logger logger) {
        if (logger == null) {
            throw new IllegalArgumentException(ERROR_NULL_LOGGER);
        }
        this.logger = logger;
    }

    @Override
    public boolean isDebugEnabled() {
        return logger.isDebugEnabled();
    }

    @Override
    public boolean isErrorEnabled() {
        return logger.isErrorEnabled();
    }

    @Override
    public boolean isInfoEnabled() {
        return logger.isInfoEnabled();
    }

    @Override
    public boolean isWarnEnabled() {
        return logger.isWarnEnabled();
    }

    @Override
    public boolean isTraceEnabled() {
        return logger.isTraceEnabled();
    }

    @Override
    public void debug(Supplier<?> function) {
        if (function != null && logger.isDebugEnabled()) {
            logger.debug(String.valueOf(function.get()));
        }
    }

    @Override
    public void debug(Object format, Object... arguments) {
        if (logger.isDebugEnabled()) {
            LogMessage msg = LogMessage.of(format, arguments);
            if (msg.hasThrowable()) {
                logger.debug(msg.getMessage(), msg.getThrowable());
            } else {
                logger.debug(msg.getMessage());
            }
        }
    }

    @Override
    public void debug(Object message, Throwable t) {
        if (logger.isDebugEnabled()) {
            logger.debug(String.valueOf(message), t);
        }
    }

    @Override
    public void error(Supplier<?> function) {
        if (function != null && logger.isErrorEnabled()) {
            logger.error(String.valueOf(function.get()));
        }
    }

    @Override
    public void error(Object format, Object... arguments) {
        if (logger.isErrorEnabled()) {
            LogMessage msg = LogMessage.of(format, arguments);
            if (msg.hasThrowable()) {
                logger.error(msg.getMessage(), msg.getThrowable());
            } else {
                logger.error(msg.getMessage());
            }
        }
    }

    @Override
    public void error(Object message, Throwable t) {
        if (logger.isErrorEnabled()) {
            logger.error(String.valueOf(message), t);
        }
    }

    @Override
    public void info(Supplier<?> function) {
        if (function != null && logger.isInfoEnabled()) {
            logger.info(String.valueOf(function.get()));
        }
    }

    @Override
    public void info(Object format, Object... arguments) {
        if (logger.isInfoEnabled()) {
            LogMessage msg = LogMessage.of(format, arguments);
            if (msg.hasThrowable()) {
                logger.info(msg.getMessage(), msg.getThrowable());
            } else {
                logger.info(msg.getMessage());
            }
        }
    }

    @Override
    public void info(Object message, Throwable t) {
        if (logger.isInfoEnabled()) {
            logger.info(String.valueOf(message), t);
        }
    }

    @Override
    public void trace(Supplier<?> function) {
        if (function != null && logger.isTraceEnabled()) {
            logger.trace(String.valueOf(function.get()));
        }
    }

    @Override
    public void trace(Object format, Object... arguments) {
        if (logger.isTraceEnabled()) {
            LogMessage msg = LogMessage.of(format, arguments);
            if (msg.hasThrowable()) {
                logger.trace(msg.getMessage(), msg.getThrowable());
            } else {
                logger.trace(msg.getMessage());
            }
        }
    }

    @Override
    public void trace(Object message, Throwable t) {
        if (logger.isTraceEnabled()) {
            logger.trace(String.valueOf(message), t);
        }
    }

    @Override
    public void warn(Supplier<?> function) {
        if (function != null && logger.isWarnEnabled()) {
            logger.warn(String.valueOf(function.get()));
        }
    }

    @Override
    public void warn(Object format, Object... arguments) {
        if (logger.isWarnEnabled()) {
            LogMessage msg = LogMessage.of(format, arguments);
            if (msg.hasThrowable()) {
                logger.warn(msg.getMessage(), msg.getThrowable());
            } else {
                logger.warn(msg.getMessage());
            }
        }
    }

    @Override
    public void warn(Object message, Throwable t) {
        if (logger.isWarnEnabled()) {
            logger.warn(String.valueOf(message), t);
        }
    }

    @Override
    public Object unwrap() {
        return logger;
    }
}
