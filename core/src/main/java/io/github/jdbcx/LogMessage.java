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

import java.util.Arrays;
import java.util.IllegalFormatException;
import java.util.Locale;

/**
 * Log message with arguments and/or error.
 */
public final class LogMessage {
    /**
     * Creates a log message with arguments. The latest argument could be a
     * {@code java.lang.Throwable} providing details like stack trace of an error.
     *
     * @param format    Object format, could be {@code null}
     * @param arguments arguments, could be {@code null} or empty array
     * @return log message
     */
    public static LogMessage of(Object format, Object... arguments) {
        String message = String.valueOf(format);
        Throwable t = null;

        final int len;
        if (arguments != null && (len = arguments.length) > 0) {
            Object lastArg = arguments[len - 1];
            if (lastArg instanceof Throwable) {
                t = (Throwable) lastArg;
            }

            try {
                message = String.format(Locale.ROOT, message, arguments);
            } catch (IllegalFormatException e) {
                message = new StringBuilder(message).append(':').append(Arrays.toString(arguments)).toString();
            }
        }

        return new LogMessage(message, t);
    }

    private final String message;
    private final Throwable throwable;

    /**
     * Default constructor.
     *
     * @param message non-null message
     * @param t       throwable
     */
    private LogMessage(String message, Throwable t) {
        this.message = message;
        this.throwable = t;
    }

    /**
     * Gets log message.
     *
     * @return non-null log message
     */
    public String getMessage() {
        return this.message;
    }

    /**
     * Gets error which may or may not be null.
     *
     * @return error, could be {@code null}
     */
    public Throwable getThrowable() {
        return this.throwable;
    }

    /**
     * Checks if error is available or not.
     *
     * @return true if there's error; false otherwise
     */
    public boolean hasThrowable() {
        return this.throwable != null;
    }
}
