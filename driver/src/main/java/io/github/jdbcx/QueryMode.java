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
package io.github.jdbcx;

import java.util.Locale;

public enum QueryMode {
    /**
     * Submits a query for later execution.
     */
    SUBMIT("submit", 's', 'S'),
    /**
     * Submits a query and gets redirected to retrieve the results.
     */
    REDIRECT("redirect", 'r', 'R'),
    /**
     * Executes a query asynchronously.
     */
    ASYNC("async", 'a', 'A'),
    /**
     * Executes a query immediately and waits the result.
     */
    DIRECT("query", 'q', 'Q', 'd', 'D'),
    /**
     * Executes a mutation immediately and waits for the result.
     */
    MUTATION("mutate", 'm', 'M'),
    /**
     * Executes batch queries.
     */
    BATCH("batch", 'b', 'B');

    private final char[] codes;
    private final String path;

    QueryMode(String path, char... codes) {
        this.path = path;
        this.codes = codes;
    }

    /**
     * Gets a single character code representing the query mode.
     *
     * @return single character code
     */
    public char code() {
        return codes[0];
    }

    /**
     * Gets semantic relative path used in constructing the query URL.
     *
     * @return non-empty semantic relative path
     */
    public String path() {
        return path;
    }

    /**
     * Gets query mode based on given path.
     *
     * @param path path
     * @return non-null query mode, defaults to {@link #SUBMIT}
     */
    public static QueryMode fromPath(String path) {
        return fromPath(path, SUBMIT);
    }

    /**
     * Gets query mode based on given path.
     *
     * @param path        path
     * @param defaultMode default mode
     * @return query mode
     */
    public static QueryMode fromPath(String path, QueryMode defaultMode) {
        final QueryMode mode;
        if (ASYNC.path.equals(path)) {
            mode = ASYNC;
        } else if (DIRECT.path.equals(path)) {
            mode = DIRECT;
        } else if (REDIRECT.path.equals(path)) {
            mode = REDIRECT;
        } else if (MUTATION.path.equals(path)) {
            mode = MUTATION;
        } else if (SUBMIT.path.equals(path)) {
            mode = SUBMIT;
        } else if (BATCH.path.equals(path)) {
            mode = BATCH;
        } else {
            mode = defaultMode;
        }
        return mode;
    }

    /**
     * Gets query mode based on the given code.
     *
     * @param code code
     * @return non-null query mode, defaults to {@link #SUBMIT}
     */
    public static QueryMode fromCode(char code) {
        return fromCode(code, SUBMIT);
    }

    /**
     * Gets query mode based on the given code.
     *
     * @param code        code
     * @param defaultMode default mode
     * @return query mode
     */
    public static QueryMode fromCode(char code, QueryMode defaultMode) {
        for (QueryMode m : values()) {
            for (char c : m.codes) {
                if (c == code) {
                    return m;
                }
            }
        }
        return defaultMode;
    }

    /**
     * Gets query mode based on given string, which could be a single character
     * {@code code}, {@code path} or {@code name}.
     *
     * @param str single character, path or name
     * @return non-null query mode, defaults to {@link #SUBMIT}
     */
    public static QueryMode of(String str) {
        QueryMode mode = null;
        if (Checker.isNullOrEmpty(str)) {
            mode = SUBMIT;
        } else if (str.length() == 1) {
            mode = fromCode(str.charAt(0), null);
            if (mode == null) {
                throw new IllegalArgumentException("Invalid single character code: " + str);
            }
        } else {
            mode = fromPath(str, null);
        }
        return mode != null ? mode : valueOf(str.toUpperCase(Locale.ROOT));
    }
}
