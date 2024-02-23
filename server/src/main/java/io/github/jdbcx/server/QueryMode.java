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
package io.github.jdbcx.server;

public enum QueryMode {
    SUBMIT_QUERY("s"), // submit query without execution
    SUBMIT_REDIRECT("r"), // submit query and redirect
    ASYNC_QUERY("a"),
    DIRECT_QUERY("d"), // execute query
    MUTATION("m"); // mutation

    private String code;

    QueryMode(String code) {
        this.code = code;
    }

    public String code() {
        return code;
    }

    public static QueryMode of(String str) {
        QueryMode mode = null;
        if (str == null || str.isEmpty()) {
            mode = SUBMIT_QUERY;
        } else if (str.length() == 1) {
            switch (str.charAt(0)) {
                case 'a':
                case 'A':
                    mode = ASYNC_QUERY;
                    break;
                case 's':
                case 'S':
                    mode = SUBMIT_QUERY;
                    break;
                case 'r':
                case 'R':
                    mode = SUBMIT_REDIRECT;
                    break;
                case 'd':
                case 'D':
                    mode = DIRECT_QUERY;
                    break;
                case 'm':
                case 'M':
                    mode = MUTATION;
                    break;
                default:
                    break;
            }
        }
        if (mode == null) {
            throw new IllegalArgumentException("Unsupported query mode: " + str);
        }
        return mode;
    }
}
