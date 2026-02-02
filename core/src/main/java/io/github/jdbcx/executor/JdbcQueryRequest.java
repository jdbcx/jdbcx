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

final class JdbcQueryRequest {
    static class Builder {
        private String query;
        private String resultType;
        private int queryTimeoutSec;
        private int parallelism;
        private long[] stats;

        Builder(JdbcQueryRequest request) {
            this.query = request.query;
            this.resultType = request.resultType;
            this.queryTimeoutSec = request.queryTimeoutSec;
            this.parallelism = request.parallelism;
            this.stats = new long[] { 0L, 0L, 0L };
        }

        Builder query(String query) {
            this.query = query;
            return this;
        }

        Builder resultType(String resultType) {
            this.resultType = resultType;
            return this;
        }

        Builder queryTimeoutSec(int queryTimeoutSec) {
            this.queryTimeoutSec = queryTimeoutSec;
            return this;
        }

        Builder parallelism(int parallelism) {
            this.parallelism = parallelism;
            return this;
        }

        Builder stats(long[] stats) {
            this.stats = stats;
            return this;
        }

        JdbcQueryRequest build() {
            return new JdbcQueryRequest(query, resultType, queryTimeoutSec, parallelism, stats);
        }
    }

    final String query;
    final String resultType;
    final int queryTimeoutSec;
    final int parallelism;
    final long[] stats;

    private JdbcQueryRequest(String query, String resultType, int queryTimeoutSec, int parallelism, long[] stats) {
        this.query = query;
        this.resultType = resultType;
        this.queryTimeoutSec = queryTimeoutSec;
        this.parallelism = parallelism;
        this.stats = stats;
    }

    JdbcQueryRequest(String query, String resultType, int queryTimeoutSec, int parallelism) {
        this.query = query;
        this.resultType = resultType;
        this.queryTimeoutSec = queryTimeoutSec;
        this.parallelism = parallelism;
        this.stats = new long[] {
                0L, // reads
                0L, // updates
                0L // affected rows
        };
    }

    Builder update() {
        return new Builder(this);
    }
}
