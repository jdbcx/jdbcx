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
package io.github.jdbcx.server;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import io.github.jdbcx.Checker;
import io.github.jdbcx.Compression;
import io.github.jdbcx.Constants;
import io.github.jdbcx.Format;
import io.github.jdbcx.Result;

final class QueryInfo implements AutoCloseable, Serializable {
    final String qid;
    final String query;
    final String txid;
    final Format format;
    final Compression compress;
    final String token;
    final String user;
    final String client;

    private transient AtomicReference<Result<?>> result;
    private transient AtomicReference<AutoCloseable[]> resources;

    QueryInfo(String qid, String query, String txid, Format format, Compression compress, String token, String user,
            String client) {
        if (Checker.isNullOrEmpty(qid)) {
            this.qid = UUID.randomUUID().toString();
        } else {
            this.qid = qid;
        }
        this.query = Checker.isNullOrBlank(query) ? Constants.EMPTY_STRING : query;
        this.txid = txid != null ? txid : Constants.EMPTY_STRING;
        this.format = format != null ? format : Format.TSV;
        this.compress = compress != null ? compress : Compression.NONE;
        this.token = token != null ? token : Constants.EMPTY_STRING;
        this.user = user != null ? user : Constants.EMPTY_STRING;
        this.client = client != null ? client : Constants.EMPTY_STRING;

        this.result = new AtomicReference<>();
        this.resources = new AtomicReference<>();
    }

    Result<?> getResult() { // NOSONAR
        return this.result.get();
    }

    AutoCloseable[] getResources() {
        AutoCloseable[] arr = this.resources.get();
        final int len;
        if (arr == null || (len = arr.length) == 0) {
            return arr;
        }

        AutoCloseable[] res = new AutoCloseable[len];
        for (int i = 0; i < len; i++) {
            res[i] = arr[i]; // NOSONAR
        }
        return res;
    }

    @SuppressWarnings("resource")
    QueryInfo setResult(Result<?> result) {
        if (result == null) {
            throw new IllegalArgumentException("Non-null result is required");
        } else if (!this.result.compareAndSet(null, result)) {
            throw new IllegalStateException("Cannot replace non-null result: " + this.result.get());
        }
        return this;
    }

    QueryInfo setResources(AutoCloseable... resources) {
        final int len;
        if (resources == null || (len = resources.length) == 0) {
            throw new IllegalArgumentException("At least one AutoCloseable resource is required");
        } else {
            List<AutoCloseable> list = new ArrayList<>(len);
            for (int i = 0; i < len; i++) {
                AutoCloseable r = resources[i];
                if (r != null) {
                    list.add(r);
                }
            }
            if (list.isEmpty()) {
                throw new IllegalArgumentException("At least one non-null AutoCloseable is required");
            } else if (!this.resources.compareAndSet(null, list.toArray(new AutoCloseable[0]))) {
                throw new IllegalStateException("Cannot replace non-null resources: " + this.resources.get());
            }
        }
        return this;
    }

    @Override
    public void close() {
        final Result<?> r = this.result.getAndUpdate(v -> null);
        if (r != null) {
            try {
                r.close();
            } catch (Throwable t) { // NOSONAR
                // ignore
            }
        }

        final AutoCloseable[] res = this.resources.getAndUpdate(v -> null);
        if (res != null) {
            for (int i = 0, len = res.length; i < len; i++) {
                try {
                    res[i].close();
                } catch (Throwable t) { // NOSONAR
                    // ignore
                }
            }
        }
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int hash = prime + qid.hashCode();
        hash = prime * hash + query.hashCode();
        hash = prime * hash + txid.hashCode();
        hash = prime * hash + format.hashCode();
        hash = prime * hash + compress.hashCode();
        hash = prime * hash + token.hashCode();
        hash = prime * hash + user.hashCode();
        hash = prime * hash + client.hashCode();
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        QueryInfo other = (QueryInfo) obj;
        return qid.equals(other.qid) && query.equals(other.query) && txid.equals(other.txid) && format == other.format
                && compress == other.compress && token.equals(other.token) && user.equals(other.user)
                && client.equals(other.client);
    }

    @Override
    public String toString() {
        return new StringBuilder(getClass().getSimpleName()).append('@').append(hashCode()).append("[id=").append(qid)
                .append(", tx=").append(txid).append(", fmt=").append(format).append(", comp=").append(compress)
                .append(", user=").append(user).append(", client=").append(client).append("]:\n").append(query)
                .toString();
    }
}
