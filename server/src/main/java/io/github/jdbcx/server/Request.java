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

import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;

import io.github.jdbcx.Checker;
import io.github.jdbcx.Compression;
import io.github.jdbcx.Constants;
import io.github.jdbcx.Format;

public class Request implements Serializable {
    protected final QueryMode mode;
    protected final String method;
    protected final boolean hasQid;
    protected final String qid;
    protected final String query;
    protected final String txid;
    protected final Format format;
    protected final Compression compress;

    protected final transient Object userObject;

    protected Request(String method, QueryMode mode, String qid, String query, String txid, Format format,
            Compression compress, Object userObject) {
        this.method = method != null ? method : Constants.EMPTY_STRING;
        this.mode = mode != null ? mode : QueryMode.SUBMIT_QUERY;
        if (Checker.isNullOrEmpty(qid)) {
            this.hasQid = false;
            this.qid = UUID.randomUUID().toString();
        } else {
            this.hasQid = true;
            this.qid = qid;
        }
        this.query = Checker.isNullOrBlank(query) ? Constants.EMPTY_STRING : query;
        this.txid = txid != null ? txid : Constants.EMPTY_STRING;
        this.format = format != null ? format : Format.TSV;
        this.compress = compress != null ? compress : Compression.NONE;
        this.userObject = userObject;
    }

    public boolean hasCompression() {
        return compress != Compression.NONE;
    }

    public boolean hasQueryId() {
        return hasQid;
    }

    public boolean isTransactional() {
        return !txid.isEmpty();
    }

    public boolean isMutation() {
        return mode == QueryMode.MUTATION;
    }

    public String getMethod() {
        return method;
    }

    public QueryMode getQueryMode() {
        return mode;
    }

    public String getQueryId() {
        return qid;
    }

    public String getQuery() {
        return query;
    }

    public String getTransactionId() {
        return txid;
    }

    public Format getFormat() {
        return format;
    }

    public Compression getCompression() {
        return compress;
    }

    public <T> T getUserObject(Class<T> clazz) {
        return clazz.cast(userObject);
    }

    public String toUrl() {
        return toUrl(null);
    }

    public String toUrl(String baseUrl) {
        StringBuilder builder = new StringBuilder();
        if (!Checker.isNullOrEmpty(baseUrl)) {
            builder.append(baseUrl);
        }
        builder.append(getQueryId()).append(format.fileExtension());
        if (compress != Compression.NONE) {
            builder.append(compress.fileExtension());
        }
        return builder.toString();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = prime + mode.hashCode();
        result = prime * result + method.hashCode();
        result = prime * result + (hasQid ? 1231 : 1237);
        result = prime * result + qid.hashCode();
        result = prime * result + query.hashCode();
        result = prime * result + txid.hashCode();
        result = prime * result + format.hashCode();
        result = prime * result + compress.hashCode();
        result = prime * result + userObject.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        Request other = (Request) obj;
        return method.equals(other.method) && mode == other.mode && hasQid == other.hasQid
                && qid.equals(other.qid) && query.equals(other.query) && txid.equals(other.txid)
                && format == other.format && compress == other.compress && Objects.equals(userObject, other.userObject);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder(getClass().getSimpleName()).append('@').append(hashCode())
                .append("[").append(method).append(", mode=").append(mode).append(", id=");
        if (!hasQid) {
            builder.append('?');
        }
        return builder.append(qid).append(", tx=").append(txid).append(", fmt=").append(format).append(", comp=")
                .append(compress).append(", impl=").append(userObject).append("]:\n").append(query).toString();
    }
}
