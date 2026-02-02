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

import java.util.Objects;

import io.github.jdbcx.Checker;
import io.github.jdbcx.Compression;
import io.github.jdbcx.Constants;
import io.github.jdbcx.Format;
import io.github.jdbcx.JdbcDialect;
import io.github.jdbcx.QueryMode;
import io.github.jdbcx.Result;

public class Request {
    protected final String method;
    protected final QueryMode mode;
    protected final String rawParams;
    protected final boolean hasQid;
    protected final QueryInfo info;
    protected final JdbcDialect dialect;
    protected final Object implementation;

    protected Request(String method, QueryMode mode, QueryInfo info, JdbcDialect dialect, Object implementation) {
        this.method = method != null ? method : Constants.EMPTY_STRING;
        this.mode = mode != null ? mode : QueryMode.SUBMIT;
        this.rawParams = Constants.EMPTY_STRING;
        this.hasQid = true;
        this.info = Checker.nonNull(info, QueryInfo.class);
        this.dialect = dialect;
        this.implementation = implementation;
    }

    protected Request(String method, QueryMode mode, String rawParams, String qid, String query, String txid,
            Format format, Compression compress, String accessToken, String user, String client, String tenant,
            JdbcDialect dialect, Object implementation) {
        this.method = method != null ? method : Constants.EMPTY_STRING;
        this.mode = mode != null ? mode : QueryMode.SUBMIT;
        this.rawParams = rawParams != null ? rawParams : Constants.EMPTY_STRING;
        this.hasQid = !Checker.isNullOrEmpty(qid);
        this.info = new QueryInfo(qid, query, txid, format, compress, accessToken, tenant, user, client);
        this.dialect = dialect;
        this.implementation = implementation;
    }

    public boolean hasCompression() {
        return info.compress != Compression.NONE;
    }

    public boolean hasQueryId() {
        return hasQid;
    }

    public boolean hasResult() {
        return info.getResult() != null;
    }

    public boolean hasTenantId() {
        return !info.tenant.isEmpty();
    }

    public int getResultState() {
        final Result<?> result = info.getResult();
        if (result != null) {
            return result.isActive() ? -1 : 1;
        }
        return 0;
    }

    public boolean isTransactional() {
        return !info.txid.isEmpty();
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

    public String getRawParameters() {
        return rawParams;
    }

    public String getQueryId() {
        return info.qid;
    }

    public QueryInfo getQueryInfo() {
        return info;
    }

    public String getQuery() {
        return info.query;
    }

    public String getTenant() {
        return info.tenant;
    }

    public String getTransactionId() {
        return info.txid;
    }

    public Format getFormat() {
        return info.format;
    }

    public Compression getCompression() {
        return info.compress;
    }

    public JdbcDialect getDialect() {
        return dialect;
    }

    public <T> T getImplementation(Class<T> clazz) {
        return clazz.cast(implementation);
    }

    public String toUrl(String baseUrl) {
        StringBuilder builder = new StringBuilder();
        if (!Checker.isNullOrEmpty(baseUrl)) {
            builder.append(baseUrl);
        }
        builder.append(info.qid).append(info.format.fileExtension());

        if (info.compress != Compression.NONE) {
            if (info.format.supportsCompressionCodec()) {
                builder.append('?');
                if (rawParams.isEmpty()) {
                    builder.append("codec=").append(info.compress.name());
                } else {
                    builder.append(rawParams);
                }
            } else {
                builder.append(info.compress.fileExtension());
            }
        } else if (!rawParams.isEmpty()) {
            builder.append('?').append(rawParams);
        }

        String url = builder.toString();
        Result<?> result = info.getResult();
        if (result != null) {
            url = dialect.getMapper().toRemoteTable(url, info.format, info.compress, result);
        }
        return url;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = prime + method.hashCode();
        result = prime * result + mode.hashCode();
        result = prime * result + rawParams.hashCode();
        result = prime * result + (hasQid ? 1231 : 1237);
        result = prime * result + info.hashCode();
        result = prime * result + dialect.hashCode();
        result = prime * result + (implementation == null ? 0 : implementation.hashCode());
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
        return method.equals(other.method) && mode == other.mode && rawParams.equals(other.rawParams)
                && info.equals(other.info) && dialect.equals(other.dialect)
                && Objects.equals(implementation, other.implementation);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder(getClass().getSimpleName()).append('@').append(hashCode())
                .append("[").append(method).append(", mode=").append(mode).append(", id=");
        if (!hasQid) {
            builder.append('?');
        }
        return builder.append(info.qid).append(", params=").append(rawParams).append(", tx=").append(info.txid)
                .append(", fmt=").append(info.format).append(", comp=").append(info.compress).append(", tenant=")
                .append(info.tenant).append(", user=").append(info.user).append(", client=").append(info.client)
                .append(", impl=").append(implementation).append("]:\n").append(info.query).toString();
    }
}
