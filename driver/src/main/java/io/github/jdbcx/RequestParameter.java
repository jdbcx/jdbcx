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
package io.github.jdbcx;

import java.util.Locale;
import java.util.Map;

import io.github.jdbcx.executor.WebExecutor;

public enum RequestParameter {
    /**
     * Agent or client of the request.
     */
    AGENT(WebExecutor.HEADER_USER_AGENT.toLowerCase(Locale.ROOT), "a", Constants.EMPTY_STRING),
    /**
     * Preferred encoding or compression of response.
     */
    COMPRESSION(WebExecutor.HEADER_ACCEPT_ENCODING.toLowerCase(Locale.ROOT), "c", Compression.NONE.encoding()),
    /**
     * Preferred MIME type of response.
     */
    FORMAT(WebExecutor.HEADER_ACCEPT.toLowerCase(Locale.ROOT), "f", Format.CSV.mimeType()),
    /**
     * Query.
     */
    QUERY(Constants.EMPTY_STRING, "q", Constants.EMPTY_STRING),
    /**
     * Unique ID of the query.
     */
    QUERY_ID("x-query-id", "qid", Constants.EMPTY_STRING),
    /**
     * {@link QueryMode} of the request.
     */
    MODE("x-query-mode", "m", QueryMode.SUBMIT.path()),
    /**
     * Transaction ID.
     */
    TRANSACTION_ID("x-transaction-id", "txid", Constants.EMPTY_STRING),
    /**
     * User who created the request.
     */
    USER("x-query-user", "u", Constants.EMPTY_STRING);

    private final String header;
    private final String parameter;
    private final String defaultValue;

    RequestParameter(String header, String parameter, String defaultValue) {
        this.header = header;
        this.parameter = parameter;
        this.defaultValue = defaultValue;
    }

    /**
     * Gets HTTP request header name in lower case.
     *
     * @return non-empty header name
     */
    public String header() {
        return header;
    }

    /**
     * Gets query parameter name can be used in request URL.
     *
     * @return non-empty parameter name
     */
    public String parameter() {
        return parameter;
    }

    /**
     * Gets default value of the parameter.
     *
     * @return non-null default value
     */
    public String defaultValue() {
        return defaultValue;
    }

    /**
     * Gets value of the query parameter by checking {@code headers},
     * {@code params}, and {@code defaultValues} in order. {@link #defaultValue()}
     * will be used if no value found in inputs.
     *
     * @param headers       request headers
     * @param params        query parameters in request URL
     * @param defaultValues default values
     * @return non-null value of the query parameter
     */
    public String getValue(Map<String, String> headers, Map<String, String> params, String... defaultValues) {
        String value = null;
        if (!header.isEmpty() && headers != null && !headers.isEmpty()) {
            value = headers.get(header);
        }

        if (!parameter.isEmpty() && Checker.isNullOrEmpty(value) && params != null && !params.isEmpty()) {
            value = params.get(parameter);
        }

        if (Checker.isNullOrEmpty(value) && defaultValues != null) {
            for (String v : defaultValues) {
                if (v != null) {
                    value = v;
                    break;
                }
            }
        }
        return value != null ? value : defaultValue;
    }
}
