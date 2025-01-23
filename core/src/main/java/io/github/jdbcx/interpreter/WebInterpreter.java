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
package io.github.jdbcx.interpreter;

import java.io.IOException;
import java.io.InputStream;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.sql.JDBCType;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

import io.github.jdbcx.Checker;
import io.github.jdbcx.Constants;
import io.github.jdbcx.Executor;
import io.github.jdbcx.Field;
import io.github.jdbcx.Option;
import io.github.jdbcx.QueryContext;
import io.github.jdbcx.Result;
import io.github.jdbcx.Utils;
import io.github.jdbcx.VariableTag;
import io.github.jdbcx.executor.WebExecutor;

public class WebInterpreter extends AbstractInterpreter {
    public static final String KEY_HEADERS = "headers";

    public static final Option OPTION_BASE_URL = Option
            .of(new String[] { "base.url", "Base URL without trailing slash" });
    public static final Option OPTION_URL_TEMPLATE = Option
            .of(new String[] { "url.template", "URL template", "${base.url}" });
    public static final Option OPTION_REQUEST_HEADERS = Option
            .of(new String[] { "request.headers", "Comma separated key value pairs" });
    public static final Option OPTION_REQUEST_TEMPLATE = Option
            .of(new String[] { "request.template", "Request template" });
    public static final Option OPTION_REQUEST_ENCODE = Option
            .of(new String[] { "request.encode", "Request body encoding", Constants.EMPTY_STRING,
                    ScriptHelper.ENCODER_BASE64, ScriptHelper.ENCODER_JSON, ScriptHelper.ENCODER_URL,
                    ScriptHelper.ENCODER_XML });
    public static final Option OPTION_REQUEST_ESCAPE_CHAR = Option
            .of(new String[] { "request.escape.char", "The character that will be used for escaping." });
    public static final Option OPTION_REQUEST_ESCAPE_TARGET = Option
            .of(new String[] { "request.escape.target",
                    "The target character that will be escaped in request." });
    public static final Option OPTION_AUTH_BASIC_USER = Option
            .of(new String[] { "auth.basic.user", "Username used for HTTP basic authentication" });
    public static final Option OPTION_AUTH_BASIC_PASSWORD = Option
            .of(new String[] { "auth.basic.password", "Password used for HTTP basic authentication" });
    public static final Option OPTION_AUTH_BEARER_TOKEN = Option
            .of(new String[] { "auth.bearer.token", "Access token used for HTTP bearer authentication" });

    public static final List<Option> OPTIONS = Collections
            .unmodifiableList(
                    Arrays.asList(Option.EXEC_ERROR, OPTION_BASE_URL, OPTION_URL_TEMPLATE, OPTION_REQUEST_HEADERS,
                            OPTION_REQUEST_TEMPLATE, OPTION_REQUEST_ESCAPE_CHAR, OPTION_REQUEST_ENCODE,
                            OPTION_REQUEST_ESCAPE_TARGET, WebExecutor.OPTION_CONNECT_TIMEOUT, Option.INPUT_FILE,
                            WebExecutor.OPTION_FOLLOW_REDIRECT, Option.PROXY, WebExecutor.OPTION_SOCKET_TIMEOUT));

    private static final List<Field> dryRunFields = Collections.unmodifiableList(Arrays.asList(
            Field.of("url"), Field.of("method"), Field.of("request"), Field.of("connect_timeout_ms", JDBCType.BIGINT),
            Field.of("socket_timeout_ms", JDBCType.BIGINT), Field.of(KEY_HEADERS), Executor.FIELD_OPTIONS));

    static final Map<String, String> getHeaders(VariableTag tag, Properties config) {
        Map<String, String> headers = new HashMap<>(Utils
                .toKeyValuePairs(Utils.applyVariables(OPTION_REQUEST_HEADERS.getValue(config), tag, config)));
        String token = Utils.applyVariables(OPTION_AUTH_BEARER_TOKEN.getValue(config), tag, config);
        if (!Checker.isNullOrBlank(token)) { // recommended
            headers.put(WebExecutor.HEADER_AUTHORIZATION, WebExecutor.AUTH_SCHEME_BEARER.concat(token));
        } else { // less secure
            token = OPTION_AUTH_BASIC_USER.getValue(config);
            if (!Checker.isNullOrBlank(token)) {
                headers.put(WebExecutor.HEADER_AUTHORIZATION, WebExecutor.AUTH_SCHEME_BASIC.concat(Base64.getEncoder()
                        .encodeToString(new StringBuilder(token).append(':')
                                .append(OPTION_AUTH_BASIC_PASSWORD.getValue(config)).toString()
                                .getBytes(Constants.DEFAULT_CHARSET))));
            }
        }
        return headers;
    }

    private final Map<String, String> defaultHeaders;
    private final String defaultTemplate;
    private final String defaultUrl;

    private final WebExecutor executor;

    public WebInterpreter(QueryContext context, Properties config) {
        super(context);

        final VariableTag tag = getVariableTag();
        this.defaultHeaders = Collections.unmodifiableMap(getHeaders(tag, config));
        this.defaultTemplate = OPTION_REQUEST_TEMPLATE.getValue(config, Constants.EMPTY_STRING);
        this.defaultUrl = Utils.applyVariables(OPTION_URL_TEMPLATE.getValue(config), tag, config);

        this.executor = new WebExecutor(tag, config);
    }

    public final Map<String, String> getDefaultRequestHeaders() {
        return defaultHeaders;
    }

    public final String getDefaultRequestTemplate() {
        return defaultTemplate;
    }

    public final String getDefaultUrl() {
        return defaultUrl;
    }

    @Override
    public Result<?> interpret(String query, Properties props) {
        InputStream input = null;
        try {
            final VariableTag tag = getVariableTag();
            final URL url = Utils
                    .toURL(Utils.applyVariables(OPTION_URL_TEMPLATE.getValue(props, defaultUrl), tag, props));
            Map<?, ?> headers = (Map<?, ?>) getContext().get(KEY_HEADERS);
            if (headers == null || headers.isEmpty()) {
                headers = getHeaders(tag, props);
                if (headers.isEmpty()) {
                    headers = getDefaultRequestHeaders();
                }
            }

            final String request;
            final String template = OPTION_REQUEST_TEMPLATE.getValue(props, defaultTemplate);
            if (Checker.isNullOrEmpty(template)) {
                request = query;
            } else {
                Properties newProps = new Properties(props);
                String encoder = OPTION_REQUEST_ENCODE.getValue(props);
                String escapeTarget = OPTION_REQUEST_ESCAPE_TARGET.getValue(props);
                String escapeChar = OPTION_REQUEST_ESCAPE_CHAR.getValue(props);
                if (!Checker.isNullOrEmpty(encoder)) {
                    newProps.setProperty(Constants.EMPTY_STRING, ScriptHelper.getInstance().encode(query, encoder));
                } else {
                    newProps.setProperty(Constants.EMPTY_STRING,
                            Checker.isNullOrEmpty(escapeTarget) || Checker.isNullOrEmpty(escapeChar) ? query
                                    : Utils.escape(query, escapeTarget.charAt(0), escapeChar.charAt(0)));
                }
                request = Utils.applyVariables(template, tag, newProps);
            }

            if (executor.getDryRun(props)) {
                return Result.of(dryRunFields,
                        new Object[][] { { url,
                                Checker.isNullOrEmpty(executor.getInputFile(props)) && Checker.isNullOrBlank(request)
                                        ? "GET"
                                        : "POST",
                                request, executor.getConnectTimeout(props), executor.getSocketTimeout(props), headers,
                                props } });
            } else {
                input = executor.execute(url, request, props, headers);
            }
            return process(request, input, props);
        } catch (SocketTimeoutException e) {
            return handleError(new TimeoutException(e.getMessage()), query, props, input);
        } catch (IOException e) {
            return handleError(e, query, props, input);
        }
    }
}
