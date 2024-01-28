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
package io.github.jdbcx.interpreter;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.nio.charset.Charset;
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
import io.github.jdbcx.executor.Stream;
import io.github.jdbcx.executor.WebExecutor;

public class WebInterpreter extends AbstractInterpreter {
    static final String HEADER_AUTHORIZATION = "Authorization";
    static final String AUTH_SCHEME_BASIC = "Basic ";
    static final String AUTH_SCHEME_BEARER = "Bearer ";

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
    public static final Option OPTION_REQUEST_TEMPLATE_FILE = Option
            .of(new String[] { "request.template.file", "Request template file" });
    public static final Option OPTION_AUTH_BASIC_USER = Option
            .of(new String[] { "auth.basic.user", "Username used for HTTP basic authentication" });
    public static final Option OPTION_AUTH_BASIC_PASSWORD = Option
            .of(new String[] { "auth.basic.password", "Password used for HTTP basic authentication" });
    public static final Option OPTION_AUTH_BEARER_TOKEN = Option
            .of(new String[] { "auth.bearer.token", "Access token used for HTTP bearer authentication" });

    public static final List<Option> OPTIONS = Collections
            .unmodifiableList(
                    Arrays.asList(Option.EXEC_ERROR, OPTION_BASE_URL, OPTION_URL_TEMPLATE, OPTION_REQUEST_HEADERS,
                            OPTION_REQUEST_TEMPLATE, OPTION_REQUEST_TEMPLATE_FILE, OPTION_REQUEST_ESCAPE_CHAR,
                            OPTION_REQUEST_ENCODE, OPTION_REQUEST_ESCAPE_TARGET, WebExecutor.OPTION_CONNECT_TIMEOUT,
                            WebExecutor.OPTION_PROXY, WebExecutor.OPTION_SOCKET_TIMEOUT));

    private static final List<Field> dryRunFields = Collections.unmodifiableList(Arrays.asList(
            Field.of("url"), Field.of("method"), Field.of("request"), Field.of("connect_timeout_ms", JDBCType.BIGINT),
            Field.of("socket_timeout_ms", JDBCType.BIGINT), Field.of(KEY_HEADERS), Executor.FIELD_OPTIONS));

    private final Map<String, String> defaultHeaders;
    private final String defaultTemplate;
    private final String defaultUrl;

    private final WebExecutor executor;

    static final Map<String, String> getHeaders(Properties config) {
        Map<String, String> headers = new HashMap<>(Utils
                .toKeyValuePairs(Utils.applyVariables(OPTION_REQUEST_HEADERS.getValue(config), config)));
        String secret = Utils.applyVariables(OPTION_AUTH_BEARER_TOKEN.getValue(config), config);
        if (!Checker.isNullOrBlank(secret)) { // recommended
            headers.put(HEADER_AUTHORIZATION, AUTH_SCHEME_BEARER.concat(secret));
        } else { // less secure
            secret = OPTION_AUTH_BASIC_USER.getValue(config);
            if (!Checker.isNullOrBlank(secret)) {
                headers.put(HEADER_AUTHORIZATION, AUTH_SCHEME_BASIC.concat(Base64.getEncoder()
                        .encodeToString(new StringBuilder(secret).append(':')
                                .append(OPTION_AUTH_BASIC_PASSWORD.getValue(config)).toString()
                                .getBytes(Constants.DEFAULT_CHARSET))));
            }
        }
        return headers;
    }

    static final String getRequestTemplate(Properties config, String defaultTemplate) {
        String template = OPTION_REQUEST_TEMPLATE.getValue(config, defaultTemplate);
        if (Checker.isNullOrEmpty(template)) {
            String file = Utils
                    .normalizePath(Utils.applyVariables(OPTION_REQUEST_TEMPLATE_FILE.getValue(config), config)).trim();
            if (!Checker.isNullOrEmpty(file)) {
                try {
                    template = Stream.readAllAsString(new FileInputStream(file),
                            Charset.forName(Option.INPUT_CHARSET.getValue(config)));
                    if (Checker.isNullOrEmpty(template)) {
                        template = defaultTemplate;
                    }
                } catch (IOException e) {
                    throw new IllegalArgumentException(e);
                }
            }
        }
        return template;
    }

    public WebInterpreter(QueryContext context, Properties config) {
        super(context);

        this.defaultHeaders = Collections.unmodifiableMap(getHeaders(config));
        this.defaultTemplate = getRequestTemplate(config, Constants.EMPTY_STRING);
        this.defaultUrl = Utils.applyVariables(OPTION_URL_TEMPLATE.getValue(config), config);
        this.executor = new WebExecutor(config);
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
            final URL url = new URL(Utils.applyVariables(OPTION_URL_TEMPLATE.getValue(props, defaultUrl), props));
            Map<?, ?> headers = (Map<?, ?>) getContext().get(KEY_HEADERS);
            if (headers == null || headers.isEmpty()) {
                headers = getHeaders(props);
                if (headers.isEmpty()) {
                    headers = getDefaultRequestHeaders();
                }
            }

            final String request;
            String template = getRequestTemplate(props, defaultTemplate);
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
                request = Utils.applyVariables(template, newProps);
            }

            if (executor.getDryRun(props)) {
                return Result.of(dryRunFields,
                        new Object[][] { { url, Checker.isNullOrBlank(request) ? "GET" : "POST", request,
                                executor.getConnectTimeout(props), executor.getSocketTimeout(props), headers,
                                props } });
            } else if (Checker.isNullOrBlank(request)) {
                input = executor.get(url, props, headers);
            } else {
                input = executor.post(url, request, props, headers);
            }
            return process(request, input, props);
        } catch (SocketTimeoutException e) {
            return handleError(new TimeoutException(e.getMessage()), query, props, input);
        } catch (IOException e) {
            return handleError(e, query, props, input);
        }
    }
}
