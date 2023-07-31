/*
 * Copyright 2022-2023, Zhichun Wu
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
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import io.github.jdbcx.Checker;
import io.github.jdbcx.Constants;
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
            .of(new String[] { "request.headers", "Comma separated key value pairs", "" });
    public static final Option OPTION_REQUEST_TEMPLATE = Option
            .of(new String[] { "request.template", "Request template", "" });
    public static final Option OPTION_REQUEST_TEMPLATE_FILE = Option
            .of(new String[] { "request.template.file", "Request template file", "" });
    public static final Option OPTION_AUTH_BASIC_USER = Option
            .of(new String[] { "auth.basic.user", "Username used for HTTP basic authentication" });
    public static final Option OPTION_AUTH_BASIC_PASSWORD = Option
            .of(new String[] { "auth.basic.password", "Password used for HTTP basic authentication" });
    public static final Option OPTION_AUTH_BEARER_TOKEN = Option
            .of(new String[] { "auth.bearer.token", "Access token used for HTTP bearer authentication" });

    public static final List<Option> OPTIONS = Collections
            .unmodifiableList(
                    Arrays.asList(Option.EXEC_ERROR, OPTION_BASE_URL, OPTION_URL_TEMPLATE, OPTION_REQUEST_HEADERS,
                            OPTION_REQUEST_TEMPLATE, OPTION_REQUEST_TEMPLATE_FILE, WebExecutor.OPTION_CONNECT_TIMEOUT,
                            WebExecutor.OPTION_PROXY, WebExecutor.OPTION_SOCKET_TIMEOUT));

    private final Map<String, String> defaultHeaders;
    private final String defaultTemplate;
    private final String defaultUrl;

    private final WebExecutor executor;

    public WebInterpreter(QueryContext context, Properties config) {
        super(context);

        Map<String, String> headers = new HashMap<>(Utils
                .toKeyValuePairs(Utils.applyVariables(OPTION_REQUEST_HEADERS.getValue(config), config)));
        String secret = OPTION_AUTH_BEARER_TOKEN.getValue(config);
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
        this.defaultHeaders = Collections.unmodifiableMap(headers);
        String template = OPTION_REQUEST_TEMPLATE.getValue(config);
        if (Checker.isNullOrEmpty(template)) {
            String file = Utils.normalizePath(OPTION_REQUEST_TEMPLATE_FILE.getValue(config)).trim();
            if (!Checker.isNullOrEmpty(file)) {
                try {
                    template = Stream.readAllAsString(new FileInputStream(file),
                            Charset.forName(Option.INPUT_CHARSET.getValue(config)));
                } catch (IOException e) {
                    throw new IllegalArgumentException(e);
                }
            }
        }
        this.defaultTemplate = template;
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
        try {
            final URL url = new URL(Utils.applyVariables(OPTION_URL_TEMPLATE.getValue(props, defaultUrl), props));
            Map<?, ?> headers = (Map<?, ?>) getContext().get(KEY_HEADERS);
            if (headers == null || headers.isEmpty()) {
                headers = getDefaultRequestHeaders();
            }
            String request = query;
            if (Checker.isNullOrBlank(request)) {
                if (Checker.isNullOrEmpty(defaultTemplate)) {
                    return Result.of(executor.get(url, props, headers));
                } else {
                    request = Utils.applyVariables(defaultTemplate, props);
                }
            }
            return Result.of(executor.post(url, request, props, headers));
        } catch (IOException e) {
            return handleError(e, query, props);
        }
    }
}
