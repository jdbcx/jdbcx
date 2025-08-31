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
package io.github.jdbcx.extension;

import java.io.InputStream;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.UUID;

import io.github.jdbcx.Checker;
import io.github.jdbcx.Compression;
import io.github.jdbcx.ConfigManager;
import io.github.jdbcx.Constants;
import io.github.jdbcx.DriverExtension;
import io.github.jdbcx.Format;
import io.github.jdbcx.JdbcDialect;
import io.github.jdbcx.Logger;
import io.github.jdbcx.LoggerFactory;
import io.github.jdbcx.Option;
import io.github.jdbcx.QueryContext;
import io.github.jdbcx.QueryMode;
import io.github.jdbcx.RequestParameter;
import io.github.jdbcx.Result;
import io.github.jdbcx.Utils;
import io.github.jdbcx.VariableTag;
import io.github.jdbcx.executor.Stream;
import io.github.jdbcx.executor.WebExecutor;
import io.github.jdbcx.interpreter.WebInterpreter;

public class BridgeDriverExtension implements DriverExtension {
    private static final Logger log = LoggerFactory.getLogger(BridgeDriverExtension.class);

    static final String DEFAULT_CONNECT_TIMEOUT = "3000";
    static final String DEFAULT_SOCKET_TIMEOUT = "5000";

    static final String KEY_QUERY_ID = "query_id";

    static final Option OPTION_COMPRESSION = Option.of(new String[] { "compression", "Compression algorithm" });
    static final Option OPTION_FORMAT = Option.of(new String[] { "format", "Data format" });
    static final Option OPTION_QUERY_MODE = Option.of(new String[] { "mode", "Query mode" });
    static final Option OPTION_PATH = Option.of(new String[] { DriverExtension.PROPERTY_PATH, "URL path" });
    static final Option OPTION_URL = Option
            .of(new String[] { DriverExtension.PROPERTY_BRIDGE_URL, "Bridge server URL" });
    static final Option OPTION_TOKEN = Option
            .of(new String[] { DriverExtension.PROPERTY_BRIDGE_TOKEN, "Secure token for accessing bridge server" });

    static final class UnexpectedServerException extends RuntimeException {
        UnexpectedServerException(String error) {
            super(error);
        }
    }

    static final class ActivityListener extends AbstractActivityListener {
        static StringBuilder getUrlBuilder(String url) {
            if (Checker.isNullOrEmpty(url)) {
                return new StringBuilder();
            }

            StringBuilder builder;
            int index = url.indexOf('?');
            if (index != -1) {
                builder = new StringBuilder(url.substring(0, index));
            } else {
                builder = new StringBuilder(url);
            }
            if (builder.charAt(builder.length() - 1) != '/') {
                builder.append('/');
            }
            return builder;
        }

        static WebExecutor getTempWebExecutor(VariableTag tag, Properties config) {
            WebExecutor web = new WebExecutor(tag, config);
            WebExecutor.OPTION_CONNECT_TIMEOUT.setValue(config, DEFAULT_CONNECT_TIMEOUT);
            WebExecutor.OPTION_SOCKET_TIMEOUT.setValue(config, DEFAULT_SOCKET_TIMEOUT);
            WebExecutor.OPTION_FOLLOW_REDIRECT.setValue(config, Constants.FALSE_EXPR);
            return web;
        }

        ActivityListener(QueryContext context, Properties config) {
            super(new WebInterpreter(context, config), config);
        }

        String rewrite(String query) {
            final int len;
            if (query == null || (len = query.length()) == 0) {
                return query;
            }
            final char escapeChar = '\\';
            final char target = interpreter.getVariableTag().rightChar();
            StringBuilder builder = new StringBuilder(len);
            boolean escaped = false;
            for (int i = 0; i < len; i++) {
                char ch = query.charAt(i);
                if (escaped) {
                    if (ch != escapeChar && ch != target) {
                        builder.append(escapeChar);
                    }
                    builder.append(ch);
                    escaped = false;
                } else if (ch == escapeChar) {
                    escaped = true;
                } else {
                    builder.append(ch);
                }
            }
            if (escaped) {
                builder.append(escapeChar);
            }
            return builder.toString();
        }

        void checkError() {
            final QueryContext context = interpreter.getContext();
            final Object queryId = context.get(KEY_QUERY_ID);
            if (queryId != null) { // only when query id is available
                final StringBuilder builder = getUrlBuilder(
                        OPTION_URL.getValue((Properties) context.get(QueryContext.KEY_BRIDGE)));
                try {
                    Properties config = new Properties();
                    WebExecutor web = getTempWebExecutor(interpreter.getVariableTag(), config);
                    try (InputStream input = web.get(Utils.toURL(builder.append("error/").append(queryId).toString()),
                            config,
                            Collections.singletonMap(RequestParameter.FORMAT.header(), Format.TXT.mimeType()))) {
                        throw new UnexpectedServerException(Stream.readAllAsString(input));
                    }
                } catch (UnexpectedServerException exp) {
                    throw exp;
                } catch (Exception exp) {
                    log.error("Failed to retrieve error message from bridge server: " + builder.toString(), exp);
                }
            }
        }

        @Override
        public Result<?> onResult(Result<?> rs) throws SQLException {
            return rs.update().postCloseTask(this::checkError).build();
        }

        @Override
        protected SQLException onException(Exception e) {
            Throwable cause = e;
            Throwable rootCause;
            do {
                // unable to access bridge server
                if (cause instanceof SocketException || cause instanceof UnknownHostException) {
                    return super.onException(e);
                }
                rootCause = cause;
                cause = cause.getCause();
            } while (cause != null);

            try {
                checkError();
            } catch (UnexpectedServerException exp) {
                return new SQLException(exp.getMessage(), rootCause);
            }

            return super.onException(e);
        }

        @Override
        public Result<?> onQuery(String query) throws SQLException {
            return super.onQuery(rewrite(query));
        }

        @Override
        public String onQuery(PreparedStatement statement, String query) throws SQLException {
            return super.onQuery(statement, rewrite(query));
        }
    }

    static void updateBridgeConfig(Properties bridgeConfig, String url, VariableTag tag, String bridgeToken) {
        try {
            final StringBuilder builder = ActivityListener.getUrlBuilder(url);
            Properties config = new Properties();
            WebExecutor web = ActivityListener.getTempWebExecutor(tag, config);
            try (InputStream input = web.get(Utils.toURL(builder.append("config").toString()), config,
                    Collections.singletonMap(RequestParameter.FORMAT.header(), Format.TXT.mimeType()))) {
                bridgeConfig.load(input);
            }

            if (!Checker.isNullOrEmpty(bridgeToken)
                    && Boolean.parseBoolean(Option.SERVER_AUTH.getValue(bridgeConfig))) {
                bridgeConfig.setProperty(DriverExtension.PROPERTY_BRIDGE_TOKEN, bridgeToken);
            }
        } catch (Exception e) {
            log.error("Failed to update bridge server config from: " + url, e);
        }
    }

    /**
     * Builds new configuration for {@link WebInterpreter} according to the given
     * query context and configuration.
     *
     * @param context non-null query context with both bridge context and dialect
     * @param config  non-null configuration
     * @return non-null new configuration for {@link WebInterpreter}
     */
    static Properties build(QueryContext context, Properties config) {
        final Properties props = new Properties(config);
        final Properties bridgeCtx = (Properties) context.get(QueryContext.KEY_BRIDGE);
        final JdbcDialect dialect = (JdbcDialect) context.get(QueryContext.KEY_DIALECT);

        final String defaultBridgeUrl = OPTION_URL.getValue(bridgeCtx);
        final String path = OPTION_PATH.getValue(config);

        String url = OPTION_URL.getValue(config, defaultBridgeUrl);
        if (url.isEmpty()) {
            url = defaultBridgeUrl;
        } else if (url != defaultBridgeUrl) { // NOSONAR
            // here url could be different from the server.url returned from bridge server
            updateBridgeConfig(bridgeCtx, url, context.getVariableTag(), OPTION_TOKEN.getValue(config));
        }
        if (!Checker.isNullOrEmpty(path)) { // URL takes priority and then path
            StringBuilder builder = new StringBuilder(url);
            int len = builder.length() - 1;
            if (len < 0) { // should not happen, as bridge URL is never empty
                builder.append(path);
            } else if (builder.charAt(len) == '/') {
                builder.append(path.charAt(0) == '/' ? path.substring(1) : path);
            } else {
                if (path.charAt(0) != '/') {
                    builder.append('/');
                }
                builder.append(path);
            }
            WebInterpreter.OPTION_URL_TEMPLATE.setValue(props, builder.toString());
        } else { // fall back to the default bridge server
            WebInterpreter.OPTION_URL_TEMPLATE.setValue(props, url);
        }

        StringBuilder builder = new StringBuilder(WebExecutor.HEADER_USER_AGENT).append('=')
                .append(Utils.escape(bridgeCtx.getProperty(DriverExtension.PROPERTY_PRODUCT, Constants.PRODUCT_NAME),
                        ','));

        // access token
        String value = OPTION_TOKEN.getValue(config);
        if (Checker.isNullOrEmpty(value)) {
            value = OPTION_TOKEN.getValue(bridgeCtx);
        }
        if (!Checker.isNullOrEmpty(value)) {
            WebInterpreter.OPTION_AUTH_BEARER_TOKEN.setValue(props, value);
        }

        // query user
        value = bridgeCtx.getProperty(DriverExtension.PROPERTY_USER);
        if (!Checker.isNullOrEmpty(value)) {
            builder.append(',').append(RequestParameter.USER.header()).append('=').append(Utils.escape(value, ','));
        }

        // query mode, might have been covered in URL/path
        value = OPTION_QUERY_MODE.getValue(config);
        if (!Checker.isNullOrEmpty(value)) {
            builder.append(',').append(RequestParameter.MODE.header()).append('=')
                    .append(Checker.isNullOrEmpty(value) ? QueryMode.ASYNC.code() : QueryMode.of(value).code());
        }

        // data format, might have been covered in URL/path
        value = OPTION_FORMAT.getValue(config);
        if (Checker.isNullOrEmpty(value)) {
            value = Option.SERVER_FORMAT.getValue(bridgeCtx);
        }
        if (!Checker.isNullOrEmpty(value)) {
            Format format = Format.fromFileExtension(value, null);
            if (format == null) {
                format = Format.valueOf(value.toUpperCase(Locale.ROOT));
            }
            builder.append(',').append(RequestParameter.FORMAT.header()).append('=')
                    .append(dialect == null ? format.mimeType() : dialect.getMimeTypes(format));
        }

        // compression algorithm, might have been covered in URL/path
        value = OPTION_COMPRESSION.getValue(config);
        if (Checker.isNullOrEmpty(value)) {
            value = Option.SERVER_COMPRESSION.getValue(bridgeCtx);
        }
        if (!Checker.isNullOrEmpty(value)) {
            Compression compress = Compression.fromFileExtension(value, null);
            if (compress == null) {
                compress = Compression.valueOf(value.toUpperCase(Locale.ROOT));
            }
            builder.append(',').append(RequestParameter.COMPRESSION.header()).append('=')
                    .append(dialect == null ? compress.encoding() : dialect.getEncodings(compress));
        }

        final String queryId = UUID.randomUUID().toString();
        context.put(KEY_QUERY_ID, queryId);
        builder.append(',').append(RequestParameter.QUERY_ID.header()).append('=').append(queryId);

        WebInterpreter.OPTION_REQUEST_HEADERS.setValue(props, builder.toString());
        return props;
    }

    public static final List<Option> OPTIONS = Collections.unmodifiableList(Arrays.asList(Option.EXEC_ERROR,
            Option.EXEC_TIMEOUT, OPTION_URL, OPTION_QUERY_MODE, OPTION_COMPRESSION, OPTION_FORMAT, OPTION_TOKEN,
            WebExecutor.OPTION_CONNECT_TIMEOUT, Option.INPUT_FILE, Option.PROXY, WebExecutor.OPTION_SOCKET_TIMEOUT,
            WebInterpreter.OPTION_REQUEST_HEADERS));

    @Override
    public List<Option> getDefaultOptions() {
        return OPTIONS;
    }

    @Override
    public ActivityListener createListener(QueryContext context, Connection conn, Properties props) {
        return new ActivityListener(context,
                build(context, getConfig((ConfigManager) context.get(QueryContext.KEY_CONFIG), props)));
    }

    @Override
    public String getDescription() {
        return "Extension for accessing remote datasets via bridge server.";
    }

    @Override
    public String getUsage() {
        return "{{ bridge(url=http://127.0.0.1:8080/): select 1 }}";
    }

    @Override
    public boolean requiresBridgeContext() {
        return true;
    }
}
