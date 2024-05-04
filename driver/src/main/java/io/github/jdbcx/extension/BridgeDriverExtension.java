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
package io.github.jdbcx.extension;

import java.io.InputStream;
import java.net.URL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

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
import io.github.jdbcx.executor.WebExecutor;
import io.github.jdbcx.interpreter.WebInterpreter;

public class BridgeDriverExtension implements DriverExtension {
    private static final Logger log = LoggerFactory.getLogger(BridgeDriverExtension.class);

    static final Option OPTION_COMPRESSION = Option.of(new String[] { "compression", "Compression algorithm" });
    static final Option OPTION_FORMAT = Option.of(new String[] { "format", "Data format" });
    static final Option OPTION_QUERY_MODE = Option.of(new String[] { "mode", "Query mode" });
    static final Option OPTION_PATH = Option.of(new String[] { DriverExtension.PROPERTY_PATH, "URL path" });
    static final Option OPTION_URL = Option
            .of(new String[] { DriverExtension.PROPERTY_BRIDGE_URL, "Bridge server URL" });
    static final Option OPTION_TOKEN = Option
            .of(new String[] { DriverExtension.PROPERTY_BRIDGE_TOKEN, "Secure token for accessing bridge server" });

    static class ActivityListener extends AbstractActivityListener {
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
            final StringBuilder builder;
            int index = url.indexOf('?');
            if (index != -1) {
                builder = new StringBuilder(url.substring(0, index));
            } else {
                builder = new StringBuilder(url);
            }
            if (builder.charAt(builder.length() - 1) != '/') {
                builder.append('/');
            }
            Properties config = new Properties();
            WebExecutor web = new WebExecutor(tag, config);
            WebExecutor.OPTION_CONNECT_TIMEOUT.setValue(config, "1000");
            WebExecutor.OPTION_SOCKET_TIMEOUT.setValue(config, "3000");
            WebExecutor.OPTION_FOLLOW_REDIRECT.setValue(config, Constants.FALSE_EXPR);
            try (InputStream input = web.get(new URL(builder.append("config").toString()), config,
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

        WebInterpreter.OPTION_REQUEST_HEADERS.setValue(props, builder.toString());
        return props;
    }

    public static final List<Option> OPTIONS = Collections.unmodifiableList(Arrays.asList(Option.EXEC_ERROR,
            Option.EXEC_TIMEOUT, OPTION_URL, OPTION_QUERY_MODE, OPTION_COMPRESSION, OPTION_FORMAT, OPTION_TOKEN,
            WebExecutor.OPTION_CONNECT_TIMEOUT, WebExecutor.OPTION_PROXY, WebExecutor.OPTION_SOCKET_TIMEOUT,
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
