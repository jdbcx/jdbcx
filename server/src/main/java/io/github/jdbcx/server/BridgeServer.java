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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import io.github.jdbcx.Checker;
import io.github.jdbcx.Compression;
import io.github.jdbcx.Constants;
import io.github.jdbcx.Format;
import io.github.jdbcx.JdbcDialect;
import io.github.jdbcx.Logger;
import io.github.jdbcx.LoggerFactory;
import io.github.jdbcx.Option;
import io.github.jdbcx.Result;
import io.github.jdbcx.Utils;
import io.github.jdbcx.Version;
import io.github.jdbcx.driver.ConnectionManager;
import io.github.jdbcx.executor.WebExecutor;

public abstract class BridgeServer {
    private static final Logger log = LoggerFactory.getLogger(BridgeServer.class);

    public static final String HEADER_ACCEPT = "accept"; // format
    public static final String HEADER_ACCEPT_ENCODING = "accept-encoding"; // compression
    public static final String HEADER_CONTENT_ENCODING = "content-encoding";
    public static final String HEADER_CONTENT_TYPE = "content-type";
    public static final String HEADER_LOCATION = "location";
    public static final String HEADER_QUERY_ID = "x-query-id";
    public static final String HEADER_QUERY_USER = WebExecutor.HEADER_QUERY_USER.toLowerCase(Locale.ROOT);
    public static final String HEADER_TRANSACTION_ID = "x-transaction-id";
    public static final String HEADER_USER_AGENT = WebExecutor.HEADER_USER_AGENT.toLowerCase(Locale.ROOT);

    public static final String METHOD_HEAD = "HEAD";

    public static final String PARAM_COMPRESSION = "c";
    public static final String PARAM_FORMAT = "f";
    public static final String PARAM_REDIRECT = "r";
    public static final String PARAM_QUERY = "q";
    public static final String PARAM_QUERY_MODE = "m";

    public static final String PATH_CONFIG = "config";

    public static final long DEFAULT_REQUEST_LIMIT = 10000L;

    public static final Option OPTION_DATASOURCE_CONFIG = Option.of("server.datasource.config",
            "Path to HikariCP configuration file, defaults to datasource.properties in current directory",
            "datasource.properties");
    public static final Option OPTION_REQUEST_LIMIT = Option.of("server.request.limit",
            "Maximum submitted requests will be kept in memory, zero or negative number means " + DEFAULT_REQUEST_LIMIT
                    + ".",
            String.valueOf(DEFAULT_REQUEST_LIMIT));
    public static final Option OPTION_REQUEST_TIMEOUT = Option.of("server.request.timeout",
            "Milliseconds before a submitted request expires, zero or negative number means no expiration.", "10000");
    public static final Option OPTION_QUERY_TIMEOUT = Option.of("server.query.timeout",
            "Maximum query execution time in millisecond", "30000");

    public static final Option OPTION_BACKLOG = Option.of("server.backlog", "Server backlog", "0");
    public static final Option OPTION_FORMAT = Option.ofEnum("server.format", "Server response format", null,
            Format.class);
    public static final Option OPTION_COMPRESSION = Option.ofEnum("server.compress", "Server response compression",
            null, Compression.class);

    protected static final Properties load() {
        Properties sysProps = System.getProperties();
        final String fileName = Utils
                .normalizePath(sysProps.getProperty(Option.PROPERTY_PREFIX.concat(Option.CONFIG_PATH.getName()),
                        Option.CONFIG_PATH.getDefaultValue()));

        if (Checker.isNullOrEmpty(fileName)) {
            log.debug("No default config file specified");
            return sysProps;
        }

        Properties defProps = new Properties(sysProps);
        Path path = Paths.get(fileName);
        if (!path.isAbsolute()) {
            path = Paths.get(Constants.CURRENT_DIR, fileName).normalize();
        }
        File file = path.toFile();
        if (file.exists() && file.canRead()) {
            try (Reader reader = new InputStreamReader(new FileInputStream(file), Constants.DEFAULT_CHARSET)) {
                defProps.load(reader);
                log.debug("Loaded default config from file \"%s\".", fileName);
            } catch (IOException e) {
                log.warn("Failed to load default config from file \"%s\"", fileName, e);
            }
        } else {
            log.debug("Skip loading default config as file \"%s\" is not accessible.", fileName);
        }
        return defProps;
    }

    private final Cache<String, String> queries;

    protected final HikariDataSource datasource;
    protected final long queryTimeout;

    protected final String host;
    protected final int port;
    protected final String baseUrl;
    protected final String context;
    protected final int backlog;
    // protected final int threads;

    protected final String tag;

    protected final Format defaultFormat;
    protected final Compression defaultCompress;

    private final Properties essentials;

    protected BridgeServer(Properties props) {
        String dsConfigFile = OPTION_DATASOURCE_CONFIG.getJdbcxValue(props);
        HikariConfig config = null;
        if (!Checker.isNullOrEmpty(dsConfigFile)) {
            try {
                config = new HikariConfig(dsConfigFile);
            } catch (Exception e) {
                log.warn(e.getMessage());
            }
        }
        if (config == null) {
            Properties properties = new Properties(props);
            String key = "jdbcUrl";
            properties.setProperty(key, properties.getProperty(key, "jdbcx:"));
            config = new HikariConfig(properties);
        }
        datasource = new HikariDataSource(config);

        queryTimeout = Long.parseLong(OPTION_QUERY_TIMEOUT.getJdbcxValue(props));

        final long requestLimit = Long.parseLong(OPTION_REQUEST_LIMIT.getJdbcxValue(props));
        final long requestTimeout = Long.parseLong(OPTION_REQUEST_TIMEOUT.getJdbcxValue(props));

        // TODO load persistent requests from disk file or database
        Caffeine<Object, Object> builder = Caffeine.newBuilder()
                .maximumSize(requestLimit > 0L ? requestLimit : DEFAULT_REQUEST_LIMIT);
        if (requestTimeout > 0L) {
            builder.expireAfterWrite(requestTimeout, TimeUnit.MILLISECONDS);
        }
        queries = builder.recordStats().<String, String>build();

        host = Option.SERVER_HOST.getJdbcxValue(props);
        port = Integer.parseInt(Option.SERVER_PORT.getJdbcxValue(props));

        final String url = Option.SERVER_URL.getJdbcxValue(props);
        if (Checker.isNullOrEmpty(url)) {
            String path = Option.SERVER_CONTEXT.getJdbcxValue(props);
            if (Checker.isNullOrEmpty(path)) {
                path = "/";
            } else {
                if (path.charAt(0) != '/') {
                    path = "/".concat(path);
                }
                if (path.charAt(path.length() - 1) != '/') {
                    path = path.concat("/");
                }
            }
            context = path;
            baseUrl = Utils.format("http://%s:%d%s", Checker.isNullOrEmpty(host) ? "127.0.0.1" : host, port, context);
        } else {
            int index = url.indexOf("://");
            if (index == -1) {
                throw new IllegalArgumentException(Utils.format("Invalid %s [%s]", Option.SERVER_URL.getName(), url));
            }
            index = url.indexOf('/', index + 3);
            if (index == -1) {
                context = "/";
                baseUrl = url.concat(context);
            } else {
                baseUrl = url.charAt(url.length() - 1) == '/' ? url : url.concat("/");
                context = baseUrl.substring(index);
            }
        }

        backlog = Integer.parseInt(OPTION_BACKLOG.getJdbcxValue(props));

        tag = Option.TAG.getJdbcxValue(props);

        defaultFormat = Format.valueOf(OPTION_FORMAT.getJdbcxValue(props));
        defaultCompress = Compression.valueOf(OPTION_COMPRESSION.getJdbcxValue(props));

        essentials = new Properties();
        Option.SERVER_URL.setValue(essentials, baseUrl);
        if (!Checker.isNullOrEmpty(tag)) {
            Option.TAG.setValue(essentials, tag);
        }
        if (defaultFormat != null) {
            OPTION_FORMAT.setValue(essentials, defaultFormat.name());
        }
        if (defaultCompress != null && defaultCompress != Compression.NONE) {
            OPTION_COMPRESSION.setValue(essentials, defaultCompress.name());
        }
        log.info("Initialized bridge server with below configuration:\n%s", essentials);
    }

    protected Request create(String method, QueryMode mode, String qid, String query, String txid, Format format,
            Compression compress, JdbcDialect dialect, Object userObject) throws IOException { // NOSONAR
        if (Checker.isNullOrBlank(query) && !Checker.isNullOrEmpty(qid)) {
            query = queries.getIfPresent(qid);
            log.debug("Loaded query [%s] from cache:\n%s", qid, query);
        }
        return new Request(method, mode, qid, query, txid, format, compress, dialect, userObject);
    }

    protected final Properties getConfig() {
        Properties props = new Properties();
        props.putAll(essentials);
        return props;
    }

    protected final void writeConfig(OutputStream out) throws IOException {
        essentials.store(out, Utils.format("Bridge server %s configuration", Version.current().toShortString()));
    }

    protected void query(Request request) throws IOException {
        log.debug("Executing query [%s]...", request.getQueryId());
        boolean success = false;
        try (Connection conn = datasource.getConnection();
                Statement stmt = conn.createStatement();
                Result<?> result = request.isMutation() ? Result.of(stmt.executeLargeUpdate(request.getQuery()))
                        : Result.of(stmt.executeQuery(request.getQuery()))) {
            try (OutputStream out = request.getCompression().provider().compress(getResponseOutputStream(request))) {
                success = true; // query was a success and we got the response output stream without any issue
                final SQLWarning warning = stmt.getWarnings();
                if (warning != null) {
                    log.warn("SQLWarning from [%s]", stmt, warning);
                }
                Result.writeTo(result, request.getFormat(), null, out);
            }
            // in case the query took too long
            queries.put(request.getQueryId(), request.getQuery());
        } catch (SQLException e) {
            // invalidate the query now so that client-side retry later will end up with 404
            queries.invalidate(request.getQueryId());
            log.debug("Invalidated query [%s] due to error: %s", request.getQueryId(), e.getMessage());
            throw new IOException(e);
        } finally {
            if (!success) {
                respond(request, HttpURLConnection.HTTP_INTERNAL_ERROR);
            }
        }
        log.debug("Query [%s] finished successfully", request.getQueryId());
    }

    protected abstract void execute(Request request) throws IOException;

    /**
     * Sets response code to 200 and gets raw response output stream.
     *
     * @param request non-null request object
     * @return raw response output stream
     * @throws IOException when failed to get response output stream
     */
    protected abstract OutputStream getResponseOutputStream(Request request) throws IOException;

    protected abstract void redirect(Request request) throws IOException;

    protected abstract void respond(Request request, int code, String message) throws IOException;

    protected final void respond(Request request, int code) throws IOException {
        respond(request, code, null);
    }

    protected abstract void showConfig(Object userObject) throws IOException;

    protected void dispatch(String method, String path, Map<String, String> headers, Map<String, String> params,
            Object userObject) throws IOException {
        if (!path.startsWith(context)) {
            throw new IOException(Utils.format("Request URI must starts with [%s]", context));
        } else if (path.endsWith(PATH_CONFIG) && (path.length() == context.length() + PATH_CONFIG.length())) {
            log.debug("Sending server configuration");
            showConfig(userObject);
            return;
        } else {
            path = path.substring(context.length());
            if (!path.isEmpty() && path.charAt(0) == '/') {
                path = path.substring(1);
            }
        }

        final JdbcDialect dialect = ConnectionManager.findDialect(headers.get(HEADER_USER_AGENT));

        String qid = headers.get(HEADER_QUERY_ID);
        String txid = headers.get(HEADER_TRANSACTION_ID);

        // priorities: parameter > path > header > default
        final String compressParam = params.get(PARAM_COMPRESSION);
        final String formatParam = params.get(PARAM_FORMAT);

        List<String> parts = Utils.split(path, '.');
        int size = parts.size();
        Compression compress = Compression.fromEncoding(headers.get(HEADER_ACCEPT_ENCODING), defaultCompress);
        Format format = Format.fromMimeType(headers.get(HEADER_ACCEPT), defaultFormat);
        if (size >= 3) { // my.tsv.gz
            compress = Checker.isNullOrEmpty(compressParam)
                    ? Compression.fromFileExtension(parts.get(size - 1), compress)
                    : Compression.fromFileExtension(compressParam, compress);
            format = Checker.isNullOrEmpty(formatParam) ? Format.fromFileExtension(parts.get(size - 2), format)
                    : Format.fromFileExtension(formatParam, format);
            if (Checker.isNullOrEmpty(qid)) {
                qid = String.join(".", parts.subList(0, size - 2));
            }
        } else if (size == 2) { // my.gz or my.tsv
            String ext = parts.get(1);
            Compression c = Compression.fromFileExtension(ext, null);
            if (c != null) {
                compress = Checker.isNullOrEmpty(compressParam) ? c
                        : Compression.fromFileExtension(compressParam, compress);
            } else {
                format = Checker.isNullOrEmpty(formatParam) ? Format.fromFileExtension(ext, format)
                        : Format.fromFileExtension(formatParam, format);
            }
            if (Checker.isNullOrEmpty(qid)) {
                qid = parts.get(0);
            }
        } else if (Checker.isNullOrEmpty(qid)) {
            qid = path;

            if (!Checker.isNullOrEmpty(compressParam)) {
                compress = Compression.fromFileExtension(compressParam, compress);
            }
            if (!Checker.isNullOrEmpty(formatParam)) {
                format = Format.fromFileExtension(formatParam, format);
            }
        }

        final String queryMode = params.get(PARAM_QUERY_MODE);
        final QueryMode mode;
        if (Checker.isNullOrEmpty(queryMode)) {
            mode = Checker.isNullOrEmpty(qid) ? QueryMode.SUBMIT_QUERY : QueryMode.DIRECT_QUERY;
        } else {
            mode = QueryMode.of(queryMode);
        }
        Request request = create(method, mode, qid, params.get(PARAM_QUERY), txid, format, compress, dialect,
                userObject);
        log.debug("Dispatching %s", request);
        if (Checker.isNullOrBlank(request.getQuery())) {
            log.warn("Non-existent or expired %s", request);
            respond(request, HttpURLConnection.HTTP_NOT_FOUND);
        } else if (METHOD_HEAD.equals(request.getMethod())) {
            respond(request, HttpURLConnection.HTTP_OK);
        } else {
            switch (request.getQueryMode()) {
                case SUBMIT_QUERY: { // submit query only
                    queries.put(request.getQueryId(), request.getQuery());
                    respond(request, HttpURLConnection.HTTP_OK, request.toUrl(baseUrl));
                    break;
                }
                case SUBMIT_REDIRECT: { // submit query and then redirect
                    queries.put(request.getQueryId(), request.getQuery());
                    redirect(request);
                    break;
                }
                case DIRECT_QUERY:
                case MUTATION: {
                    execute(request);
                    // queries.invalidate(request.getQueryId());
                    // log.debug("Explicitly removed cached query [%s]", request.getQueryId());
                    break;
                }
                default:
                    log.warn("Unsupported %s", request);
                    respond(request, HttpURLConnection.HTTP_BAD_REQUEST,
                            "Unsupported query mode: " + request.getQueryMode());
                    break;

            }
        }
    }

    public final String getBaseUrl() {
        return baseUrl;
    }

    public abstract void start();

    public void stop() {
        log.debug("Invaliding query cache: %s", queries.stats());
        queries.invalidateAll();
        log.debug("Remaining entries in query cache: %s", queries.asMap());

        log.debug("Stoping connection pool: %s", datasource);
        datasource.close();
    }

    public static void main(String[] args) {
        final BridgeServer server = Utils.getService(BridgeServer.class);

        Runtime.getRuntime().addShutdownHook(new Thread(server::stop));

        server.start();
    }
}
