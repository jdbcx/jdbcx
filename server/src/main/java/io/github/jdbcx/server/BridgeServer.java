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

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import io.github.jdbcx.Checker;
import io.github.jdbcx.Compression;
import io.github.jdbcx.Field;
import io.github.jdbcx.Format;
import io.github.jdbcx.Logger;
import io.github.jdbcx.LoggerFactory;
import io.github.jdbcx.Option;
import io.github.jdbcx.Result;
import io.github.jdbcx.Row;
import io.github.jdbcx.Utils;
import io.github.jdbcx.Value;

public abstract class BridgeServer {
    private static final Logger log = LoggerFactory.getLogger(BridgeServer.class);

    public static final String HEADER_ACCEPT = "Accept";
    public static final String HEADER_ACCEPT_ENCODING = "Accept-Encoding";
    public static final String HEADER_CONTENT_ENCODING = "Content-Encoding";
    public static final String HEADER_CONTENT_TYPE = "Content-Type";
    public static final String HEADER_LOCATION = "Location";
    public static final String HEADER_QUERY_ID = "X-Query-Id";
    public static final String HEADER_QUERY_USER = "X-Query-User";
    public static final String HEADER_TRANSACTION_ID = "X-Transaction-Id";

    public static final String METHOD_HEAD = "HEAD";

    public static final String PARAM_COMPRESSION = "c";
    public static final String PARAM_FORMAT = "f";
    public static final String PARAM_REDIRECT = "r";
    public static final String PARAM_QUERY = "q";
    public static final String PARAM_QUERY_MODE = "m";

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

    public static final Option OPTION_FORMAT = Option.ofEnum("server.format", "Server response format", null,
            Format.class);
    public static final Option OPTION_COMPRESSION = Option.ofEnum("server.compress", "Server response compression",
            null, Compression.class);

    private final Cache<String, String> queries;

    protected final HikariDataSource datasource;
    protected final long queryTimeout;

    protected final String host;
    protected final int port;
    protected final String baseUrl;
    protected final String context;
    protected final int backlog;
    // protected final int threads;

    protected final Format defaultFormat;
    protected final Compression defaultCompress;

    protected BridgeServer(Properties props) {
        String dsConfigFile = OPTION_DATASOURCE_CONFIG.getValue(props);
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

        queryTimeout = Long.parseLong(OPTION_QUERY_TIMEOUT.getValue(props));

        final long requestLimit = Long.parseLong(OPTION_REQUEST_LIMIT.getValue(props));
        final long requestTimeout = Long.parseLong(OPTION_REQUEST_TIMEOUT.getValue(props));

        // TODO load persistent requests from disk file or database
        Caffeine<Object, Object> builder = Caffeine.newBuilder()
                .maximumSize(requestLimit > 0L ? requestLimit : DEFAULT_REQUEST_LIMIT);
        if (requestTimeout > 0L) {
            builder.expireAfterAccess(requestTimeout, TimeUnit.MILLISECONDS);
        }
        queries = builder.<String, String>build();

        host = Option.SERVER_HOST.getValue(props);
        port = Integer.parseInt(Option.SERVER_PORT.getValue(props));

        final String url = Option.SERVER_URL.getValue(props);
        if (Checker.isNullOrEmpty(url)) {
            String path = Option.SERVER_CONTEXT.getValue(props);
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

        backlog = Integer.parseInt(Option.SERVER_BACKLOG.getValue(props, "0"));

        defaultFormat = Format.valueOf(OPTION_FORMAT.getValue(props));
        defaultCompress = Compression.valueOf(OPTION_COMPRESSION.getValue(props));
    }

    protected Request create(String method, QueryMode mode, String qid, String query, String txid, Format format,
            Compression compress, Object userObject) throws IOException { // NOSONAR
        if (Checker.isNullOrBlank(query) && !Checker.isNullOrEmpty(qid)) {
            query = queries.getIfPresent(qid);
            log.debug("Loaded query [%s] from cache:\n%s", qid, query);
        }
        return new Request(method, mode, qid, query, txid, format, compress, userObject);
    }

    protected void query(Request request, OutputStream out) throws IOException {
        boolean success = false;
        try (Connection conn = datasource.getConnection();
                Statement stmt = conn.createStatement();
                Result<?> result = request.isMutation() ? Result.of(stmt.executeLargeUpdate(request.getQuery()))
                        : Result.of(stmt.executeQuery(request.getQuery()))) {
            // final SQLWarning warning = stmt.getWarnings();
            // if (warning != null) {
            // log.warn("Warning from [%s]", stmt, warning);
            // }
            final List<Field> fields = result.fields();
            final int size = fields.size();
            out.write(fields.get(0).name().getBytes());
            for (int i = 1; i < size; i++) {
                Field f = fields.get(i);
                out.write('\t');
                out.write(f.name().getBytes());
            }
            for (Row r : result.rows()) {
                out.write('\n');
                out.write(r.value(0).asString().getBytes());
                for (int i = 1; i < size; i++) {
                    Value v = r.value(i);
                    out.write('\t');
                    out.write(v.asString().getBytes());
                }
            }
            success = true;
        } catch (SQLException e) {
            throw new IOException(e);
        } finally {
            if (!success) {
                log.error("Failed to execute query for %s", request);
            }
            // queries.invalidate(request.getQueryId());
            // log.debug("Explicitly removed cached query [%s]", request.getQueryId());
        }
    }

    protected abstract void execute(Request request) throws IOException;

    protected abstract void redirect(Request request) throws IOException;

    protected abstract void respond(Request request, int code, String message) throws IOException;

    protected final void respond(Request request, int code) throws IOException {
        respond(request, code, null);
    }

    protected void dispatch(String method, String path, Map<String, String> headers, Map<String, String> params,
            Object userObject) throws IOException {
        if (!path.startsWith(context)) {
            throw new IOException(Utils.format("Request URI must starts with [%s]", context));
        } else {
            path = path.substring(context.length());
            if (!path.isEmpty() && path.charAt(0) == '/') {
                path = path.substring(1);
            }
        }

        String qid = headers.get(HEADER_QUERY_ID);
        String txid = headers.get(HEADER_TRANSACTION_ID);

        // priorities: parameter > path > header > default
        final String compressParam = params.get(PARAM_COMPRESSION);
        final String formatParam = params.get(PARAM_FORMAT);

        List<String> parts = Utils.split(path, '.');
        int size = parts.size();
        Compression compress = Compression.fromEncoding(headers.get(HEADER_CONTENT_ENCODING), defaultCompress);
        Format format = Format.fromMimeType(headers.get(HEADER_CONTENT_TYPE), defaultFormat);
        if (size >= 3) { // my.tsv.gz
            compress = Checker.isNullOrEmpty(compressParam)
                    ? Compression.fromFileExtension(parts.get(size - 1), compress)
                    : Compression.fromEncoding(compressParam, compress);
            format = Checker.isNullOrEmpty(formatParam) ? Format.fromFileExtension(parts.get(size - 2), format)
                    : Format.fromMimeType(formatParam, format);
            if (Checker.isNullOrEmpty(qid)) {
                qid = String.join(".", parts.subList(0, size - 2));
            }
        } else if (size == 2) { // my.gz or my.tsv
            String ext = parts.get(1);
            Compression c = Compression.fromFileExtension(ext, null);
            if (c != null) {
                compress = Checker.isNullOrEmpty(compressParam) ? c : Compression.fromEncoding(compressParam, compress);
            } else {
                format = Checker.isNullOrEmpty(formatParam) ? Format.fromFileExtension(ext, format)
                        : Format.fromMimeType(formatParam, format);
            }
            if (Checker.isNullOrEmpty(qid)) {
                qid = parts.get(0);
            }
        } else if (Checker.isNullOrEmpty(qid)) {
            qid = path;

            if (!Checker.isNullOrEmpty(compressParam)) {
                compress = Compression.fromEncoding(compressParam, compress);
            }
            if (!Checker.isNullOrEmpty(formatParam)) {
                format = Format.fromMimeType(formatParam, format);
            }
        }

        final String queryMode = params.get(PARAM_QUERY_MODE);
        final QueryMode mode;
        if (Checker.isNullOrEmpty(queryMode)) {
            mode = Checker.isNullOrEmpty(qid) ? QueryMode.SUBMIT_QUERY : QueryMode.DIRECT_QUERY;
        } else {
            mode = QueryMode.of(queryMode);
        }
        Request request = create(method, mode, qid, params.get(PARAM_QUERY), txid, format, compress, userObject);
        log.debug("Dispatching %s", request);
        if (Checker.isNullOrBlank(request.getQuery())) {
            log.debug("Invalid %s", request);
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
                    break;
                }
                default:
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

    public boolean startAndWait(long timeout) {
        if (timeout < 0) {
            start();
            return test();
        }

        start();

        final long startTime = timeout > 0L ? System.currentTimeMillis() : 0L;
        while (timeout >= 0L) {
            if (test()) {
                return true;
            } else if (startTime > 0L) {
                timeout -= System.currentTimeMillis() - startTime;
            }
        }
        return false;
    }

    public abstract void stop();

    public abstract boolean test();

    public static void main(String[] args) {
        final BridgeServer server = Utils.getService(BridgeServer.class);

        Runtime.getRuntime().addShutdownHook(new Thread(server::stop));

        server.start();
    }
}
