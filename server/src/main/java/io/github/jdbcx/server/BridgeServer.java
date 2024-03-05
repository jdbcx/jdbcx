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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import io.github.jdbcx.Checker;
import io.github.jdbcx.Compression;
import io.github.jdbcx.Constants;
import io.github.jdbcx.Format;
import io.github.jdbcx.Logger;
import io.github.jdbcx.LoggerFactory;
import io.github.jdbcx.Option;
import io.github.jdbcx.QueryMode;
import io.github.jdbcx.Result;
import io.github.jdbcx.Utils;
import io.github.jdbcx.Version;
import io.github.jdbcx.driver.ConnectionManager;
import io.github.jdbcx.executor.WebExecutor;
import io.micrometer.core.instrument.binder.cache.CaffeineCacheMetrics;
import io.micrometer.core.instrument.binder.system.UptimeMetrics;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;

public abstract class BridgeServer implements RemovalListener<String, QueryInfo> {
    private static final Logger log = LoggerFactory.getLogger(BridgeServer.class);

    // format
    public static final String HEADER_ACCEPT = WebExecutor.HEADER_ACCEPT.toLowerCase(Locale.ROOT);
    // compression
    public static final String HEADER_ACCEPT_ENCODING = WebExecutor.HEADER_ACCEPT_ENCODING.toLowerCase(Locale.ROOT);
    public static final String HEADER_AUTHORIZATION = WebExecutor.HEADER_AUTHORIZATION.toLowerCase(Locale.ROOT);
    public static final String HEADER_CONTENT_ENCODING = "content-encoding";
    public static final String HEADER_CONTENT_TYPE = "content-type";
    public static final String HEADER_LOCATION = "location";
    public static final String HEADER_QUERY_ID = "x-query-id";
    public static final String HEADER_QUERY_MODE = WebExecutor.HEADER_QUERY_MODE.toLowerCase(Locale.ROOT);
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
    public static final String PATH_METRICS = "metrics";

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
    public static final Option OPTION_ACL = Option.of("server.acl",
            "Server access control list, defaults to acl.properties in current directory", "acl.properties");
    public static final Option OPTION_SECRET = Option
            .of(new String[] { "server.secret",
                    "Server secret for ACL encryption, only used when authentication & authorization are enabled" });

    protected static final List<ServerAcl> loadAcl() {
        final String fileName = Utils
                .normalizePath(System.getProperty(Option.PROPERTY_PREFIX.concat(OPTION_ACL.getName()),
                        OPTION_ACL.getDefaultValue()));
        if (Checker.isNullOrEmpty(fileName)) {
            log.debug("No ACL config file specified");
            return Collections.emptyList();
        }

        final List<ServerAcl> acl;
        Path path = Paths.get(fileName);
        if (!path.isAbsolute()) {
            path = Paths.get(Constants.CURRENT_DIR, fileName).normalize();
        }
        File file = path.toFile();
        if (file.exists() && file.canRead()) {
            Properties props = new Properties();
            try (Reader reader = new InputStreamReader(new FileInputStream(file), Constants.DEFAULT_CHARSET)) {
                props.load(reader);
                log.debug("Loaded ACL config from file \"%s\".", fileName);
            } catch (IOException e) {
                log.warn("Failed to load ACL config from file \"%s\"", fileName, e);
            }

            final String suffix = ".token";
            Set<String> list = new LinkedHashSet<>();
            for (Entry<Object, Object> e : props.entrySet()) {
                String key = (String) e.getKey();
                String val = ((String) e.getValue()).trim();
                if (key.endsWith(suffix) && !val.isEmpty()) {
                    list.add(key.substring(0, key.length() - suffix.length()));
                }
            }

            if (list.isEmpty()) {
                acl = Collections.emptyList();
            } else {
                List<ServerAcl> items = new ArrayList<>(list.size());
                for (String key : list) {
                    // TODO decrypt token using server.secret
                    items.add(new ServerAcl(props.getProperty(key + suffix), props.getProperty(key + ".hosts"),
                            props.getProperty(key + ".ips")));
                }
                acl = Collections.unmodifiableList(items);
            }
        } else {
            log.debug("Skip loading ACL config as file \"%s\" is not accessible.", fileName);
            acl = Collections.emptyList();
        }
        return acl;
    }

    protected static final Properties loadConfig() {
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

    private final PrometheusMeterRegistry promRegistry;

    private final Cache<String, QueryInfo> queries;

    protected final HikariDataSource datasource;
    protected final long queryTimeout;

    protected final boolean auth;
    protected final String host;
    protected final int port;
    protected final String baseUrl;
    protected final String context;
    protected final int backlog;
    // protected final int threads;

    protected final String tag;

    protected final Format defaultFormat;
    protected final Compression defaultCompress;

    private final List<ServerAcl> acl;
    private final Properties essentials;

    protected BridgeServer(Properties props) {
        auth = Boolean.parseBoolean(Option.SERVER_AUTH.getJdbcxValue(props));
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

        promRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        promRegistry.config().commonTags("instance", baseUrl);
        new UptimeMetrics().bindTo(promRegistry);

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
        config.setPoolName("default");
        config.setMetricRegistry(promRegistry);
        datasource = new HikariDataSource(config);

        queryTimeout = Long.parseLong(OPTION_QUERY_TIMEOUT.getJdbcxValue(props));

        final long requestLimit = Long.parseLong(OPTION_REQUEST_LIMIT.getJdbcxValue(props));
        final long requestTimeout = Long.parseLong(OPTION_REQUEST_TIMEOUT.getJdbcxValue(props));

        // TODO load persistent requests from disk file or database
        Caffeine<String, QueryInfo> builder = Caffeine.newBuilder()
                .maximumSize(requestLimit > 0L ? requestLimit : DEFAULT_REQUEST_LIMIT).removalListener(this);
        if (requestTimeout > 0L) {
            builder.expireAfterWrite(requestTimeout, TimeUnit.MILLISECONDS);
        }
        queries = builder.recordStats().scheduler(Scheduler.systemScheduler()).build();
        CaffeineCacheMetrics.monitor(promRegistry, queries, "query");

        backlog = Integer.parseInt(OPTION_BACKLOG.getJdbcxValue(props));

        tag = Option.TAG.getJdbcxValue(props);

        defaultFormat = Format.valueOf(Option.SERVER_FORMAT.getJdbcxValue(props));
        defaultCompress = Compression.valueOf(Option.SERVER_COMPRESSION.getJdbcxValue(props));

        acl = auth ? loadAcl() : Collections.emptyList();
        essentials = new Properties();
        Option.SERVER_AUTH.setValue(essentials, Boolean.toString(auth));
        Option.SERVER_URL.setValue(essentials, baseUrl);
        if (!Checker.isNullOrEmpty(tag)) {
            Option.TAG.setValue(essentials, tag);
        }
        if (defaultFormat != null) {
            Option.SERVER_FORMAT.setValue(essentials, defaultFormat.name());
        }
        if (defaultCompress != null && defaultCompress != Compression.NONE) {
            Option.SERVER_COMPRESSION.setValue(essentials, defaultCompress.name());
        }
        log.info("Initialized bridge server with below configuration:\n%s", essentials);
    }

    protected Request create(String method, QueryMode mode, String qid, String query, String txid, Format format,
            Compression compress, String token, String user, String client, Object implementation) throws IOException { // NOSONAR
        final QueryInfo info;
        final Request request;
        if (Checker.isNullOrBlank(query) && !Checker.isNullOrEmpty(qid) && (info = queries.getIfPresent(qid)) != null) {
            log.debug("Loaded query from cache: %s", info);
            request = new Request(method, mode, info, ConnectionManager.findDialect(info.client), implementation);
        } else {
            if (!Checker.isNullOrEmpty(token)) {
                try {
                    token = new String(Base64.getDecoder().decode(token), Constants.DEFAULT_CHARSET);
                } catch (Exception e) {
                    log.warn("Failed to decode token [%s] due to: %s", token, e.getMessage());
                }
            }
            request = new Request(method, mode, qid, query, txid, format, compress, token, user, client,
                    ConnectionManager.findDialect(client), implementation);
        }
        return request;
    }

    protected final Properties getConfig() {
        Properties props = new Properties();
        props.putAll(essentials);
        return props;
    }

    protected final void writeConfig(OutputStream out) throws IOException {
        essentials.store(out, Utils.format("Bridge server %s configuration", Version.current().toShortString()));
    }

    protected final void writeMetrics(OutputStream out) throws IOException {
        try (Writer writer = new BufferedWriter(new OutputStreamWriter(out, Constants.DEFAULT_CHARSET),
                Constants.DEFAULT_BUFFER_SIZE)) {
            promRegistry.scrape(writer);
        }
    }

    protected void query(Request request) throws IOException {
        final QueryInfo info = request.getQueryInfo();
        log.debug("Executing query [%s]...", info.qid);
        boolean success = false;
        try (Connection conn = datasource.getConnection();
                Statement stmt = conn.createStatement();
                Result<?> result = request.isMutation() ? Result.of(stmt.executeLargeUpdate(info.query))
                        : Result.of(stmt.executeQuery(info.query))) {
            try (OutputStream out = request.getCompression().provider().compress(prepareResponse(request))) {
                success = true; // query was a success and we got the response output stream without any issue
                final SQLWarning warning = stmt.getWarnings();
                if (warning != null) {
                    log.warn("SQLWarning from [%s]", stmt, warning);
                }
                Result.writeTo(result, info.format, null, out);
            }
            // in case the query took too long
            queries.put(info.qid, info);
        } catch (SQLException e) {
            // invalidate the query now so that client-side retry later will end up with 404
            queries.invalidate(info.qid);
            log.debug("Invalidated query [%s] due to error: %s", info.qid, e.getMessage());
            throw new IOException(e);
        } finally {
            if (!success) {
                respond(request, HttpURLConnection.HTTP_INTERNAL_ERROR);
            }
        }
        log.debug("Query [%s] finished successfully", info.qid);
    }

    protected void queryAsync(Request request) throws IOException { // NOSONAR
        final QueryInfo info = request.getQueryInfo();
        log.debug("Executing async query [%s]...", info.qid);

        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            conn = datasource.getConnection();
            stmt = conn.createStatement();
            final Result<?> result;
            if (request.isMutation()) {
                result = Result.of(stmt.executeQuery(info.query));
            } else {
                result = Result.of(rs = stmt.executeQuery(info.query));
            }
            queries.put(info.qid, info.setResult(result));
            log.debug("Async query [%s] returned successfully", info.qid);
            respond(request, HttpURLConnection.HTTP_OK, request.toUrl(baseUrl));
        } catch (SQLException e) {
            // invalidate the query now so that client-side retry later will end up with 404
            queries.invalidate(info.qid);
            log.debug("Invalidated query [%s] due to error: %s", info.qid, e.getMessage());
            throw new IOException(e);
        } finally {
            if (rs != null || stmt != null || conn != null) {
                info.setResources(rs, stmt, conn);
            }
        }
    }

    /**
     * Sets necessary response headers like {@code Content-Type} and
     * {@code Content-Encoding} before writing any data.
     *
     * @param request non-null request object
     * @throws IOException when failed to set response headers
     */
    protected abstract void setResponseHeaders(Request request) throws IOException;

    /**
     * Sets response code to 200 and gets raw response output stream for writing.
     *
     * @param request non-null request object
     * @return raw response output stream
     * @throws IOException when failed to get response output stream
     */
    protected abstract OutputStream prepareResponse(Request request) throws IOException;

    protected abstract void redirect(Request request) throws IOException;

    protected abstract void respond(Request request, int code, String message) throws IOException;

    protected final void respond(Request request, int code) throws IOException {
        respond(request, code, null);
    }

    protected final boolean checkAcl(String token, InetAddress address) {
        log.debug("Checking ACL...");
        for (ServerAcl sa : acl) {
            if (sa.token.equals(token) && sa.isValid(address)) {
                return true;
            }
        }
        return false;
    }

    protected abstract void showConfig(Object implementation) throws IOException;

    protected abstract void showMetrics(Object implementation) throws IOException;

    protected void dispatch(String method, String path, InetSocketAddress clientAddress, String encodedToken,
            Map<String, String> headers, Map<String, String> params, Object implementation) throws IOException {
        if (!path.startsWith(context)) {
            throw new IOException(Utils.format("Request URI must starts with [%s]", context));
        } else {
            path = path.substring(context.length());
            if (!path.isEmpty() && path.charAt(0) == '/') {
                path = path.substring(1);
            }
        }

        if (PATH_CONFIG.equals(path)) {
            log.debug("Sending server configuration to %s", clientAddress);
            showConfig(implementation);
            return;
        } else if (PATH_METRICS.equals(path)) {
            log.debug("Sending server metrics to %s", clientAddress);
            showMetrics(implementation);
            return;
        }

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

        final String queryMode = params.getOrDefault(PARAM_QUERY_MODE, headers.get(HEADER_QUERY_MODE));
        final QueryMode mode;
        if (Checker.isNullOrEmpty(queryMode)) {
            mode = Checker.isNullOrEmpty(qid) ? QueryMode.SUBMIT : QueryMode.DIRECT;
        } else {
            mode = QueryMode.of(queryMode);
        }
        Request request = create(method, mode, qid, params.get(PARAM_QUERY), txid, format, compress, encodedToken,
                headers.get(HEADER_QUERY_USER), headers.get(HEADER_USER_AGENT), implementation);
        log.debug("Dispatching %s", request);
        if (Checker.isNullOrBlank(request.getQuery())) {
            log.warn("Non-existent or expired %s", request);
            respond(request, HttpURLConnection.HTTP_NOT_FOUND);
        } else if (METHOD_HEAD.equals(request.getMethod())) {
            respond(request, HttpURLConnection.HTTP_OK);
        } else {
            switch (request.getQueryMode()) {
                case SUBMIT: {
                    queries.put(request.getQueryId(), request.getQueryInfo());
                    respond(request, HttpURLConnection.HTTP_OK, request.toUrl(baseUrl));
                    break;
                }
                case REDIRECT: {
                    queries.put(request.getQueryId(), request.getQueryInfo());
                    redirect(request);
                    break;
                }
                case ASYNC: {
                    if (auth && !checkAcl(request.getQueryInfo().token, clientAddress.getAddress())) {
                        respond(request, HttpURLConnection.HTTP_FORBIDDEN);
                    } else {
                        setResponseHeaders(request);
                        queryAsync(request);
                    }
                    break;
                }
                case DIRECT:
                case MUTATION: {
                    if (auth && !checkAcl(request.getQueryInfo().token, clientAddress.getAddress())) {
                        respond(request, HttpURLConnection.HTTP_FORBIDDEN);
                    } else {
                        setResponseHeaders(request);
                        if (request.hasResult()) {
                            log.debug("Reusing cached result for query [%s]...", request.getQueryId());
                            try (QueryInfo info = request.getQueryInfo();
                                    Result<?> result = info.getResult();
                                    OutputStream out = request.getCompression().provider()
                                            .compress(prepareResponse(request))) {
                                Result.writeTo(result, info.format, null, out);
                                log.debug("Query [%s] finished successfully", info.qid);
                            }
                        } else {
                            query(request);
                        }
                    }
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

    @Override
    public void onRemoval(String key, QueryInfo value, RemovalCause cause) {
        if (value != null) {
            log.debug("Releasing cached query [%s] due to %s", key, cause);
            final Result<?> result = value.getResult();
            if (result != null && result.isActive()) {
                log.debug("Skip active result and relevant resources as they are being used somewhere else");
            } else {
                value.close();
                log.debug("Released cached query [%s] due to %s", key, cause);
            }
        }
    }
}
