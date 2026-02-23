/*
 * Copyright 2022-2026, Zhichun Wu
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
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
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
import io.github.jdbcx.ConfigManager;
import io.github.jdbcx.Constants;
import io.github.jdbcx.Format;
import io.github.jdbcx.Logger;
import io.github.jdbcx.LoggerFactory;
import io.github.jdbcx.Option;
import io.github.jdbcx.QueryContext;
import io.github.jdbcx.QueryMode;
import io.github.jdbcx.RequestParameter;
import io.github.jdbcx.Result;
import io.github.jdbcx.Threads;
import io.github.jdbcx.Utils;
import io.github.jdbcx.ValueFactory;
import io.github.jdbcx.Version;
import io.github.jdbcx.WrappedDriver;
import io.github.jdbcx.driver.ConnectionManager;
import io.github.jdbcx.driver.ManagedConnection;
import io.github.jdbcx.driver.QueryParser;
import io.github.jdbcx.driver.WrappedConnection;
import io.github.jdbcx.executor.JdbcExecutor;
import io.github.jdbcx.executor.WebExecutor;
import io.github.jdbcx.interpreter.JdbcInterpreter;
import io.github.jdbcx.interpreter.JsonHelper;
import io.micrometer.core.instrument.binder.cache.CaffeineCacheMetrics;
import io.micrometer.core.instrument.binder.system.UptimeMetrics;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;

public abstract class BridgeServer implements RemovalListener<String, QueryInfo> {
    private static final Logger log = LoggerFactory.getLogger(BridgeServer.class);

    public static final String CLAIM_ALLOWED_HOSTS = "allowed_hosts";
    public static final String CLAIM_ALLOWED_IPS = "allowed_ips";
    public static final String CLAIM_SUBJECT = "sub";

    public static final String HEADER_ACCEPT_RANGES = "accept-ranges";
    public static final String HEADER_AUTHORIZATION = WebExecutor.HEADER_AUTHORIZATION.toLowerCase(Locale.ROOT);
    public static final String HEADER_CONNECTION = "connection";
    public static final String HEADER_CONTENT_ENCODING = "content-encoding";
    public static final String HEADER_CONTENT_TYPE = "content-type";
    public static final String HEADER_JDBCX_PREFIX = Constants.PRODUCT_NAME.toLowerCase(Locale.ROOT) + "_";
    public static final String HEADER_LOCATION = "location";

    public static final String METHOD_HEAD = "HEAD";

    public static final List<String> DB_EXTENSIONS = Collections.unmodifiableList(Arrays.asList("db", "sql"));

    public static final String PARAM_SUBJECT = "subject";
    public static final String PARAM_HOSTS = "hosts";
    public static final String PARAM_IPS = "ips";
    public static final String PARAM_EXPIRES = "expires";

    public static final String PATH_CONFIG = "config";
    public static final String PATH_ENCRYPT = "encrypt";
    public static final String PATH_ERROR = "error/";
    public static final String PATH_METRICS = "metrics";
    public static final String PATH_REGISTER = "register";

    public static final String CONNECTION_CLOSE = "close";
    public static final String RANGE_NONE = "none";

    public static final long DEFAULT_USER_LIMIT = 100L;
    public static final long DEFAULT_REQUEST_LIMIT = 10000L;

    public static final Option OPTION_DATASOURCE_CONFIG = Option.of("server.datasource.config",
            "Path to HikariCP configuration file, defaults to datasource.properties in current directory",
            "datasource.properties");
    public static final Option OPTION_USER_LIMIT = Option.ofLong("server.user.limit",
            "Maximum number of authorized users will be kept in memory, zero or negative number means "
                    + DEFAULT_USER_LIMIT + ".",
            DEFAULT_USER_LIMIT);
    public static final Option OPTION_REQUEST_LIMIT = Option.ofLong("server.request.limit",
            "Maximum submitted requests will be kept in memory, zero or negative number means " + DEFAULT_REQUEST_LIMIT
                    + ".",
            DEFAULT_REQUEST_LIMIT);
    public static final Option OPTION_REQUEST_TIMEOUT = Option.of("server.request.timeout",
            "Milliseconds before a submitted request expires, zero or negative number means no expiration.", "10000");
    public static final Option OPTION_QUERY_TIMEOUT = Option.of("server.query.timeout",
            "Maximum query execution time in millisecond", "30000");

    public static final Option OPTION_BACKLOG = Option.of("server.backlog", "Server backlog", "0");
    public static final Option OPTION_THREADS = Option.of("server.threads", "Maximum size of the thread pool.",
            String.valueOf(Math.max(Threads.DEFAULT_POOL_SIZE, Constants.MIN_CORE_THREADS * 2)));

    protected static final Properties extractConfig(Map<String, String> headers, Map<String, String> params) {
        Properties config = new Properties();
        config.putAll(params);
        for (RequestParameter p : RequestParameter.values()) {
            config.remove(p.parameter());
        }
        for (Entry<String, String> entry : headers.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith(HEADER_JDBCX_PREFIX)) {
                key = key.substring(HEADER_JDBCX_PREFIX.length()).replace('_', '.');
                final String val;
                if (!key.isEmpty() && (!Checker.isNullOrEmpty(val = entry.getValue()))) { // NOSONAR
                    config.setProperty(key, val);
                }
            }
        }
        return config;
    }

    private final PrometheusMeterRegistry promRegistry;

    private final Cache<String, ServerAcl> acls;
    private final Cache<String, String> errors;
    private final Cache<String, QueryInfo> queries;

    protected final HikariDataSource datasource;
    protected final long queryTimeout;

    protected final boolean auth;
    protected final String host;
    protected final int port;
    protected final String baseUrl;
    protected final String context;
    protected final int backlog;
    protected final int threads;

    protected final String tag;

    protected final Format defaultFormat;
    protected final Compression defaultCompress;

    private final ConfigManager cm;
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
            int index = url.indexOf(Constants.PROTOCOL_DELIMITER);
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

        log.debug("Instantiating configuration manager...");
        final ConfigManager configManager = ConfigManager.newInstance(props);
        configManager.decrypt(props, null); // in case there's anything encrypted

        log.debug("Initializing metrics registry...");
        promRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        promRegistry.config().commonTags("instance", baseUrl);
        new UptimeMetrics().bindTo(promRegistry);

        log.debug("Initializing connection pool...");
        final String dsConfigFile = OPTION_DATASOURCE_CONFIG.getJdbcxValue(props);
        HikariConfig config = null;
        if (!Checker.isNullOrEmpty(dsConfigFile)) {
            log.debug("Loading HikariConfig from [%s]...", dsConfigFile);
            try {
                config = new HikariConfig(configManager.load(dsConfigFile, props));
            } catch (Exception e) {
                log.warn(e.getMessage());
            }
        }
        if (config == null) {
            Properties properties = new Properties(props);
            String key = "jdbcUrl";
            properties.setProperty(key, properties.getProperty(key, ConnectionManager.JDBCX_PREFIX));
            config = new HikariConfig(properties);
        }
        if (config.getPoolName() == null || config.getPoolName().startsWith("HikariPool-")) {
            config.setPoolName("default");
        }
        config.setMetricRegistry(promRegistry);
        if (Utils.startsWith(config.getJdbcUrl(), ConnectionManager.JDBCX_PREFIX, true)) {
            config.setDriverClassName(WrappedDriver.class.getName()); // activate the driver
            // FIXME needs a better way to pass all JDBCX properties from server to driver
            for (Option o : new Option[] { Option.CONFIG_PATH, Option.SERVER_AUTH, Option.SERVER_URL,
                    Option.SERVER_TOKEN, Option.TENANT }) {
                config.addDataSourceProperty(o.getJdbcxName(), o.getJdbcxValue(props));
            }
        }
        datasource = new HikariDataSource(config);
        try (Connection conn = datasource.getConnection()) {
            WrappedConnection wrappedConn = conn.unwrap(WrappedConnection.class);
            cm = wrappedConn != null ? wrappedConn.getManager().getConfigManager() : configManager;
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }

        queryTimeout = Long.parseLong(OPTION_QUERY_TIMEOUT.getJdbcxValue(props));

        final long userLimit = Long.parseLong(OPTION_USER_LIMIT.getJdbcxValue(props));
        final long requestLimit = Long.parseLong(OPTION_REQUEST_LIMIT.getJdbcxValue(props));
        final long requestTimeout = Long.parseLong(OPTION_REQUEST_TIMEOUT.getJdbcxValue(props));

        log.debug("Initializing query cache...");
        acls = Caffeine.newBuilder().maximumSize(userLimit > 0L ? userLimit : DEFAULT_USER_LIMIT).recordStats().build();
        CaffeineCacheMetrics.monitor(promRegistry, acls, "acl");

        // TODO load persistent requests from disk file or database
        errors = Caffeine.newBuilder().maximumSize(requestLimit > 0L ? requestLimit : DEFAULT_REQUEST_LIMIT)
                .recordStats().build();
        CaffeineCacheMetrics.monitor(promRegistry, errors, "error");

        Caffeine<String, QueryInfo> builder = Caffeine.newBuilder()
                .maximumSize(requestLimit > 0L ? requestLimit : DEFAULT_REQUEST_LIMIT).removalListener(this);
        if (requestTimeout > 0L) {
            builder.expireAfterWrite(requestTimeout, TimeUnit.MILLISECONDS).scheduler(Scheduler.systemScheduler());
        }
        queries = builder.recordStats().build();
        CaffeineCacheMetrics.monitor(promRegistry, queries, "query");

        backlog = Integer.parseInt(OPTION_BACKLOG.getJdbcxValue(props));
        threads = Integer.parseInt(OPTION_THREADS.getJdbcxValue(props));

        tag = Option.TAG.getJdbcxValue(props);

        defaultFormat = Format.valueOf(Option.SERVER_FORMAT.getJdbcxValue(props));
        defaultCompress = Compression.valueOf(Option.SERVER_COMPRESSION.getJdbcxValue(props));

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

    protected Request create(String method, QueryMode mode, String rawParams, String qid, String query, String txid,
            Format format, Compression compress, String token, String user, String client, String tenant,
            Object implementation) throws IOException { // NOSONAR
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
                    // ignore the error for security reason
                }
            }
            request = new Request(method, mode, rawParams, qid, query, txid, format, compress, token, user, client,
                    tenant, ConnectionManager.findDialect(client), implementation);
        }
        return request;
    }

    protected final Properties getConfig() {
        Properties props = new Properties();
        props.putAll(essentials);
        return props;
    }

    protected final void writeConfig(OutputStream out) throws IOException {
        essentials.store(out, Utils.format("%s bridge server %s configuration", Constants.PRODUCT_NAME,
                Version.current().toShortString()));
    }

    protected final void writeMetrics(OutputStream out) throws IOException {
        promRegistry.scrape(out);
    }

    protected final void writeNamedDataSources(Writer writer, String extension) throws IOException {
        writer.write('[');
        try (Connection conn = datasource.getConnection()) {
            ManagedConnection managedConn = conn.unwrap(ManagedConnection.class);
            if (managedConn != null) {
                ConfigManager manager = managedConn.getManager().getConfigManager();
                StringBuilder builder = new StringBuilder();
                for (String id : manager.getAllIDs(extension)) {
                    Properties props = manager.getConfig(extension, id);
                    builder.append('{').append(Constants.JSON_PROP_ID).append(JsonHelper.encode(id)).append(',')
                            .append(Constants.JSON_PROP_DESC)
                            .append(JsonHelper.encode(Option.DESCRIPTION.getJdbcxValue(props)))
                            .append('}');
                    writer.write(builder.toString());
                    builder.setLength(0);
                    builder.append(',');
                }
            }
        } catch (Exception e) {
            // ignore
        }
        writer.write(']');
    }

    protected final void appendJsonIfNotEmpty(Writer writer, String str, boolean encode)
            throws IOException {
        if (!Checker.isNullOrEmpty(str)) {
            writer.write(',');
            writer.write(encode ? JsonHelper.encode(str) : str);
        }
    }

    protected final void writeDataSourceConfig(Writer writer, String extension, String name) throws IOException {
        writer.write('{');
        try (Connection conn = datasource.getConnection()) {
            ManagedConnection managedConn = conn.unwrap(ManagedConnection.class);
            writer.write(Constants.JSON_PROP_TYPE);
            writer.write(JsonHelper.encode(extension));
            if (managedConn == null) {
                return;
            }

            ConfigManager manager = managedConn.getManager().getConfigManager();
            Properties props = manager.getConfig(extension, name);
            writer.write(',');
            writer.write(Constants.JSON_PROP_ID);
            writer.write(JsonHelper.encode(name));
            String str = ConfigManager.OPTION_ALIAS.getJdbcxValue(props);
            if (!Checker.isNullOrEmpty(str)) {
                writer.write(',');
                writer.write(Constants.JSON_PROP_ALIASES);
                writer.write(JsonHelper.encode(Utils.split(str, ',', true, true, true)));
            }
            str = Option.DESCRIPTION.getJdbcxValue(props);
            if (!Checker.isNullOrEmpty(str)) {
                writer.write(',');
                writer.write(Constants.JSON_PROP_DESC);
                writer.write(JsonHelper.encode(str));
            }

            if (DB_EXTENSIONS.contains(extension)) {
                try (Connection c = JdbcInterpreter.getConnectionByConfig(props, null)) {
                    final DatabaseMetaData metaData = c.getMetaData();
                    // database product
                    appendJsonIfNotEmpty(writer, JdbcInterpreter.getDatabaseProduct(metaData), false);
                    // current catalog & schema
                    appendJsonIfNotEmpty(writer, JdbcInterpreter.getCurrentDatabaseCatalogAndSchema(c, metaData),
                            false);
                    // databases
                    appendJsonIfNotEmpty(writer, JdbcInterpreter.getDatabaseCatalogs(metaData, name,
                            JdbcInterpreter.DEFAULT_TABLE_PATTERN, JdbcInterpreter.DEFAULT_TABLE_TYPES), false);
                }
            }
        } catch (Exception e) {
            log.warn("Failed to inspect datasource [%s].[%s]", extension, name, e);
        }
        writer.write('}');
    }

    protected final void writeDataSourceDetail(Writer writer, String extension, String name, String detail)
            throws IOException {
        writer.write('{');
        try (Connection conn = datasource.getConnection()) {
            ManagedConnection managedConn = conn.unwrap(ManagedConnection.class);
            writer.write(Constants.JSON_PROP_TYPE);
            writer.write(JsonHelper.encode(extension));
            if (managedConn == null) {
                return;
            }

            ConfigManager manager = managedConn.getManager().getConfigManager();
            Properties props = manager.getConfig(extension, name);
            if (DB_EXTENSIONS.contains(extension)) {
                try (Connection c = JdbcInterpreter.getConnectionByConfig(props, null)) {
                    String json = JdbcInterpreter.getDatabaseTable(c, detail);
                    if (!Checker.isNullOrEmpty(json)) {
                        writer.write(',');
                        writer.write(json);
                    }
                }
            } else {
                String value = props.getProperty(detail);
                if (!Checker.isNullOrEmpty(value)) {
                    writer.write(',');
                    writer.write(JsonHelper.encode(detail));
                    writer.write(':');
                    writer.write(JsonHelper.encode(value));
                }
            }
        } catch (Exception e) {
            log.warn("Failed to inspect [%s] of datasource [%s].[%s]", detail, extension, name, e);
        }
        writer.write('}');
    }

    protected int batch(Request request, Properties config) throws IOException {
        final QueryInfo info = request.getQueryInfo();
        final List<String[]> list = QueryParser.split(info.query);
        final int len = list.size();
        log.debug("Executing batch query [%s] (%d queries)...", info.qid, len);

        int responesCode = HttpURLConnection.HTTP_OK;
        boolean success = false;
        int i = 1;
        String name = null;
        try (Connection conn = datasource.getConnection(); Statement stmt = conn.createStatement()) {
            Result<?> lastResult = null;
            for (String[] q : list) {
                name = q[0];
                log.debug("Executing batch query [%s] %d/%d [%s]...", info.qid, i, len, name);
                // don't use executeBatch as we need mixed queries
                boolean isResultSet = stmt.execute(q[1]); // NOSONAR
                final SQLWarning warning = stmt.getWarnings();
                if (warning != null) {
                    log.warn("SQLWarning from [%s]", stmt, warning);
                }
                if (i < len) {
                    if (isResultSet) {
                        stmt.getResultSet().close();
                    }
                } else {
                    lastResult = isResultSet ? Result.of(stmt.getResultSet())
                            : Result.of(JdbcExecutor.getUpdateCount(stmt));
                }
                i++;
            }

            try (Result<?> result = lastResult;
                    OutputStream out = request.getCompression().provider().compress(prepareResponse(request))) {
                success = true;
                Result.writeTo(result, info.format, config, out);
            }
        } catch (SQLException e) {
            log.error("Failed to execute batch query [%s] %d/%d [%s].", info.qid, i, len, name, e);
            throw new IOException(e);
        } finally {
            if (!success) {
                responesCode = respond(request, HttpURLConnection.HTTP_INTERNAL_ERROR);
            }
        }
        log.debug("Batch query [%s] (%d queries) finished successfully", info.qid, len);
        return responesCode;
    }

    protected int query(Request request, Properties config) throws IOException {
        final QueryInfo info = request.getQueryInfo();
        log.debug("Executing query [%s]...", info.qid);
        int responseCode = HttpURLConnection.HTTP_OK;
        String errorMessage = "Unknown error";
        boolean responded = false;
        try (Connection conn = datasource.getConnection(); Statement stmt = conn.createStatement();) {
            if (request.hasTenantId()) {
                QueryContext.getCurrentContext().put(QueryContext.KEY_TENANT, request.getTenant());
            }
            // request.isMutation()
            try (Result<?> result = stmt.execute(info.query) ? Result.of(stmt.getResultSet())
                    : Result.of(JdbcExecutor.getUpdateCount(stmt))) {
                try (OutputStream out = request.getCompression().provider().compress(prepareResponse(request))) {
                    errorMessage = null; // query was a success and we got the response output stream without any issue
                    final SQLWarning warning = stmt.getWarnings();
                    if (warning != null) {
                        log.warn("SQLWarning from [%s]", stmt, warning);
                    }
                    responded = true;
                    Result.writeTo(result, info.format, config, out);
                }
            }
            // in case the query took too long
            queries.put(info.qid, info);
        } catch (SQLException e) {
            errorMessage = e.getMessage();
            // invalidate the query now so that client-side retry later will end up with 404
            queries.invalidate(info.qid);
            log.debug("Invalidated query [%s] due to error: %s", info.qid, errorMessage);
            throw new IOException(e);
        } catch (Exception e) {
            errorMessage = e.getMessage();
            throw e;
        } finally {
            if (errorMessage != null && !responded) {
                respond(request, responseCode = HttpURLConnection.HTTP_INTERNAL_ERROR, errorMessage);
            }
        }
        log.debug("Query [%s] finished successfully", info.qid);
        return responseCode;
    }

    protected int queryAsync(Request request, Properties config) throws IOException { // NOSONAR
        final QueryInfo info = request.getQueryInfo();
        log.debug("Executing async query [%s]...", info.qid);

        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            conn = datasource.getConnection();
            stmt = conn.createStatement();

            if (request.hasTenantId()) {
                QueryContext.getCurrentContext().put(QueryContext.KEY_TENANT, request.getTenant());
            }

            final Result<?> result;
            if (stmt.execute(info.query)) {
                rs = stmt.getResultSet();
                result = Result.of(rs);
            } else {
                result = Result.of(JdbcExecutor.getUpdateCount(stmt));
            }
            info.setResources(rs, stmt, conn);
            rs = null;
            stmt = null;
            conn = null;

            queries.put(info.qid, info.setResult(result));
            log.debug("Async query [%s] returned successfully", info.qid);
            return respond(request, HttpURLConnection.HTTP_OK, request.toUrl(baseUrl));
        } catch (SQLException e) {
            final String errorMsg = e.getMessage();
            errors.put(info.qid, errorMsg);
            log.warn("Failed to execute query [%s] due to error: %s", info.qid, errorMsg);
            throw new IOException(e);
        } catch (Exception e) {
            errors.put(info.qid, e.getMessage());
            throw e;
        } finally {
            if (rs != null || stmt != null || conn != null) {
                Utils.closeQuietly(rs, stmt, conn);
                log.debug("Invalidated query [%s] due to error", info.qid);
                queries.invalidate(info.qid);
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

    protected abstract int redirect(Request request) throws IOException;

    protected abstract int respond(Request request, int code, String message) throws IOException;

    protected final int respond(Request request, int code) throws IOException {
        return respond(request, code, null);
    }

    protected final boolean checkAcl(String token, InetAddress address) {
        if (!auth) {
            return true;
        }

        log.debug("Checking ACL...");
        if (Checker.isNullOrEmpty(token)) {
            log.warn("Missing access token from [%s]", address);
        } else {
            Map<String, String> claims = cm.verifyToken(baseUrl, token);
            if (!claims.isEmpty()) {
                ServerAcl acl = acls.getIfPresent(token);
                if (acl == null) {
                    acls.put(token,
                            acl = new ServerAcl(claims.get(CLAIM_ALLOWED_HOSTS), claims.get(CLAIM_ALLOWED_IPS)));
                }
                if (acl.isValid(address)) {
                    log.debug("Authorized request from user [%s] at [%s]", claims.get(CLAIM_SUBJECT), address);
                    return true;
                }
            }
            log.warn("Unauthorized request from [%s]", address);
        }
        return false;
    }

    protected abstract void setResponse(Object implementation, int responseCode, String responseMsg, String... headers)
            throws IOException;

    protected abstract OutputStream getResponseStream(Object implementation, int responseCode, String... headers)
            throws IOException;

    protected final OutputStream getResponseStream(Object implementation, String... headers)
            throws IOException {
        return getResponseStream(implementation, HttpURLConnection.HTTP_OK, headers);
    }

    protected final Writer getResponseWriter(Object implementation, int responseCode, String... headers)
            throws IOException {
        return new OutputStreamWriter(getResponseStream(implementation, responseCode, headers),
                Constants.DEFAULT_CHARSET);
    }

    protected final Writer getResponseWriter(Object implementation, String... headers) throws IOException {
        return new OutputStreamWriter(getResponseStream(implementation, headers), Constants.DEFAULT_CHARSET);
    }

    protected final int respondEncrypt(InetSocketAddress clientAddress, Request request) throws IOException {
        log.debug("Sending encrypted secrets to %s", clientAddress);
        final String tenant = request.getTenant();
        if (tenant.isEmpty()) {
            return respond(request, HttpURLConnection.HTTP_BAD_REQUEST);
        } else if (!checkAcl(request.getQueryInfo().token, clientAddress.getAddress())) {
            return respond(request, HttpURLConnection.HTTP_FORBIDDEN);
        }

        try (Writer writer = getResponseWriter(request.implementation, HEADER_CONTENT_TYPE, Format.JSON.mimeType())) {
            Map<String, String> secrets = ValueFactory.flatMapFromJson(request.getQuery());
            Map<String, String> encrypted = new LinkedHashMap<>();
            for (Entry<String, String> s : secrets.entrySet()) {
                final String key = s.getKey();
                final String val = s.getValue();
                if (Checker.isNullOrEmpty(val)) {
                    continue;
                }

                if (key.endsWith(ConfigManager.PROPERTY_ENCRYPTED_SUFFIX)) {
                    encrypted.put(key, val);
                } else {
                    encrypted.put(key + ConfigManager.PROPERTY_ENCRYPTED_SUFFIX, cm.encrypt(val, tenant, null));
                }
            }
            writer.write(ValueFactory.toJson(encrypted));
        }
        return HttpURLConnection.HTTP_OK;
    }

    protected final int respondRegister(InetSocketAddress clientAddress, Request request) throws IOException {
        log.debug("Sending registration to %s", clientAddress);
        final String tenant = request.getTenant();
        if (tenant.isEmpty()) {
            return respond(request, HttpURLConnection.HTTP_BAD_REQUEST);
        } else if (!checkAcl(request.getQueryInfo().token, clientAddress.getAddress())) {
            return respond(request, HttpURLConnection.HTTP_FORBIDDEN);
        }

        try (Writer writer = getResponseWriter(request.implementation, HEADER_CONTENT_TYPE, Format.JSON.mimeType())) {
            Map<String, String> encryptedSecrets = ValueFactory.flatMapFromJson(request.getQuery());
            Properties secrets = new Properties();
            for (Entry<String, String> s : encryptedSecrets.entrySet()) {
                final String key = s.getKey();
                final String val = s.getValue();
                if (Checker.isNullOrEmpty(val)) {
                    continue;
                }

                if (key.endsWith(ConfigManager.PROPERTY_ENCRYPTED_SUFFIX)) {
                    secrets.put(key.substring(0, key.length() - ConfigManager.PROPERTY_ENCRYPTED_SUFFIX.length()),
                            cm.decrypt(val, tenant, null));
                }
            }
            if (secrets.size() > 0) {
                cm.register(tenant, secrets);
            }
        }
        return HttpURLConnection.HTTP_OK;
    }

    protected final int respondMetrics(InetSocketAddress clientAddress, Object implementation) throws IOException {
        log.debug("Sending server metrics to %s", clientAddress);
        try (OutputStream out = getResponseStream(implementation, HEADER_CONTENT_TYPE,
                Format.TXT.mimeType())) {
            writeMetrics(out);
        }
        return HttpURLConnection.HTTP_OK;
    }

    protected final boolean respondConfig(String path, InetSocketAddress clientAddress, Object implementation)
            throws IOException {
        boolean processed = true;
        if (path.length() == PATH_CONFIG.length()) {
            log.debug("Sending server configuration to %s", clientAddress);
            try (OutputStream out = getResponseStream(implementation, HEADER_CONTENT_TYPE,
                    Format.TXT.mimeType())) {
                writeConfig(out);
            }
        } else if (path.charAt(PATH_CONFIG.length()) == '/') {
            List<String> list = Utils.split(path.substring(PATH_CONFIG.length() + 1), '/');
            switch (list.size()) {
                case 1: // list named datasources
                    try (Writer writer = getResponseWriter(implementation, HEADER_CONTENT_TYPE,
                            Format.JSON.mimeType())) {
                        writeNamedDataSources(writer, list.get(0));
                    }
                    break;
                case 2: // show details of the named datasource
                    try (Writer writer = getResponseWriter(implementation, HEADER_CONTENT_TYPE,
                            Format.JSON.mimeType())) {
                        writeDataSourceConfig(writer, list.get(0), list.get(1));
                    }
                    break;
                case 3: // show specific detail of the named datasource
                    try (Writer writer = getResponseWriter(implementation, HEADER_CONTENT_TYPE,
                            Format.JSON.mimeType())) {
                        writeDataSourceDetail(writer, list.get(0), list.get(1), list.get(2));
                    }
                    break;
                default:
                    processed = false;
                    break;
            }
        }
        return processed;
    }

    protected final int respondError(String path, InetSocketAddress clientAddress, Object implementation)
            throws IOException {
        log.debug("Sending query error to %s", clientAddress);
        final int responseCode;
        String errorMsg = errors.getIfPresent(path.substring(PATH_ERROR.length()));
        if (Checker.isNullOrEmpty(errorMsg)) {
            setResponse(implementation, responseCode = HttpURLConnection.HTTP_NOT_FOUND, null, HEADER_CONTENT_TYPE,
                    Format.TXT.mimeType());
        } else {
            setResponse(implementation, responseCode = HttpURLConnection.HTTP_OK, errorMsg, HEADER_CONTENT_TYPE,
                    Format.TXT.mimeType());
        }
        return responseCode;
    }

    protected final int respondQuery(InetSocketAddress clientAddress, Properties config, Request request)
            throws IOException {
        log.debug("Dispatching %s", request);
        final int responseCode;
        if (Checker.isNullOrBlank(request.getQuery())) {
            log.warn("Non-existent or expired %s", request);
            responseCode = respond(request, HttpURLConnection.HTTP_NOT_FOUND);
        } else if (METHOD_HEAD.equals(request.getMethod())) {
            responseCode = respond(request, HttpURLConnection.HTTP_OK);
        } else {
            switch (request.getQueryMode()) {
                case SUBMIT: {
                    queries.put(request.getQueryId(), request.getQueryInfo());
                    responseCode = respond(request, HttpURLConnection.HTTP_OK, request.toUrl(baseUrl));
                    break;
                }
                case REDIRECT: {
                    queries.put(request.getQueryId(), request.getQueryInfo());
                    responseCode = redirect(request);
                    break;
                }
                case ASYNC: {
                    if (checkAcl(request.getQueryInfo().token, clientAddress.getAddress())) {
                        setResponseHeaders(request);
                        responseCode = queryAsync(request, config);
                    } else {
                        responseCode = respond(request, HttpURLConnection.HTTP_FORBIDDEN);
                    }
                    break;
                }
                case BATCH:
                    if (checkAcl(request.getQueryInfo().token, clientAddress.getAddress())) {
                        setResponseHeaders(request);
                        responseCode = batch(request, config);
                    } else {
                        responseCode = respond(request, HttpURLConnection.HTTP_FORBIDDEN);
                    }
                    break;
                case DIRECT:
                case MUTATION: {
                    if (checkAcl(request.getQueryInfo().token, clientAddress.getAddress())) {
                        setResponseHeaders(request);
                        final int state = request.getResultState();
                        if (state == 1) { // result ready for reading
                            log.debug("Reusing cached query [%s]...", request.getQueryId());
                            try (QueryInfo info = request.getQueryInfo();
                                    Result<?> result = info.getResult();
                                    OutputStream out = request.getCompression().provider()
                                            .compress(prepareResponse(request))) {
                                Result.writeTo(result, info.format, config, out);
                                log.debug("Query [%s] finished successfully", info.qid);
                            }
                            responseCode = HttpURLConnection.HTTP_OK;
                        } else if (state == 0) { // no result
                            responseCode = query(request, config);
                        } else { // active result
                            responseCode = respond(request, HttpURLConnection.HTTP_NO_CONTENT);
                        }
                    } else {
                        responseCode = respond(request, HttpURLConnection.HTTP_FORBIDDEN);
                    }
                    break;
                }
                default:
                    log.warn("Unsupported %s", request);
                    responseCode = respond(request, HttpURLConnection.HTTP_BAD_REQUEST,
                            "Unsupported query mode: " + request.getQueryMode());
                    break;

            }
        }
        return responseCode;
    }

    protected int dispatch(String method, String path, String rawParams, InetSocketAddress clientAddress,
            String encodedToken, Map<String, String> headers, Object implementation) throws IOException {
        if (!path.startsWith(context)) {
            throw new IOException(Utils.format("Request URI must starts with [%s]", context));
        }
        path = path.substring(context.length());
        if (!path.isEmpty() && path.charAt(0) == '/') {
            path = path.substring(1);
        }

        if (PATH_METRICS.equals(path)) {
            return respondMetrics(clientAddress, implementation);
        } else if (path.startsWith(PATH_CONFIG)) {
            if (respondConfig(path, clientAddress, implementation)) {
                return HttpURLConnection.HTTP_OK;
            }
        } else if (path.startsWith(PATH_ERROR)) {
            return respondError(path, clientAddress, implementation);
        }

        final Map<String, String> params = Utils.toKeyValuePairs(rawParams, '&', true);
        String qid = RequestParameter.QUERY_ID.getValue(headers, params, Constants.EMPTY_STRING);
        String txid = RequestParameter.TRANSACTION_ID.getValue(headers, params, Constants.EMPTY_STRING);

        if (PATH_ENCRYPT.equals(path)) {
            return respondEncrypt(clientAddress,
                    create(method, QueryMode.DIRECT, rawParams, qid, Constants.EMPTY_STRING, txid, Format.JSON,
                            Compression.NONE, encodedToken, RequestParameter.USER.getValue(headers, params),
                            RequestParameter.AGENT.getValue(headers, params),
                            RequestParameter.TENANT_ID.getValue(headers, params), implementation));
        } else if (PATH_REGISTER.equals(path)) {
            return respondRegister(clientAddress,
                    create(method, QueryMode.DIRECT, rawParams, qid, Constants.EMPTY_STRING, txid, Format.JSON,
                            Compression.NONE, encodedToken, RequestParameter.USER.getValue(headers, params),
                            RequestParameter.AGENT.getValue(headers, params),
                            RequestParameter.TENANT_ID.getValue(headers, params), implementation));
        }

        final QueryMode defaultMode;
        final int index = path.indexOf('/');
        QueryMode m = QueryMode.fromPath(index > 0 ? path.substring(0, index) : path, null);
        if (m != null) {
            defaultMode = m;
            if (index > 0) {
                path = path.substring(index + 1);
            } else {
                path = Constants.EMPTY_STRING;
            }
        } else {
            defaultMode = null;
        }

        // priorities: header > parameter > path > default
        final String compressParam = RequestParameter.COMPRESSION.getValue(null, params, Constants.EMPTY_STRING);
        final String formatParam = RequestParameter.FORMAT.getValue(null, params, Constants.EMPTY_STRING);

        List<String> parts = Utils.split(path, '.');
        int size = parts.size();
        Compression compress = Compression.fromEncoding(headers.get(RequestParameter.COMPRESSION.header()),
                defaultCompress);
        Format format = Format.fromMimeType(headers.get(RequestParameter.FORMAT.header()), defaultFormat);
        if (size >= 3) { // my.tsv.gz
            compress = Compression.fromFileExtension(
                    Checker.isNullOrEmpty(compressParam) ? parts.get(size - 1) : compressParam, compress);
            format = Format.fromFileExtension(Checker.isNullOrEmpty(formatParam) ? parts.get(size - 2) : formatParam,
                    format);
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
                format = Format.fromFileExtension(Checker.isNullOrEmpty(formatParam) ? ext : formatParam, format);
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

        final String queryMode = RequestParameter.MODE.getValue(headers, params, Constants.EMPTY_STRING);
        final QueryMode mode;
        if (queryMode.isEmpty()) {
            if (defaultMode != null) {
                mode = defaultMode;
            } else if (Checker.isNullOrEmpty(qid)) {
                mode = QueryMode.SUBMIT;
            } else {
                mode = QueryMode.DIRECT;
            }
        } else {
            mode = QueryMode.of(queryMode);
        }
        return respondQuery(clientAddress, extractConfig(headers, params), create(method, mode, rawParams, qid,
                RequestParameter.QUERY.getValue(headers, params, Constants.EMPTY_STRING), txid, format, compress,
                encodedToken, RequestParameter.USER.getValue(headers, params),
                RequestParameter.AGENT.getValue(headers, params), RequestParameter.TENANT_ID.getValue(headers, params),
                implementation));
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
                log.debug(
                        "Released cached query [%s] without closing associated resources as they are currently in use by other processes",
                        key);
            } else {
                value.close();
                log.debug("Released cached query [%s] and all associated resources", key);
            }
        }
    }
}
