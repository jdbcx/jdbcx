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
package io.github.jdbcx.server.impl;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import io.github.jdbcx.Checker;
import io.github.jdbcx.Compression;
import io.github.jdbcx.ConfigManager;
import io.github.jdbcx.Constants;
import io.github.jdbcx.Format;
import io.github.jdbcx.Logger;
import io.github.jdbcx.LoggerFactory;
import io.github.jdbcx.Option;
import io.github.jdbcx.QueryMode;
import io.github.jdbcx.Utils;
import io.github.jdbcx.Version;
import io.github.jdbcx.executor.Stream;
import io.github.jdbcx.executor.WebExecutor;
import io.github.jdbcx.server.BridgeServer;
import io.github.jdbcx.server.Request;

public final class JdkHttpServer extends BridgeServer implements HttpHandler {
    private static final Logger log = LoggerFactory.getLogger(JdkHttpServer.class);

    private final HttpServer server;

    public JdkHttpServer() {
        this(ConfigManager.loadConfig(Option.CONFIG_PATH.getEffectiveDefaultValue(Option.PROPERTY_PREFIX), null,
                System.getProperties()));
    }

    public JdkHttpServer(Properties props) {
        super(props);

        try {
            server = HttpServer.create(new InetSocketAddress(Checker.isNullOrEmpty(host) ? "0.0.0.0" : host, port),
                    backlog);
        } catch (NumberFormatException | IOException e) {
            throw new IllegalArgumentException(Utils.format("Failed to initialize server(%s)", baseUrl), e);
        }
        server.createContext(context, this);
        server.setExecutor(null); // Use the default executor
    }

    private HttpExchange check(Object implementation) throws IOException {
        if (!(implementation instanceof HttpExchange)) {
            throw new IOException("Require HttpExchange to proceed but we got: " + implementation);
        }
        return (HttpExchange) implementation;
    }

    @Override
    protected Request create(String method, QueryMode mode, String rawParams, String qid, String query, String txid,
            Format format, Compression compress, String token, String user, String client, Object implementation)
            throws IOException {
        if (mode != QueryMode.MUTATION && Checker.isNullOrBlank(query)) {
            HttpExchange exchange = (HttpExchange) implementation;
            query = Stream.readAllAsString(exchange.getRequestBody());
        }
        return super.create(method, mode, rawParams, qid, query, txid, format, compress, token, user, client,
                implementation);
    }

    @Override
    protected void setResponseHeaders(Request request) throws IOException {
        HttpExchange exchange = request.getImplementation(HttpExchange.class);

        Headers headers = exchange.getResponseHeaders();
        headers.set(HEADER_ACCEPT_RANGES, RANGE_NONE);
        headers.set(HEADER_CONNECTION, CONNECTION_CLOSE);
        headers.set(HEADER_CONTENT_TYPE, request.getFormat().mimeType());
        if (request.hasCompression()) {
            headers.set(HEADER_CONTENT_ENCODING, request.getCompression().encoding());
        }
    }

    @Override
    protected OutputStream prepareResponse(Request request) throws IOException {
        HttpExchange exchange = request.getImplementation(HttpExchange.class);
        exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, 0L);
        return exchange.getResponseBody();
    }

    @Override
    protected void redirect(Request request) throws IOException {
        HttpExchange exchange = request.getImplementation(HttpExchange.class);
        StringBuilder builder = new StringBuilder(baseUrl).append(request.getQueryId())
                .append(request.getFormat().fileExtension(true));
        if (request.hasCompression()) {
            builder.append(request.getCompression().fileExtension(true));
        }

        final String newUrl = builder.toString();
        log.debug("Redirecting to %s", newUrl);

        final Headers respHeaders = exchange.getResponseHeaders();
        respHeaders.set(HEADER_LOCATION, newUrl);
        respond(request, HttpURLConnection.HTTP_MOVED_TEMP, newUrl);
    }

    @Override
    protected void respond(Request request, int code, String message) throws IOException {
        log.debug("Responding %d to %s: %s", code, request, message);
        HttpExchange exchange = request.getImplementation(HttpExchange.class);
        if (Checker.isNullOrEmpty(message)) {
            exchange.sendResponseHeaders(code, -1L);
        } else if (METHOD_HEAD.equals(exchange.getRequestMethod())) {
            if (code == HttpURLConnection.HTTP_OK) {
                setResponseHeaders(request);
            }
            // this is to eliminate the following warning:
            // sendResponseHeaders: being invoked with a content length for a HEAD request
            exchange.sendResponseHeaders(code, -1L);
        } else {
            byte[] bytes = message.getBytes(Constants.DEFAULT_CHARSET);
            exchange.sendResponseHeaders(code, bytes.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(bytes);
            }
        }
    }

    @Override
    protected void showConfig(Object implementation) throws IOException {
        HttpExchange exchange = check(implementation);

        Headers responseHeaders = exchange.getResponseHeaders();
        responseHeaders.set(HEADER_CONTENT_TYPE, Format.TXT.mimeType());
        exchange.sendResponseHeaders(200, 0L);
        try (OutputStream out = exchange.getResponseBody()) {
            writeConfig(out);
        }
    }

    @Override
    protected void showMetrics(Object implementation) throws IOException {
        HttpExchange exchange = check(implementation);

        Headers responseHeaders = exchange.getResponseHeaders();
        responseHeaders.set(HEADER_CONTENT_TYPE, Format.TXT.mimeType());
        exchange.sendResponseHeaders(200, 0L);
        try (OutputStream out = exchange.getResponseBody()) {
            writeMetrics(out);
        }
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        // id, query, format, compression, parameters
        final String method;
        final URI requestUri;
        final Map<String, String> headers;

        try {
            method = exchange.getRequestMethod();
            requestUri = exchange.getRequestURI();
            headers = new HashMap<>();
            String encodedToken = null;
            for (Entry<String, List<String>> entry : exchange.getRequestHeaders().entrySet()) {
                List<String> value = entry.getValue();
                String key = entry.getKey().toLowerCase(Locale.ROOT);
                if (HEADER_AUTHORIZATION.equals(key)) {
                    if (auth) {
                        encodedToken = value.get(value.size() - 1);
                        if (!Checker.isNullOrEmpty(encodedToken)
                                && encodedToken.startsWith(WebExecutor.AUTH_SCHEME_BEARER)) {
                            encodedToken = encodedToken.substring(WebExecutor.AUTH_SCHEME_BEARER.length());
                        }
                    }
                } else {
                    headers.put(key, value.get(value.size() - 1));
                }
            }

            final InetSocketAddress clientAddress = exchange.getRemoteAddress();

            log.debug("Handling request[%s, from=%s, url=%s, headers=%s]", method, clientAddress, requestUri, headers);

            dispatch(method, requestUri.getPath(), requestUri.getRawQuery(), exchange.getRemoteAddress(), encodedToken,
                    headers, exchange);
        } catch (Throwable e) { // NOSONAR
            log.error("Failed to handle request", e);
            try {
                exchange.sendResponseHeaders(500, -1L);
            } catch (IOException ex) {
                // ignore
            }
        } finally {
            try {
                exchange.close();
            } catch (Throwable e) { // NOSONAR
                // ignore
            }
        }
    }

    @Override
    public void start() {
        server.start();
        log.info("%s bridge server %s started at %s", Constants.PRODUCT_NAME, Version.current().toCompactString(),
                baseUrl);
    }

    @Override
    public void stop() {
        log.info("Shutting down server...");
        server.stop(0);
        log.info("Server is shutdown");

        super.stop();
    }
}
