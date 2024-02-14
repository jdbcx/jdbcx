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
import io.github.jdbcx.Constants;
import io.github.jdbcx.Format;
import io.github.jdbcx.Logger;
import io.github.jdbcx.LoggerFactory;
import io.github.jdbcx.Utils;
import io.github.jdbcx.executor.Stream;
import io.github.jdbcx.server.BridgeServer;
import io.github.jdbcx.server.QueryMode;
import io.github.jdbcx.server.Request;

public final class JdkHttpServer extends BridgeServer implements HttpHandler {
    private static final Logger log = LoggerFactory.getLogger(JdkHttpServer.class);

    private final HttpServer server;

    public JdkHttpServer() {
        this(System.getProperties());
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

    @Override
    protected Request create(String method, QueryMode mode, String qid, String query, String txid, Format format,
            Compression compress, Object userObject) throws IOException {
        if (mode != QueryMode.MUTATION && Checker.isNullOrBlank(query)) {
            HttpExchange exchange = (HttpExchange) userObject;
            query = Stream.readAllAsString(exchange.getRequestBody());
        }
        return super.create(method, mode, qid, query, txid, format, compress, userObject);
    }

    @Override
    protected void execute(Request request) throws IOException {
        HttpExchange exchange = request.getUserObject(HttpExchange.class);
        String query = request.getQuery();

        if (Checker.isNullOrEmpty(query)) {
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_NOT_FOUND, -1L);
        } else {
            Headers headers = exchange.getResponseHeaders();
            headers.set(HEADER_CONTENT_TYPE, request.getFormat().mimeType());
            if (request.hasCompression()) {
                headers.set(HEADER_CONTENT_ENCODING, request.getCompression().encoding());
            }

            exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, 0L);
            // TODO compressed output
            try (OutputStream out = exchange.getResponseBody()) {
                query(request, out);
            }
        }
    }

    @Override
    protected void redirect(Request request) throws IOException {
        HttpExchange exchange = request.getUserObject(HttpExchange.class);
        StringBuilder builder = new StringBuilder(baseUrl).append(request.getQueryId())
                .append(request.getFormat().fileExtension(true));
        if (request.getCompression() != Compression.NONE) {
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
        log.debug("Respond to %s: %d %s", request, code, message);
        HttpExchange exchange = request.getUserObject(HttpExchange.class);
        if (Checker.isNullOrEmpty(message)) {
            exchange.sendResponseHeaders(code, -1L);
        } else if (METHOD_HEAD.equals(exchange.getRequestMethod())) {
            if (code == HttpURLConnection.HTTP_OK) {
                Headers headers = exchange.getResponseHeaders();
                headers.set(HEADER_CONTENT_TYPE, request.getFormat().mimeType());
                if (request.hasCompression()) {
                    headers.set(HEADER_CONTENT_ENCODING, request.getCompression().encoding());
                }
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
    public void handle(HttpExchange exchange) throws IOException {
        // id, query, format, compression, parameters
        final String method;
        final URI requestUri;
        final Map<String, String> headers;

        try {
            method = exchange.getRequestMethod();
            requestUri = exchange.getRequestURI();
            headers = new HashMap<>();
            for (Entry<String, List<String>> entry : exchange.getRequestHeaders().entrySet()) {
                List<String> value = entry.getValue();
                headers.put(entry.getKey().toLowerCase(Locale.ROOT), value.get(value.size() - 1));
            }
            log.debug("Hanlde request[%s, url=%s, headers=%s]", method, requestUri, headers);

            dispatch(method, requestUri.getPath(), headers, Utils.toKeyValuePairs(requestUri.getRawQuery(), '&', true),
                    exchange);
        } catch (Exception e) {
            log.error("Failed to handle request", e);
            throw e;
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
        log.info("Server started: %s", baseUrl);
    }

    @Override
    public void stop() {
        log.info("Shutting down server...");
        server.stop(0);
        log.info("Server is shutdown");

        super.stop();
    }
}
