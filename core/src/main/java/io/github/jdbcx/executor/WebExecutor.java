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
package io.github.jdbcx.executor;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.Base64;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;

import io.github.jdbcx.Checker;
import io.github.jdbcx.Constants;
import io.github.jdbcx.Logger;
import io.github.jdbcx.LoggerFactory;
import io.github.jdbcx.Option;
import io.github.jdbcx.Utils;
import io.github.jdbcx.security.SslContextProvider;

public class WebExecutor extends AbstractExecutor {
    private static final Logger log = LoggerFactory.getLogger(WebExecutor.class);

    static final String PROTOCOL_DELIMITER = "://";
    static final Proxy.Type DEFAULT_PROXY_TYPE = Proxy.Type.HTTP;
    static final String DEFAULT_PROXY_HOST = "localhost";
    static final int DEFAULT_PROXY_PORT = 1080;

    public static final Option OPTION_CONNECT_TIMEOUT = Option
            .of(new String[] { "connect.timeout",
                    "Connect timeout in milliseconds, a negative number or zero disables timeout", "5000" });
    public static final Option OPTION_PROXY = Option.of(new String[] { "proxy", "Proxy server address" });
    public static final Option OPTION_SOCKET_TIMEOUT = Option
            .of(new String[] { "socket.timeout",
                    "Socket timeout in milliseconds, a negative number or zero disables timeout", "30000" });

    static void checkResponse(HttpURLConnection conn) throws IOException {
        final int respCode = conn.getResponseCode();
        if (respCode != HttpURLConnection.HTTP_OK) {
            log.debug("Got response code %d, trying to figure out why", respCode);

            String errorMsg = Stream.readAllAsString(conn.getErrorStream());
            if (Checker.isNullOrEmpty(errorMsg)) {
                log.debug("No clue in error stream, trying to get error message from input stream");
                errorMsg = Stream.readAllAsString(conn.getInputStream());
            }
            throw new IOException(
                    Checker.isNullOrEmpty(errorMsg) ? Utils.format("Response %d", conn.getResponseCode())
                            : errorMsg);
        }
    }

    static void setBasicAuth(HttpURLConnection conn, String userInfo) {
        if (!Checker.isNullOrEmpty(userInfo)) {
            String user = Constants.EMPTY_STRING;
            String passwd = Constants.EMPTY_STRING;
            for (int i = 0, len = userInfo.length(); i < len; i++) {
                if (userInfo.charAt(i) == ':') {
                    user = Utils.decode(userInfo.substring(0, i));
                    passwd = Utils.decode(userInfo.substring(i + 1));
                    break;
                }
            }
            if (user.isEmpty() && passwd.isEmpty()) {
                user = Utils.decode(userInfo);
            }
            userInfo = new StringBuilder(user).append(':').append(passwd).toString();
            conn.setRequestProperty("Authorization",
                    "Basic ".concat(Base64.getEncoder().encodeToString(userInfo.getBytes(Constants.DEFAULT_CHARSET))));
        }
    }

    private final String defaultConnectTimeout;
    private final String defaultSocketTimeout;
    private final String defaultProxy;

    protected Proxy getProxy(Properties config) throws UnknownHostException {
        final Proxy p;
        String proxy = OPTION_PROXY.getValue(config, defaultProxy);
        if (Checker.isNullOrBlank(proxy)) {
            p = Proxy.NO_PROXY;
        } else {
            if (proxy.charAt(0) == ':') {
                proxy = DEFAULT_PROXY_HOST.concat(proxy);
            }
            if (proxy.indexOf(PROTOCOL_DELIMITER) == -1) {
                proxy = new StringBuilder(DEFAULT_PROXY_TYPE.name().toLowerCase(Locale.ROOT)).append(PROTOCOL_DELIMITER)
                        .append(proxy).toString();
            }
            URI uri;
            try {
                uri = new URI(proxy);
            } catch (URISyntaxException e) {
                throw new UnknownHostException(Utils.format("Invalid proxy [%s]", proxy));
            }
            String protocol = uri.getScheme();
            if (Checker.isNullOrEmpty(protocol)) {
                protocol = DEFAULT_PROXY_TYPE.name().toLowerCase(Locale.ROOT);
            } else {
                protocol = protocol.toLowerCase(Locale.ROOT);
            }
            String path = uri.getPath();
            String host = uri.getHost();
            if (Checker.isNullOrEmpty(host)) {
                host = Checker.isNullOrEmpty(path) ? DEFAULT_PROXY_HOST : path;
            }
            int port = uri.getPort();
            if (port <= 0) {
                port = 1080;
            }
            if (protocol.startsWith("http")) {
                p = new Proxy(Proxy.Type.HTTP, InetSocketAddress.createUnresolved(host, port));
            } else if (protocol.startsWith("sock")) {
                p = new Proxy(Proxy.Type.SOCKS, InetSocketAddress.createUnresolved(host, port));
            } else {
                throw new UnknownHostException(Utils.format("Unsupported proxy [%s]", proxy));
            }
        }
        if (Checker.isNullOrEmpty(proxy)) {
            log.debug("Use proxy [%s](no config specified)", p);
        } else {
            log.debug("Use proxy [%s](config=%s)", p, proxy);
        }
        return p;
    }

    protected HttpURLConnection openConnection(URL url, Properties config, Map<?, ?> headers) throws IOException {
        final int connectTimeout = Integer.parseInt(OPTION_CONNECT_TIMEOUT.getValue(config, defaultConnectTimeout));
        final int socketTimeout = Integer.parseInt(OPTION_SOCKET_TIMEOUT.getValue(config, defaultSocketTimeout));

        final HttpURLConnection conn = (HttpURLConnection) url.openConnection(getProxy(config));

        if (headers != null) {
            for (Entry<?, ?> header : headers.entrySet()) {
                Object key = header.getKey();
                Object val = header.getValue();
                if (key != null && val != null) {
                    String str = Utils.applyVariables(val.toString(), config);
                    if (!Checker.isNullOrBlank(str)) {
                        conn.setRequestProperty(key.toString(), val.toString());
                    }
                }
            }
        }

        if (connectTimeout > 0) {
            conn.setConnectTimeout(connectTimeout);
        }
        if (socketTimeout > 0) {
            conn.setReadTimeout(socketTimeout);
        }
        conn.setAllowUserInteraction(false);
        conn.setInstanceFollowRedirects(true);
        conn.setUseCaches(false);
        conn.setDoInput(true);
        if (conn instanceof HttpsURLConnection) {
            HttpsURLConnection secureConn = (HttpsURLConnection) conn;
            SSLContext sslContext = SslContextProvider.getProvider().getSslContext(SSLContext.class, config)
                    .orElse(null);
            HostnameVerifier verifier = "strict".equalsIgnoreCase(Option.SSL_MODE.getValue(config))
                    ? HttpsURLConnection.getDefaultHostnameVerifier()
                    : (hostname, session) -> true; // NOSONAR
            secureConn.setHostnameVerifier(verifier);
            if (sslContext != null) {
                secureConn.setSSLSocketFactory(sslContext.getSocketFactory());
            }
        }

        setBasicAuth(conn, url.getUserInfo());
        return conn;
    }

    public WebExecutor(Properties props) {
        super(props);

        this.defaultConnectTimeout = OPTION_CONNECT_TIMEOUT.getValue(props);
        this.defaultSocketTimeout = OPTION_SOCKET_TIMEOUT.getValue(props, String.valueOf(defaultTimeout));
        this.defaultProxy = OPTION_PROXY.getValue(props);
    }

    public int getDefaultConnectTimeout() {
        return Integer.parseInt(defaultConnectTimeout);
    }

    public int getDefaultSocketTimeout() {
        return Integer.parseInt(defaultSocketTimeout);
    }

    public String getDefaultProxy() {
        return defaultProxy;
    }

    public InputStream get(URL url, Properties config, Map<?, ?> headers) throws IOException {
        HttpURLConnection conn = null;
        try {
            conn = openConnection(url, config, headers);
            checkResponse(conn);
        } catch (Exception e) {
            if (conn != null) {
                try {
                    conn.getInputStream().close();
                } catch (Exception exp) {
                    // ignore
                }
            }
            throw e;
        }
        return conn.getInputStream();
    }

    public InputStream post(URL url, Object request, Properties config, Map<?, ?> headers) throws IOException {
        HttpURLConnection conn = null;
        try {
            conn = openConnection(url, config, headers);
            conn.setRequestMethod("POST");
            if (request != null) {
                conn.setDoOutput(true);
                try (OutputStream out = conn.getOutputStream()) {
                    Stream.writeAll(out, request);
                }
            }
            checkResponse(conn);
        } catch (Exception e) {
            if (conn != null) {
                try {
                    conn.getInputStream().close();
                } catch (Exception exp) {
                    // ignore
                }
            }
            throw e;
        }
        return conn.getInputStream();
    }
}
