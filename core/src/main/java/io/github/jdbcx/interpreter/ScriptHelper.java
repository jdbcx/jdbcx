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
package io.github.jdbcx.interpreter;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;

import java.util.Map.Entry;
import java.util.concurrent.TimeoutException;

import io.github.jdbcx.Checker;
import io.github.jdbcx.Constants;
import io.github.jdbcx.Field;
import io.github.jdbcx.Option;
import io.github.jdbcx.Result;
import io.github.jdbcx.Row;
import io.github.jdbcx.Utils;
import io.github.jdbcx.executor.CommandLineExecutor;
import io.github.jdbcx.executor.Stream;
import io.github.jdbcx.security.SslContextProvider;

public final class ScriptHelper {
    private static final ScriptHelper instance = new ScriptHelper();

    static final String ENCODER_JSON = "json";
    static final String ENCODER_URL = "url";
    static final String ENCODER_BASE64 = "base64";
    static final String ENCODER_XML = "xml";

    static final String CDATA_START = "<![CDATA[";
    static final String CDATA_END = "]]>";

    static List<Field> toList(Object[] fields) {
        final int len;
        if (fields == null || (len = fields.length) == 0) {
            return Collections.emptyList();
        }

        List<Field> list = new ArrayList<>(len);
        for (int i = 0; i < len; i++) {
            Object f = fields[i];
            if (f instanceof Field) {
                list.add((Field) f);
            } else {
                list.add(Field.of(f == null ? "null_field_" + i : f.toString()));
            }
        }
        return list;
    }

    public static ScriptHelper getInstance() {
        return instance;
    }

    public String cli(Object obj, Object... more) throws IOException, TimeoutException {
        if (obj == null) {
            return Constants.EMPTY_STRING;
        }
        String[] args;
        if (more == null) {
            args = Constants.EMPTY_STRING_ARRAY;
        } else {
            int len = more.length;
            args = new String[len];
            for (int i = 0; i < len; i++) {
                Object o = more[i];
                args[i] = o != null ? o.toString() : Constants.EMPTY_STRING;
            }
        }
        return Stream
                .readAllAsString(
                        new CommandLineExecutor(obj.toString(), false, null, new Properties()).execute(null, null,
                                args));
    }

    public String escapeSingleQuote(Object obj) {
        if (obj == null) {
            return Constants.EMPTY_STRING;
        }
        return Utils.escape(obj.toString(), '\'');
    }

    public String escapeDoubleQuote(Object obj) {
        if (obj == null) {
            return Constants.EMPTY_STRING;
        }
        return Utils.escape(obj.toString(), '"');
    }

    public String encode(Object obj, String encoder) {
        final String encoded;
        switch (encoder) {
            case ENCODER_BASE64:
                encoded = Base64.getEncoder()
                        .encodeToString(obj != null ? obj.toString().getBytes(Constants.DEFAULT_CHARSET) : new byte[0]);
                break;
            case ENCODER_JSON:
                encoded = JsonHelper.encode(obj);
                break;
            case ENCODER_URL:
                try {
                    encoded = obj != null ? URLEncoder.encode(obj.toString(), Constants.DEFAULT_CHARSET.name())
                            : Constants.EMPTY_STRING;
                } catch (UnsupportedEncodingException e) {
                    throw new IllegalArgumentException(e); // not going to happen
                }
                break;
            case ENCODER_XML:
                encoded = new StringBuilder(CDATA_START).append(obj != null ? obj.toString() : Constants.EMPTY_STRING)
                        .append(CDATA_END).toString();
                break;
            default:
                encoded = String.valueOf(obj);
                break;
        }
        return encoded;
    }

    public String format(Object obj, Object... args) {
        if (obj == null) {
            return Constants.EMPTY_STRING;
        }
        return Utils.format(obj.toString(), args);
    }

    public String shell(Object... more) throws IOException, TimeoutException {
        return cli(Constants.IS_WINDOWS ? "cmd /c" : "/bin/sh -c", more);
    }

    public String read(Object obj) throws IOException {
        return read(obj, 0L, 0L, null, null);
    }

    public String read(Object obj, long connectTimeout, long readTimeout) throws IOException {
        return read(obj, connectTimeout, readTimeout, null, null);
    }

    public String read(Object obj, Object request) throws IOException {
        return read(obj, 0L, 0L, request, null);
    }

    public String read(Object obj, Object request, Map<?, ?> headers) throws IOException {
        return read(obj, 0L, 0L, request, headers);
    }

    public String read(Object obj, long connectTimeout, long readTimeout, Object request, Map<?, ?> headers)
            throws IOException {
        if (obj == null) {
            return Constants.EMPTY_STRING;
        }

        final URL url;
        if (obj instanceof URL) {
            url = (URL) obj;
        } else if (obj instanceof URI) {
            url = ((URI) obj).toURL();
        } else {
            String s = obj.toString();
            URL u = null;
            try {
                u = Utils.toURL(s);
            } catch (MalformedURLException e) {
                // ignore
            }
            url = u != null ? u : Paths.get(Utils.normalizePath(s)).toUri().toURL();
        }

        // TODO set proxy as needed
        final URLConnection conn = url.openConnection();
        try {
            if (headers != null) {
                for (Entry<?, ?> header : headers.entrySet()) {
                    Object key = header.getKey();
                    Object val = header.getValue();
                    if (key != null && val != null) {
                        conn.setRequestProperty(key.toString(), val.toString());
                    }
                }
            }
            if (connectTimeout > 0L) {
                conn.setConnectTimeout((int) connectTimeout);
            }
            if (readTimeout > 0L) {
                conn.setReadTimeout((int) readTimeout);
            }
            conn.setUseCaches(false);
            conn.setAllowUserInteraction(false);
            conn.setDoInput(true);
            if (conn instanceof HttpURLConnection) {
                HttpURLConnection httpConn = (HttpURLConnection) conn;
                if (httpConn instanceof HttpsURLConnection) {
                    HttpsURLConnection secureConn = (HttpsURLConnection) httpConn;
                    // TODO pass runtime properties
                    Properties props = new Properties();
                    SSLContext sslContext = SslContextProvider.getProvider()
                            .getSslContext(SSLContext.class, props).orElse(null);
                    // TODO custom hostname verifier
                    HostnameVerifier verifier = "strict".equalsIgnoreCase(Option.SSL_MODE.getValue(props))
                            ? HttpsURLConnection.getDefaultHostnameVerifier()
                            : (hostname, session) -> true; // NOSONAR
                    secureConn.setHostnameVerifier(verifier);
                    if (sslContext != null) {
                        secureConn.setSSLSocketFactory(sslContext.getSocketFactory());
                    }
                }

                String userInfo = url.getUserInfo();
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
                    httpConn.setRequestProperty("Authorization", "Basic "
                            .concat(Base64.getEncoder().encodeToString(userInfo.getBytes(Constants.DEFAULT_CHARSET))));
                }

                if (request != null) {
                    httpConn.setRequestMethod("POST");
                    httpConn.setInstanceFollowRedirects(true);
                    httpConn.setDoOutput(true);

                    try (OutputStream out = httpConn.getOutputStream()) {
                        Stream.writeAll(out, request);
                    }
                }

                if (httpConn.getResponseCode() != HttpURLConnection.HTTP_OK) {
                    String errorMsg = Stream.readAllAsString(httpConn.getErrorStream());
                    throw new IOException(
                            Checker.isNullOrEmpty(errorMsg) ? Utils.format("Response %d", httpConn.getResponseCode())
                                    : errorMsg);
                }
            }
            return Stream.readAllAsString(conn.getInputStream());
        } finally {
            try {
                conn.getInputStream().close();
            } catch (Exception e) {
                // ignore
            }
        }
    }

    public Result<?> table(Object[] fields) {
        return Result.of(toList(fields));
    }

    public Result<?> table(Object[] fields, Object[][] rows) {
        List<Field> fieldsList = toList(fields);
        if (fieldsList.isEmpty()) {
            return Result.of(fieldsList);
        }

        final List<Row> list;
        if (rows != null && rows.length > 0) {
            list = new ArrayList<>(rows.length);
            for (Object[] r : rows) {
                list.add(Row.of(fieldsList, r));
            }
        } else {
            list = Collections.emptyList();
        }
        return Result.of(fieldsList, list);
    }

    public Result<?> table(Object[] fields, Object row, Object... more) {
        List<Field> fieldsList = toList(fields);
        if (fieldsList.isEmpty()) {
            return Result.of(fieldsList);
        }

        final List<Row> rows;
        if (more != null && more.length > 0) {
            rows = new ArrayList<>(more.length + 1);
            rows.add(Row.of(fieldsList, row));
            for (Object r : more) {
                rows.add(Row.of(fieldsList, r));
            }
        } else if (row instanceof Iterable) {
            rows = new LinkedList<>();
            for (Object obj : (Iterable<?>) row) {
                rows.add(Row.of(fieldsList, obj));
            }
        } else {
            rows = Collections.singletonList(Row.of(fieldsList, row));
        }
        return Result.of(fieldsList, rows);
    }

    // TODO additional methods to simplify execution of sql, prql, script, and web
    // request etc.

    private ScriptHelper() {
    }
}
