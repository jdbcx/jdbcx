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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
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
import io.github.jdbcx.QueryContext;
import io.github.jdbcx.Result;
import io.github.jdbcx.Row;
import io.github.jdbcx.Stream;
import io.github.jdbcx.Utils;
import io.github.jdbcx.executor.CommandLineExecutor;
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

    public String encodeFile(Object file, String encoder) {
        return encodeFile(file, encoder, false);
    }

    public String encodeFile(Object file, boolean withContentTypePrefix) {
        return encodeFile(file, ENCODER_BASE64, withContentTypePrefix);
    }

    public String encodeFile(Object file, String encoder, boolean withContentTypePrefix) {
        if (file == null) {
            return Constants.EMPTY_STRING;
        }

        final StringBuilder encoded;
        if (withContentTypePrefix) {
            encoded = new StringBuilder("data:image/");
        } else {
            encoded = new StringBuilder();
        }
        final byte[] bytes;
        final String fileName;
        try {
            if (file instanceof byte[]) {
                bytes = (byte[]) file;
                fileName = Constants.EMPTY_STRING;
            } else if (file instanceof File) {
                final File f = (File) file;
                bytes = Stream.readAllBytes(new FileInputStream(f));
                fileName = f.getName();
            } else if (file instanceof Path) {
                final File f = ((Path) file).toFile();
                bytes = Stream.readAllBytes(new FileInputStream(f));
                fileName = f.getName();
            } else if (file instanceof InputStream) {
                bytes = Stream.readAllBytes((InputStream) file);
                fileName = Constants.EMPTY_STRING;
            } else if (file instanceof URI) {
                final URL url = ((URI) file).toURL();
                bytes = Stream.readAllBytes(url.openStream());
                fileName = url.getPath();
            } else if (file instanceof URL) {
                final URL url = (URL) file;
                bytes = Stream.readAllBytes(url.openStream());
                fileName = url.getPath();
            } else {
                final String s = file.toString();
                URL u = null;
                try {
                    u = Utils.toURL(s);
                } catch (MalformedURLException e) {
                    // ignore
                }

                if (u != null && !Checker.isNullOrEmpty(u.getProtocol()) && !"file".equals(u.getProtocol())) {
                    bytes = Stream.readAllBytes(u.openStream());
                    fileName = u.getPath();
                } else {
                    fileName = Utils.normalizePath(file.toString());
                    bytes = Stream.readAllBytes(Utils.getFileInputStream(fileName));
                }
            }
        } catch (IOException e) {
            throw new IllegalStateException(Utils.format("Failed to access [%s] due to %s", file, e.getMessage()));
        }

        if (ENCODER_BASE64.equals(encoder) && withContentTypePrefix) {
            final String part = ";base64,";
            if (!fileName.isEmpty()) {
                int index = fileName.lastIndexOf(".");
                if (index > 0) {
                    return encoded.append(fileName.substring(index + 1)).append(part).append(encode(bytes, encoder))
                            .toString();
                }
            }

            final int len = bytes.length;
            final String extension;
            if (len >= 2 &&
                    bytes[0] == (byte) 0x42 &&
                    bytes[1] == (byte) 0x4D) {
                extension = "bmp";
            } else if (len >= 3 &&
                    bytes[0] == (byte) 0xFF &&
                    bytes[1] == (byte) 0xD8 &&
                    bytes[2] == (byte) 0xFF) {
                extension = "jpeg";
            } else if (len >= 4 &&
                    bytes[0] == (byte) 0x00 &&
                    bytes[1] == (byte) 0x00 &&
                    bytes[2] == (byte) 0x01 &&
                    bytes[3] == (byte) 0x00) {
                extension = "ico";
            } else if (len >= 8 &&
                    bytes[0] == (byte) 0x89 &&
                    bytes[1] == (byte) 0x50 &&
                    bytes[2] == (byte) 0x4E &&
                    bytes[3] == (byte) 0x47 &&
                    bytes[4] == (byte) 0x0D &&
                    bytes[5] == (byte) 0x0A &&
                    bytes[6] == (byte) 0x1A &&
                    bytes[7] == (byte) 0x0A) {
                extension = "png";
            } else if (len >= 6 &&
                    bytes[0] == (byte) 0x47 &&
                    bytes[1] == (byte) 0x49 &&
                    bytes[2] == (byte) 0x46 &&
                    bytes[3] == (byte) 0x38 &&
                    (bytes[4] == (byte) 0x37 || bytes[4] == (byte) 0x39) &&
                    bytes[5] == (byte) 0x61) {
                extension = "gif";
            } else if (len >= 12 &&
                    bytes[0] == (byte) 0x52 &&
                    bytes[1] == (byte) 0x49 &&
                    bytes[2] == (byte) 0x46 &&
                    bytes[3] == (byte) 0x46 &&
                    bytes[8] == (byte) 0x57 &&
                    bytes[9] == (byte) 0x45 &&
                    bytes[10] == (byte) 0x42 &&
                    bytes[11] == (byte) 0x50) {
                extension = "webp";
            } else {
                throw new IllegalArgumentException(
                        Utils.format("Unable to identify file type after reading %d bytes", len));
            }
            return encoded.append(extension).append(part).append(encode(bytes, encoder)).toString();
        }
        return encode(bytes, encoder);
    }

    public String encode(Object obj, String encoder) {
        if (encoder == null) {
            encoder = Constants.EMPTY_STRING;
        }
        final String encoded;
        switch (encoder) {
            case ENCODER_BASE64:
                encoded = obj == null ? Constants.EMPTY_STRING
                        : (obj instanceof byte[] ? Utils.toBase64((byte[]) obj) : Utils.toBase64(obj.toString()));
                break;
            case ENCODER_JSON:
                encoded = JsonHelper.encode(obj);
                break;
            case ENCODER_URL:
                try {
                    encoded = obj == null ? Constants.EMPTY_STRING
                            : URLEncoder
                                    .encode(obj instanceof byte[] ? new String((byte[]) obj, Constants.DEFAULT_CHARSET)
                                            : obj.toString(), Constants.DEFAULT_CHARSET.name());
                } catch (UnsupportedEncodingException e) {
                    throw new IllegalArgumentException(e); // not going to happen
                }
                break;
            case ENCODER_XML:
                encoded = obj == null ? Constants.EMPTY_STRING
                        : new StringBuilder(CDATA_START)
                                .append(obj instanceof byte[] ? new String((byte[]) obj, Constants.DEFAULT_CHARSET)
                                        : obj.toString())
                                .append(CDATA_END).toString();
                break;
            default:
                encoded = obj instanceof byte[] ? new String((byte[]) obj, Constants.DEFAULT_CHARSET)
                        : String.valueOf(obj);
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
                    httpConn.setRequestProperty("Authorization", "Basic ".concat(Utils.toBase64(userInfo)));
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

    public String var(Object name) {
        return getVariable(name);
    }

    public String var(Object name, Object defaultValue) {
        return getVariable(name, defaultValue);
    }

    public String var(Object scope, Object name, Object defaultValue) {
        return getVariable(scope, name, defaultValue);
    }

    public String getVariable(Object name) {
        return getVariable(name, Constants.EMPTY_STRING);
    }

    public String getVariable(Object name, Object defaultValue) {
        if (name == null) {
            return Constants.EMPTY_STRING;
        }

        return QueryContext.getCurrentContext().getVariable(name.toString(),
                defaultValue != null ? defaultValue.toString() : null);
    }

    public String getVariable(Object scope, Object name, Object defaultValue) {
        if (scope == null || name == null) {
            return Constants.EMPTY_STRING;
        }
        return QueryContext.getCurrentContext().getVariableInScope(scope.toString(), name.toString(),
                defaultValue != null ? defaultValue.toString() : null);
    }

    public Object setVariable(Object name, Object value) {
        if (name == null) {
            return null;
        }

        return QueryContext.getCurrentContext().setVariable(name.toString(),
                value != null ? value.toString() : null);
    }

    public Object setVariable(Object scope, Object name, Object value) {
        if (scope == null || name == null) {
            return null;
        }

        return QueryContext.getCurrentContext().setVariableInScope(scope.toString(), name.toString(),
                value != null ? value.toString() : null);
    }

    // TODO additional methods to simplify execution of sql, prql, script, and web
    // request etc.

    private ScriptHelper() {
    }
}
