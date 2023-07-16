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
package io.github.jdbcx.script;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;

import io.github.jdbcx.CommandLine;
import io.github.jdbcx.Constants;
import io.github.jdbcx.Utils;

public final class ScriptHelper {
    private static final ScriptHelper instance = new ScriptHelper();

    public static ScriptHelper getInstance() {
        return instance;
    }

    public String cli(Object obj, Object... more) throws IOException {
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
        return new CommandLine(obj.toString(), false, new Properties()).execute(args);
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

    public String format(Object obj, Object... args) {
        if (obj == null) {
            return Constants.EMPTY_STRING;
        }
        return Utils.format(obj.toString(), args);
    }

    public String shell(Object... more) throws IOException {
        return cli(Constants.IS_WINDOWS ? "cmd /c" : "sh -c", more);
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
                u = new URL(s);
            } catch (MalformedURLException e) {
                // ignore
            }
            url = u != null ? u : Paths.get(Utils.normalizePath(s)).toUri().toURL();
        }

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
            if (request != null && conn instanceof HttpURLConnection) {
                HttpURLConnection httpConn = (HttpURLConnection) conn;
                httpConn.setRequestMethod("POST");
                httpConn.setInstanceFollowRedirects(true);
                conn.setDoOutput(true);

                try (OutputStream out = conn.getOutputStream()) {
                    Utils.writeAll(out, request);
                }

                if (httpConn.getResponseCode() != HttpURLConnection.HTTP_OK) {
                    throw new IOException("null");
                }
            }
            return Utils.readAllAsString(conn.getInputStream());
        } finally {
            try {
                conn.getInputStream().close();
            } catch (Exception e) {
                // ignore
            }
        }
    }

    // TODO additional methods to simplify execution of sql, prql, script, and web
    // request etc.

    private ScriptHelper() {
    }
}
