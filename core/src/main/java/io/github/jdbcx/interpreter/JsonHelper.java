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
package io.github.jdbcx.interpreter;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import com.google.gson.Gson;
import com.google.gson.JsonElement;

import io.burt.jmespath.Expression;
import io.burt.jmespath.JmesPath;
import io.burt.jmespath.gson.GsonRuntime;
import io.github.jdbcx.Cache;
import io.github.jdbcx.Checker;
import io.github.jdbcx.Constants;
import io.github.jdbcx.Row;
import io.github.jdbcx.Utils;

public final class JsonHelper {
    private static final Gson gson = new Gson();
    private static final JmesPath<JsonElement> jmesPath = new GsonRuntime();

    private static final Cache<String, Expression<JsonElement>> cache = Cache.create(100, 0L, jmesPath::compile);

    public static List<Row> extract(InputStream input, Charset charset, String path, String delimiter, boolean trim)
            throws IOException {
        return extract(new InputStreamReader(input, charset != null ? charset : Constants.DEFAULT_CHARSET), path,
                delimiter, trim);
    }

    public static List<Row> extract(Reader input, String path, String delimiter, boolean trim) throws IOException {
        final Expression<JsonElement> compiledPath = cache.get(path);
        try (Reader reader = input) {
            JsonElement json = compiledPath.search(gson.fromJson(reader, JsonElement.class));
            final List<Row> rows;
            if (json.isJsonNull()) {
                rows = Collections.singletonList(Row.of(Constants.EMPTY_STRING));
            } else if (json.isJsonArray()) {
                List<Row> list = new LinkedList<>();
                for (JsonElement e : json.getAsJsonArray()) {
                    if (e.isJsonNull()) {
                        list.add(Row.of(Constants.EMPTY_STRING));
                    } else if (e.isJsonPrimitive()) {
                        String s = e.getAsString();
                        if (trim) {
                            s = s.trim();
                            if (s.isEmpty()) {
                                continue;
                            }
                        }
                        list.add(Row.of(s));
                    } else {
                        list.add(Row.of(gson.toJson(e)));
                    }
                }
                rows = Collections.unmodifiableList(list);
            } else if (json.isJsonPrimitive()) {
                String s = json.getAsString();
                if (Checker.isNullOrEmpty(delimiter)) {
                    rows = Collections.singletonList(Row.of(trim ? s.trim() : s));
                } else {
                    List<Row> list = new LinkedList<>();
                    for (String splitted : Utils.split(s, delimiter)) {
                        if (trim) {
                            splitted = splitted.trim();
                            if (splitted.isEmpty()) {
                                continue;
                            }
                        }
                        list.add(Row.of(splitted));
                    }
                    rows = Collections.unmodifiableList(list);
                }
            } else {
                rows = Collections.singletonList(Row.of(gson.toJson(json)));
            }
            return rows;
        }
    }

    public static String extract(String json, String path) {
        final Expression<JsonElement> compiledPath = cache.get(path);
        JsonElement j = compiledPath.search(gson.fromJson(json, JsonElement.class));
        return j.getAsString();
    }

    public static String encode(Object str) {
        return gson.toJson(str);
    }

    private JsonHelper() {
    }
}
