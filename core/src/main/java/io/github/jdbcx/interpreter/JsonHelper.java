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

import com.google.gson.Gson;
import com.google.gson.JsonElement;

import io.burt.jmespath.Expression;
import io.burt.jmespath.JmesPath;
import io.burt.jmespath.gson.GsonRuntime;
import io.github.jdbcx.Cache;
import io.github.jdbcx.Constants;

public final class JsonHelper {
    private static final Gson gson = new Gson();
    private static final JmesPath<JsonElement> jmesPath = new GsonRuntime();

    private static final Cache<String, Expression<JsonElement>> cache = Cache.create(100, 0L, jmesPath::compile);

    public static String extract(InputStream input, Charset charset, String path) throws IOException {
        return extract(new InputStreamReader(input, charset != null ? charset : Constants.DEFAULT_CHARSET), path);
    }

    public static String extract(Reader input, String path) throws IOException {
        final Expression<JsonElement> compiledPath = cache.get(path);
        try (Reader reader = input) {
            JsonElement json = compiledPath.search(gson.fromJson(reader, JsonElement.class));
            return json.getAsString();
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
