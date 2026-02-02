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
package io.github.jdbcx.interpreter;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import com.google.gson.JsonElement;

import io.github.jdbcx.Constants;
import io.github.jdbcx.Stream;
import io.github.jdbcx.Utils;
import io.github.jdbcx.VariableTag;

final class StringOperations {
    static String[] escape(String[] arr, char target, char escape) {
        for (int i = 0, len = arr.length; i < len; i++) {
            arr[i] = Utils.escape(arr[i], target, escape);
        }
        return arr;
    }

    static String[] extract(InputStream input, Charset charset, String path) {
        return extract(new InputStreamReader(input, charset != null ? charset : Constants.DEFAULT_CHARSET), path);
    }

    static String[] extract(Reader input, String path) {
        try (Reader reader = input) {
            JsonElement json = JsonHelper.parse(reader, path);
            final List<String> rows;
            if (json.isJsonNull()) {
                rows = Collections.singletonList(Constants.EMPTY_STRING);
            } else if (json.isJsonArray()) {
                List<String> list = new LinkedList<>();
                for (JsonElement e : json.getAsJsonArray()) {
                    if (e.isJsonNull()) {
                        list.add(Constants.EMPTY_STRING);
                    } else if (e.isJsonPrimitive()) {
                        list.add(e.getAsString());
                    } else {
                        list.add(JsonHelper.toJsonString(e));
                    }
                }
                rows = Collections.unmodifiableList(list);
            } else if (json.isJsonPrimitive()) {
                rows = Collections.singletonList(json.getAsString());
            } else {
                rows = Collections.singletonList(JsonHelper.toJsonString(json));
            }
            return rows.toArray(new String[0]);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to exact JSON elements", e);
        }
    }

    static String[] read(String file) {
        try {
            return new String[] { Stream.readAllAsString(new FileInputStream(file)) };
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read JSON from file", e);
        }
    }

    static String[] read(InputStream input) {
        try {
            return new String[] { Stream.readAllAsString(input) };
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read JSON from input stream", e);
        }
    }

    static String[] read(Reader input) {
        try {
            return new String[] { Stream.readAllAsString(input) };
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read JSON from reader", e);
        }
    }

    static String[] replace(String[] arr, VariableTag tag, Properties props, boolean useDefault) {
        final int len = arr.length;
        if (useDefault) {
            for (int i = 0; i < len; i++) {
                arr[i] = Utils.applyVariablesWithDefault(arr[i], tag, props);
            }
        } else {
            for (int i = 0; i < len; i++) {
                arr[i] = Utils.applyVariables(arr[i], tag, props);
            }
        }
        return arr;
    }

    static String[] split(String[] arr, String delimiter, boolean trim, boolean discardEmpty) {
        List<String> list = new LinkedList<>();
        for (String s : arr) {
            for (String splitted : Utils.split(s, delimiter)) {
                String str = trim ? splitted.trim() : splitted;
                if (discardEmpty && str.isEmpty()) {
                    continue;
                }
                list.add(str);
            }
        }
        return list.toArray(new String[0]);
    }

    static String[] trim(String[] arr, boolean discardEmpty) {
        final int len = arr.length;
        if (len > 0) {
            if (discardEmpty) {
                List<String> list = new ArrayList<>(len);
                for (int i = 0; i < len; i++) {
                    String s = arr[i];
                    if (s != null && !(s = s.trim()).isEmpty()) {
                        list.add(s);
                    }
                }
                arr = list.toArray(new String[0]);
            } else {
                for (int i = 0; i < len; i++) {
                    String s = arr[i];
                    arr[i] = s == null ? Constants.EMPTY_STRING : s.trim();
                }
            }
        }
        return arr;
    }

    private StringOperations() {
    }
}
