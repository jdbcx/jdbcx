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
import java.net.URI;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

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

    public String load(Object obj) throws IOException {
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
            if (s.indexOf("://") != -1) {
                url = new URL(s);
            } else {
                Path p = Paths.get(Utils.normalizePath(s));
                url = p.toUri().toURL();
            }
        }

        return Utils.readAllAsString(url.openStream());
    }

    // TODO additional methods to simplify execution of sql, prql, script, and web
    // request etc.

    private ScriptHelper() {
    }
}
