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
import java.util.Properties;

import io.github.jdbcx.CommandLine;
import io.github.jdbcx.Utils;

public final class ScriptHelper {
    private static final ScriptHelper instance = new ScriptHelper();

    public static ScriptHelper getInstance() {
        return instance;
    }

    public String cli(String command, String... args) throws IOException {
        return new CommandLine(command, false, new Properties()).execute(args);
    }

    public String escapeSingleQuote(String str) {
        return Utils.escape(str, '\'');
    }

    public String escapeDoubleQuote(String str) {
        return Utils.escape(str, '"');
    }

    public String format(String template, Object... args) {
        return Utils.format(template, args);
    }

    // TODO additional methods to simplify execution of sql, prql, script, and web
    // request etc.

    private ScriptHelper() {
    }
}
