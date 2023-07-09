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
package io.github.jdbcx.prql;

import java.io.ByteArrayOutputStream;
import java.sql.SQLException;
import java.util.Locale;
import java.util.Properties;

import io.github.jdbcx.Checker;
import io.github.jdbcx.CommandLine;
import io.github.jdbcx.ConnectionListener;
import io.github.jdbcx.Constants;
import io.github.jdbcx.Option;
import io.github.jdbcx.SqlExceptionUtils;

final class PrqlConnectionListener implements ConnectionListener {
    static final String DEFAULT_COMMAND = "prqlc";
    static final boolean DEFAULT_FALLBACK = true;

    public static final Option OPTION_COMPILE_ERROR = Option
            .of(new String[] { "compile.error", "The approach to handle compile error", Option.ERROR_HANDLING_IGNORE,
                    Option.ERROR_HANDLING_THROW, Option.ERROR_HANDLING_WARN });
    static final Option OPTION_COMPILE_TARGET = Option.of(new String[] { "compile.target",
            "PRQL compile target without \"sql.\" prefix, empty string is same as \"any\"" });

    private final CommandLine cli;

    private final String compileTarget;
    private final String errorHandling;

    PrqlConnectionListener(Properties config) {
        this.errorHandling = OPTION_COMPILE_ERROR.getValue(config);
        String target = OPTION_COMPILE_TARGET.getValue(config).toLowerCase(Locale.ROOT);
        this.compileTarget = Checker.isNullOrEmpty(target) || "any".equals(target) ? Constants.EMPTY_STRING
                : "sql.".concat(target);
        this.cli = new CommandLine(DEFAULT_COMMAND, config);
    }

    @Override
    public String onQuery(String query) throws SQLException {
        try {
            final String sql;
            ByteArrayOutputStream out = new ByteArrayOutputStream(2048);
            String[] args = Checker.isNullOrEmpty(compileTarget) ? new String[] { "compile" }
                    : new String[] { "compile", "-t", compileTarget };
            int exitCode = cli.execute(cli.getDefaultTimeout(), cli.getDefaultWorkDirectory(), query,
                    cli.getDefaultInputCharset(), out, cli.getDefaultOutputCharset(), args);
            if (exitCode != 0) {
                if (Option.ERROR_HANDLING_IGNORE.equals(errorHandling)) {
                    sql = query;
                } else {
                    throw SqlExceptionUtils.clientError(new String(out.toByteArray(), cli.getDefaultOutputCharset()));
                }
            } else {
                sql = new String(out.toByteArray(), cli.getDefaultOutputCharset());
            }
            return sql;
        } catch (Exception e) {
            if (Option.ERROR_HANDLING_IGNORE.equals(errorHandling)) {
                return query;
            } else if (Option.ERROR_HANDLING_WARN.equals(errorHandling)) {
                throw SqlExceptionUtils.clientWarning(e);
            } else {
                throw SqlExceptionUtils.clientError(e);
            }
        }
    }
}
