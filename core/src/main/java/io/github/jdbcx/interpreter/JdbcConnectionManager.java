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

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import io.github.jdbcx.Checker;
import io.github.jdbcx.ExpandedUrlClassLoader;
import io.github.jdbcx.Option;
import io.github.jdbcx.Utils;

public abstract class JdbcConnectionManager {
    public static final Option OPTION_CACHE = Option
            .of(new String[] { "cache",
                    "Whether to load configuration into cache and use background thread to monitor changes" });
    public static final Option OPTION_ID = Option
            .of(new String[] { "id", "ID for looking up JDBC connection" });
    public static final Option OPTION_URL = Option.of(new String[] { "url", "JDBC connection URL" });
    public static final Option OPTION_DRIVER = Option.of(new String[] { "driver", "JDBC driver class name" });
    public static final Option OPTION_CLASSPATH = Option.of(new String[] { "classpath", "Comma separated classpath" });

    private static final JdbcConnectionManager instance = Utils.getService(JdbcConnectionManager.class);

    public static final JdbcConnectionManager getInstance() {
        return instance;
    }

    public abstract Connection getConnection(String id) throws SQLException;

    public final Connection getConnection(String url, Properties props) throws SQLException {
        final String classpath = OPTION_CLASSPATH.getJdbcxValue(props);
        if (Checker.isNullOrBlank(classpath)) {
            return DriverManager.getConnection(url, props);
        }

        final String driver = OPTION_DRIVER.getJdbcxValue(props);
        final ClassLoader loader = new ExpandedUrlClassLoader(getClass(), classpath.split(","));
        if (Checker.isNullOrEmpty(driver)) {
            for (Driver d : Utils.load(Driver.class, loader)) {
                if (d.acceptsURL(url)) {
                    return d.connect(url, props);
                }
            }
            throw new SQLException(
                    Utils.format("No suitable driver found in classpath [%s]. Please specify \"%s\" and try again.",
                            classpath, OPTION_DRIVER.getJdbcxName()));
        } else {
            try {
                Driver d = (Driver) loader.loadClass(driver).getConstructor().newInstance();
                return d.connect(url, props);
            } catch (Exception e) {
                throw new SQLException(e);
            }
        }
    }

    public abstract void reload(Properties props);
}
