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
import java.util.Map.Entry;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import io.github.jdbcx.Checker;
import io.github.jdbcx.ExpandedUrlClassLoader;
import io.github.jdbcx.Option;
import io.github.jdbcx.Utils;
import io.github.jdbcx.executor.jdbc.SqlExceptionUtils;

public abstract class JdbcConnectionManager {
    public static final Option OPTION_CACHE = Option
            .of(new String[] { "cache",
                    "Whether to load configuration into cache and use background thread to monitor changes" });
    public static final Option OPTION_ID = Option.of(new String[] { "id", "ID for looking up JDBC connection" });
    public static final Option OPTION_ALIAS = Option.of(new String[] { "alias", "Comma separated aliases" });
    public static final Option OPTION_URL = Option.of(new String[] { "url", "JDBC connection URL" });
    public static final Option OPTION_DRIVER = Option.of(new String[] { "driver", "JDBC driver class name" });
    public static final Option OPTION_CLASSPATH = Option.of(new String[] { "classpath", "Comma separated classpath" });
    public static final Option OPTION_MANAGED = Option
            .of(new String[] { "manage", "Whether all the connections are managed", "false", "true" });

    private static final JdbcConnectionManager instance = Utils.getService(JdbcConnectionManager.class);

    public static final JdbcConnectionManager getInstance() {
        return instance;
    }

    protected static final Driver getDriver(String url, ClassLoader loader) throws SQLException {
        if (loader == null) {
            loader = JdbcConnectionManager.class.getClassLoader();
        }

        Driver d = null;
        for (Driver newDriver : Utils.load(Driver.class, loader)) {
            if (newDriver.acceptsURL(url)) {
                d = newDriver;
                break;
            }
        }

        if (d == null) {
            d = DriverManager.getDriver(url);
        }
        return d;
    }

    public List<String> getAllConnectionIds() {
        return Collections.emptyList();
    }

    public abstract Connection getConnection(String id) throws SQLException;

    public final Connection getConnection(String url, Properties props, ClassLoader classLoader) throws SQLException {
        final String classpath = OPTION_CLASSPATH.getJdbcxValue(props);
        final Properties filtered = new Properties();
        for (Entry<Object, Object> entry : props.entrySet()) {
            String key = (String) entry.getKey();
            if (!key.startsWith(Option.PROPERTY_PREFIX)) {
                filtered.put(key, entry.getValue());
            }
        }
        if (Checker.isNullOrBlank(classpath)) {
            return getDriver(url, classLoader).connect(url, filtered);
        }

        final String driver = OPTION_DRIVER.getJdbcxValue(props);
        final ClassLoader loader = ExpandedUrlClassLoader.of(getClass(), classpath.split(","));
        if (Checker.isNullOrEmpty(driver)) {
            for (Driver d : Utils.load(Driver.class, loader)) {
                if (d.acceptsURL(url)) {
                    return d.connect(url, filtered);
                }
            }
            throw SqlExceptionUtils.clientError(
                    Utils.format("No suitable driver found in classpath [%s]. Please specify \"%s\" and try again.",
                            classpath, OPTION_DRIVER.getJdbcxName()));
        } else {
            Driver d = null;
            try {
                d = (Driver) loader.loadClass(driver).getConstructor().newInstance();
                return d.connect(url, filtered);
            } catch (Exception e) {
                throw SqlExceptionUtils.clientError(
                        d == null ? Utils.format("Failed to load driver [%s] due to: %s", driver, e.getMessage())
                                : Utils.format("Failed to connect to [%s] due to: %s", url, e.getMessage()),
                        e);
            }
        }
    }

    public abstract void reload(Properties props);
}
