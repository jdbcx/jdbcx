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
package io.github.jdbcx;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

final class InvalidDriver implements Driver {
    private final Properties props;

    InvalidDriver(Properties props) {
        this.props = props;
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        final String customClasspath = Option.CUSTOM_CLASSPATH.getValue(this.props);
        final String errorMessage;
        if (Checker.isNullOrEmpty(customClasspath)) {
            errorMessage = Utils.format("Connection not established due to missing driver. "
                    + "Please set \"%s\" property to driver directory path.",
                    Option.CUSTOM_CLASSPATH.getSystemProperty(WrappedDriver.PROPERTY_PREFIX));
        } else {
            errorMessage = Utils.format("Connection failed due to missing driver in directory \"%s\". "
                    + "Please update \"%s\" property to the driver directory path.", customClasspath,
                    Option.CUSTOM_CLASSPATH.getSystemProperty(WrappedDriver.PROPERTY_PREFIX));
        }
        throw new SQLException(errorMessage);
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return false;
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return 0;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return null;
    }
}
