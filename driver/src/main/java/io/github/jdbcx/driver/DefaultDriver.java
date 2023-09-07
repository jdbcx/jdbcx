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
package io.github.jdbcx.driver;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverAction;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

import io.github.jdbcx.Utils;
import io.github.jdbcx.driver.impl.DefaultConnection;

final class DefaultDriver implements Driver, DriverAction {
    static final java.util.logging.Logger parentLogger = java.util.logging.Logger.getLogger("io.github.jdbcx.driver");

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return Utils.startsWith(url, ConnectionManager.JDBCX_PREFIX, true);
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        return new DefaultConnection(driverInfo.getExtensions(), driverInfo.extension, driverInfo.actualUrl,
                driverInfo.extensionProps, driverInfo.normalizedInfo, driverInfo.mergedInfo);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return driverInfo.getExtensionInfo();
    }

    @Override
    public int getMajorVersion() {
        return ConnectionManager.DRIVER_VERSION.getMajorVersion();
    }

    @Override
    public int getMinorVersion() {
        return ConnectionManager.DRIVER_VERSION.getMinorVersion();
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return parentLogger;
    }

    private final DriverInfo driverInfo;

    DefaultDriver(DriverInfo driverInfo) {
        this.driverInfo = driverInfo;
    }

    @Override
    public void deregister() {
        driverInfo.close();
    }
}