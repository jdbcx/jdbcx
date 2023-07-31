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
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This class is essentially a wrapper of the {@link Driver} interface. It
 * accepts a connection string that starts with {@code jdbcx:}, followed by an
 * optional name of a {@code DriverExtension}, which defines the default
 * behaviors of the driver. For example, in the JDBC driver, we used to use
 * {@code jdbc:mysql://localhost/test} to connect to MySQL. However, now it has
 * changed to {@code jdbcx:mysql://localhost/test} or
 * {@code jdbcx:prql:mysql://localhost/test}, if you prefer to use PRQL.
 */
public abstract class AbstractDriver implements Driver, DriverAction {
    private final AtomicReference<DriverInfo> cache = new AtomicReference<>();

    protected DriverInfo getDriverInfo(String url, Properties props) {
        DriverInfo driverInfo = cache.get();
        if (driverInfo == null || !driverInfo.isFor(url, props)) {
            final DriverInfo newInfo = new DriverInfo(url, props);
            if (cache.compareAndSet(driverInfo, newInfo)) {
                driverInfo = newInfo;
            } else {
                driverInfo = cache.get();
            }
        }
        return driverInfo;
    }

    protected Driver getActualDriver() {
        final DriverInfo driverInfo = cache.get();
        return driverInfo != null ? driverInfo.getActualDriver() : null;
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return url != null && url.length() >= DriverInfo.JDBCX_PREFIX.length()
                && url.substring(0, DriverInfo.JDBCX_PREFIX.length()).equalsIgnoreCase(DriverInfo.JDBCX_PREFIX);
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        final DriverInfo driverInfo = getDriverInfo(url, info);
        return acceptsURL(url) ? new WrappedConnection(driverInfo)
                : driverInfo.getActualDriver().connect(driverInfo.actualUrl, driverInfo.normalizedInfo);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        final DriverInfo driverInfo = getDriverInfo(url, info);

        DriverPropertyInfo[] driverPropInfo = driverInfo.getActualDriver()
                .getPropertyInfo(driverInfo.actualUrl, driverInfo.normalizedInfo);
        if (driverPropInfo == null) {
            driverPropInfo = new DriverPropertyInfo[0];
        }
        int index = driverPropInfo.length;
        DriverPropertyInfo[] extInfo = driverInfo.getExtensionInfo();

        DriverPropertyInfo[] merged = new DriverPropertyInfo[index + extInfo.length];
        System.arraycopy(driverPropInfo, 0, merged, 0, index);
        System.arraycopy(extInfo, 0, merged, index, extInfo.length);
        return merged;
    }

    @Override
    public void deregister() {
        final DriverInfo driverInfo = cache.get();
        cache.set(null);
        if (driverInfo != null) {
            driverInfo.close();
        }
    }
}