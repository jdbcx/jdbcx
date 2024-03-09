/*
 * Copyright 2022-2024, Zhichun Wu
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
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import io.github.jdbcx.Checker;
import io.github.jdbcx.Constants;
import io.github.jdbcx.DriverExtension;
import io.github.jdbcx.Utils;
import io.github.jdbcx.driver.impl.DefaultConnection;

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
        return driverInfo != null ? driverInfo.driver : null;
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return Utils.startsWith(url, ConnectionManager.JDBCX_PREFIX, true);
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        final DriverInfo driverInfo = getDriverInfo(url, info);
        if (driverInfo.driver instanceof InvalidDriver) {
            String tailored = driverInfo.normalizedUrl;
            if (Utils.startsWith(tailored, ConnectionManager.JDBCX_PREFIX, true)) {
                tailored = tailored.substring(ConnectionManager.JDBCX_PREFIX.length());
            }
            int index = tailored.indexOf(':');
            String extName;
            if (index == -1) {
                extName = tailored;
                tailored = Constants.EMPTY_STRING;
            } else {
                extName = tailored.substring(0, index);
                tailored = tailored.substring(index + 1);
            }
            Map<String, DriverExtension> extensions = driverInfo.getExtensions();
            DriverExtension ext = extensions.get(extName);
            if (ext != null) {
                Properties extProps = ext == driverInfo.extension ? driverInfo.extensionProps
                        : DriverExtension.extractProperties(ext, info);
                Connection conn = null;
                if (!Checker.isNullOrEmpty(tailored)) {
                    conn = ext.getConnection(driverInfo.configManager, tailored, extProps);
                }
                return conn != null ? new WrappedConnection(driverInfo, conn, url)
                        : new DefaultConnection(driverInfo.configManager, extensions, ext, url, extProps,
                                driverInfo.normalizedInfo, driverInfo.mergedInfo);
            }
        }
        return acceptsURL(url) ? new WrappedConnection(driverInfo)
                : driverInfo.driver.connect(driverInfo.actualUrl, driverInfo.normalizedInfo);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        final DriverInfo driverInfo = getDriverInfo(url, info);

        DriverPropertyInfo[] driverPropInfo = driverInfo.driver.getPropertyInfo(driverInfo.actualUrl,
                driverInfo.normalizedInfo);
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
