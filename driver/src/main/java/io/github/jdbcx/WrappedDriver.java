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

import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

import io.github.jdbcx.driver.AbstractDriver;

public final class WrappedDriver extends AbstractDriver {
    private static final Logger log = LoggerFactory.getLogger(WrappedDriver.class);

    static {
        log.debug("Registering %s", WrappedDriver.class.getName());
        try {
            DriverManager.registerDriver(new WrappedDriver());
        } catch (SQLException e) {
            throw new IllegalStateException(e); // should never happen
        }
    }

    @Override
    public int getMajorVersion() {
        final Driver driver = getActualDriver();
        if (driver != null) {
            try {
                return driver.getMajorVersion();
            } catch (Exception e) {
                log.debug("Failed to get major version", e);
            }
        }
        return 0;
    }

    @Override
    public int getMinorVersion() {
        final Driver driver = getActualDriver();
        if (driver != null) {
            try {
                return driver.getMinorVersion();
            } catch (Exception e) {
                log.debug("Failed to get minor version", e);
            }
        }
        return 0;
    }

    @Override
    public boolean jdbcCompliant() {
        final Driver driver = getActualDriver();
        if (driver != null) {
            try {
                return driver.jdbcCompliant();
            } catch (Exception e) {
                log.debug("Failed to get JDBC compliance", e);
            }
        }
        return false;
    }

    @Override
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
        final Driver driver = getActualDriver();
        if (driver != null) {
            try {
                return driver.getParentLogger();
            } catch (Exception e) {
                log.debug("Failed to get parent logger", e);
            }
        }
        return null;
    }
}
