/*
 * Copyright 2022-2026, Zhichun Wu
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

import java.sql.DatabaseMetaData;

import io.github.jdbcx.Checker;
import io.github.jdbcx.Constants;

public final class ConnectionMetaData {
    static String merge(String name, String version) {
        if (version.isEmpty()) {
            return name;
        }
        return new StringBuilder(name.length() + version.length() + 1).append(name).append('/').append(version)
                .toString();
    }

    private final String packageName;
    private final String productName;
    private final String productVersion;
    private final String driverName;
    private final String driverVersion;
    private final String userName;
    private final String url;

    ConnectionMetaData(String packageName) {
        this.packageName = packageName;
        this.productName = Constants.EMPTY_STRING;
        this.productVersion = Constants.EMPTY_STRING;
        this.driverName = Constants.EMPTY_STRING;
        this.driverVersion = Constants.EMPTY_STRING;
        this.userName = Constants.EMPTY_STRING;
        this.url = Constants.EMPTY_STRING;
    }

    ConnectionMetaData(DatabaseMetaData metaData) {
        this.packageName = metaData.getClass().getPackage().getName();

        String value = null;
        try {
            value = metaData.getDatabaseProductName();
        } catch (Exception e) {
            // ignore
        }
        this.productName = Checker.isNullOrEmpty(value) ? Constants.EMPTY_STRING : value;

        value = null;
        try {
            value = new StringBuilder().append(metaData.getDatabaseMajorVersion()).append('.')
                    .append(metaData.getDatabaseMinorVersion()).toString();
        } catch (Exception e) {
            try {
                value = metaData.getDatabaseProductVersion();
            } catch (Exception ex) {
                // ignore
            }
        }
        this.productVersion = Checker.isNullOrEmpty(value) ? Constants.EMPTY_STRING : value;

        value = null;
        try {
            value = metaData.getDriverName();
        } catch (Exception e) {
            // ignore
        }
        this.driverName = Checker.isNullOrEmpty(value) ? Constants.EMPTY_STRING : value;

        value = null;
        try {
            value = new StringBuilder().append(metaData.getDriverMajorVersion()).append('.')
                    .append(metaData.getDriverMinorVersion()).toString();
        } catch (Exception e) {
            try {
                value = metaData.getDriverVersion();
            } catch (Exception ex) {
                // ignore
            }
        }
        this.driverVersion = Checker.isNullOrEmpty(value) ? Constants.EMPTY_STRING : value;

        value = null;
        try {
            value = metaData.getUserName();
        } catch (Exception e) {
            // ignore
        }
        this.userName = Checker.isNullOrEmpty(value) ? Constants.EMPTY_STRING : value;

        value = null;
        try {
            value = metaData.getURL();
        } catch (Exception e) {
            // ignore
        }
        this.url = Checker.isNullOrEmpty(value) ? Constants.EMPTY_STRING : value;
    }

    public String getPackageName() {
        return packageName;
    }

    public String getProductName() {
        return productName;
    }

    public String getProductVersion() {
        return productVersion;
    }

    public String getProduct() {
        return productName.isEmpty() ? packageName : merge(productName, productVersion);
    }

    public String getDriverName() {
        return driverName;
    }

    public String getDriverVersion() {
        return driverVersion;
    }

    public String getDriver() {
        return driverName.isEmpty() ? packageName : merge(driverName, driverVersion);
    }

    public String getUserName() {
        return userName;
    }

    public String getUrl() {
        return url;
    }

    public boolean hasUserName() {
        return !userName.isEmpty();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = prime + packageName.hashCode();
        result = prime * result + productName.hashCode();
        result = prime * result + productVersion.hashCode();
        result = prime * result + driverName.hashCode();
        result = prime * result + driverVersion.hashCode();
        result = prime * result + userName.hashCode();
        result = prime * result + url.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        ConnectionMetaData other = (ConnectionMetaData) obj;
        return packageName.equals(other.packageName) && productName.equals(other.productName)
                && productVersion.equals(other.productVersion) && driverName.equals(other.driverName)
                && driverVersion.equals(other.driverVersion) && userName.equals(other.userName)
                && url.equals(other.url);
    }
}
