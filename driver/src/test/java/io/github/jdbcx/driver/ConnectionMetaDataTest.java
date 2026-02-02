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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ConnectionMetaDataTest {
    @Test(groups = { "unit" })
    public void testConstructor() throws SQLException {
        ConnectionMetaData md = new ConnectionMetaData("1.2.3");
        Assert.assertEquals(md.getPackageName(), "1.2.3");
        Assert.assertEquals(md.getProduct(), "1.2.3");
        Assert.assertEquals(md.getDriver(), "1.2.3");
        Assert.assertEquals(md.getProductName(), "");
        Assert.assertEquals(md.getProductVersion(), "");
        Assert.assertEquals(md.getDriverName(), "");
        Assert.assertEquals(md.getDriverVersion(), "");
        Assert.assertEquals(md.getUserName(), "");
        Assert.assertEquals(md.getUrl(), "");

        try (Connection conn = DriverManager.getConnection("jdbc:sqlite::memory:")) {
            DatabaseMetaData d = conn.getMetaData();
            md = new ConnectionMetaData(conn.getMetaData());
            Assert.assertEquals(md.getPackageName(), conn.getClass().getPackage().getName());
            Assert.assertEquals(md.getProduct(),
                    d.getDatabaseProductName() + "/" + d.getDatabaseMajorVersion() + "." + d.getDatabaseMinorVersion());
            Assert.assertEquals(md.getProductName(), d.getDatabaseProductName());
            Assert.assertEquals(md.getProductVersion(),
                    d.getDatabaseMajorVersion() + "." + d.getDatabaseMinorVersion());
            Assert.assertEquals(md.getDriver(),
                    d.getDriverName() + "/" + d.getDriverMajorVersion() + "." + d.getDriverMinorVersion());
            Assert.assertEquals(md.getDriverName(), d.getDriverName());
            Assert.assertEquals(md.getDriverVersion(), d.getDriverMajorVersion() + "." + d.getDriverMinorVersion());
            Assert.assertEquals(md.getUserName(), "");
            Assert.assertEquals(md.getUrl(), d.getURL());
        }
    }

    @Test(groups = { "unit" })
    public void testEquals() throws SQLException {
        Assert.assertEquals(new ConnectionMetaData("1" + "2"), new ConnectionMetaData("312".substring(1)));

        try (Connection conn1 = DriverManager.getConnection("jdbc:sqlite::memory:");
                Connection conn2 = DriverManager.getConnection("jdbc:sqlite::memory:")) {
            Assert.assertEquals(new ConnectionMetaData(conn1.getMetaData()),
                    new ConnectionMetaData(conn2.getMetaData()));
        }
    }
}
