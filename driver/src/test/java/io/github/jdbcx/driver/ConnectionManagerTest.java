/*
 * Copyright 2022-2025, Zhichun Wu
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
import java.sql.SQLException;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import io.github.jdbcx.BaseIntegrationTest;
import io.github.jdbcx.Constants;
import io.github.jdbcx.Option;
import io.github.jdbcx.Utils;
import io.github.jdbcx.WrappedDriver;

public class ConnectionManagerTest extends BaseIntegrationTest {
    @DataProvider(name = "dbProductProvider")
    public Object[][] getDatabaseProducts() {
        return new Object[][] {
                // jdbcUrl, expected product name
                { "jdbcx:", "JDBCX/" },
                { "jdbcx:sqlite::memory:", "SQLite/" },
                { "jdbcx:duckdb:", "DuckDB/" },
                { "jdbcx:ch://" + getClickHouseServer(), "ClickHouse/" }
        };
    }

    @Test(groups = { "unit" })
    public void testHandleString() {
        String str = " re'sult ";
        Assert.assertEquals(ConnectionManager.normalize(str, null, null), str);

        Properties props = new Properties();
        Assert.assertEquals(ConnectionManager.normalize(str, null, props), str);
        Option.RESULT_STRING_TRIM.setValue(props);
        Assert.assertEquals(ConnectionManager.normalize(str, null, props), str);
        Option.RESULT_STRING_TRIM.setValue(props, Constants.TRUE_EXPR);
        Assert.assertEquals(ConnectionManager.normalize(str, null, props), str.trim());

        Option.RESULT_STRING_ESCAPE.setValue(props);
        Assert.assertEquals(ConnectionManager.normalize(str, null, props), str.trim());
        Option.RESULT_STRING_ESCAPE.setValue(props, Constants.TRUE_EXPR);
        Assert.assertEquals(ConnectionManager.normalize(str, null, props), str.trim().replace("'", "\\'"));
        Option.RESULT_STRING_ESCAPE_CHAR.setValue(props);
        Assert.assertEquals(ConnectionManager.normalize(str, null, props), str.trim().replace("'", "\\'"));
        Option.RESULT_STRING_ESCAPE_CHAR.setValue(props, "b");
        Assert.assertEquals(ConnectionManager.normalize(str, null, props), str.trim().replace("'", "b'"));
        Option.RESULT_STRING_ESCAPE_TARGET.setValue(props);
        Assert.assertEquals(ConnectionManager.normalize(str, null, props), str.trim().replace("'", "b'"));
        Option.RESULT_STRING_ESCAPE_TARGET.setValue(props, "s");
        Assert.assertEquals(ConnectionManager.normalize(str, null, props), str.trim().replace("s", "bs"));
    }

    @Test(dataProvider = "dbProductProvider", groups = { "integration" })
    public void testGetProductName(String jdbcUrl, String productName) throws SQLException {
        WrappedDriver driver = new WrappedDriver();
        Properties props = new Properties();
        try (Connection conn = driver.connect(jdbcUrl, props)) {
            Assert.assertTrue(conn instanceof ManagedConnection, "Should return managed connection");
            ConnectionManager manager = ((ManagedConnection) conn).getManager();
            Assert.assertNotNull(manager, "Should have non-null connection manager");
            Assert.assertTrue(manager.getMetaData().getProduct().startsWith(productName),
                    Utils.format("Product name should start with '%s'", productName));
        }
    }
}
