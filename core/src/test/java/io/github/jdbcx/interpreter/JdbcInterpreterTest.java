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
package io.github.jdbcx.interpreter;

import java.sql.SQLException;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.clickhouse.jdbc.ClickHouseDriver;

public class JdbcInterpreterTest {
    @Test(groups = { "unit" })
    public void testGetDriverByClass() throws SQLException {
        Assert.assertThrows(SQLException.class, () -> JdbcInterpreter.getDriverByClass(null, null));
        Assert.assertThrows(SQLException.class, () -> JdbcInterpreter.getDriverByClass("some.unknown.Driver", null));
        Assert.assertThrows(SQLException.class,
                () -> JdbcInterpreter.getDriverByClass(null, getClass().getClassLoader()));

        Assert.assertNotNull(JdbcInterpreter.getDriverByClass(ClickHouseDriver.class.getName(), null),
                "Should have driver");
        Assert.assertNotNull(
                JdbcInterpreter.getDriverByClass(ClickHouseDriver.class.getName(), getClass().getClassLoader()),
                "Should have driver");
    }

    @Test(groups = { "unit" })
    public void testGetDriverByUrl() throws SQLException {
        Assert.assertThrows(SQLException.class, () -> JdbcInterpreter.getDriverByUrl("", null));
        Assert.assertThrows(SQLException.class, () -> JdbcInterpreter.getDriverByUrl("jdbc:unknown:driver", null));
        Assert.assertThrows(SQLException.class,
                () -> JdbcInterpreter.getDriverByUrl("", getClass().getClassLoader()));

        Assert.assertNotNull(JdbcInterpreter.getDriverByUrl("jdbc:ch://localhost", null), "Should have driver");
        Assert.assertNotNull(JdbcInterpreter.getDriverByUrl("jdbc:ch://localhost", getClass().getClassLoader()),
                "Should have driver");
    }
}
