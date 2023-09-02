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

import java.sql.SQLException;

import org.testng.Assert;
import org.testng.annotations.Test;

public class JdbcInterpreterTest {
    @Test(groups = { "unit" })
    public void testGetDriver() throws SQLException {
        Assert.assertThrows(SQLException.class, () -> JdbcInterpreter.getDriver(null, null));
        Assert.assertThrows(SQLException.class, () -> JdbcInterpreter.getDriver("jdbc:unknown:driver", null));
        Assert.assertThrows(SQLException.class,
                () -> JdbcInterpreter.getDriver(null, getClass().getClassLoader()));

        Assert.assertNotNull(JdbcInterpreter.getDriver("jdbc:ch://localhost", null), "Should have driver");
        Assert.assertNotNull(JdbcInterpreter.getDriver("jdbc:ch://localhost", getClass().getClassLoader()),
                "Should have driver");
    }
}
