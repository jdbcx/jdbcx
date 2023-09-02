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
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.github.jdbcx.interpreter.ConfigManager;
import io.github.jdbcx.interpreter.JdbcInterpreter;

public class FileBasedConfigManagerTest {
    @Test(groups = { "unit" })
    public void testConstructor() {
        Assert.assertNotNull(ConfigManager.getInstance());
        Assert.assertEquals(ConfigManager.getInstance().getClass(), FileBasedConfigManager.class);
    }

    @Test(groups = { "private" })
    public void testGetConnectionById() throws Exception {
        ConfigManager manager = ConfigManager.getInstance();
        manager.reload(null);
        try (Connection conn = JdbcInterpreter.getConnection(null, manager.getConfig("db", "ch-dev"), null);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("select 5")) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getInt(1), 5);
            Assert.assertFalse(rs.next());
        }

        Properties props = new Properties();
        ConfigManager.OPTION_CACHE.setJdbcxValue(props, "true");
        FileBasedConfigManager.OPTION_DIRECTORY.setJdbcxValue(props, "non-existent-directory");
        manager.reload(props);
        Assert.assertThrows(IllegalArgumentException.class,
                () -> ConfigManager.getInstance().getConfig("db", "ch-dev"));

        FileBasedConfigManager.OPTION_DIRECTORY.setJdbcxValue(props, null); // reset to default
        manager.reload(props);
        try (Connection conn = JdbcInterpreter.getConnection(null, manager.getConfig("db", "ch-dev"), null);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("select 6")) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getInt(1), 6);
            Assert.assertFalse(rs.next());
        }
    }
}
