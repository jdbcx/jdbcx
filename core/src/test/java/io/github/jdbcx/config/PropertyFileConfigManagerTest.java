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
package io.github.jdbcx.config;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.github.jdbcx.ConfigManager;
import io.github.jdbcx.Constants;
import io.github.jdbcx.interpreter.JdbcInterpreter;

public class PropertyFileConfigManagerTest {
    @Test(groups = { "unit" })
    public void testConstructor() {
        Assert.assertNotNull(ConfigManager.newInstance(ConfigManager.PROPERTY_FILE_PROVIDER, null));
        Assert.assertNotNull(ConfigManager.newInstance(ConfigManager.PROPERTY_FILE_PROVIDER, new Properties()));
        Assert.assertEquals(
                ConfigManager.newInstance(ConfigManager.PROPERTY_FILE_PROVIDER, new Properties()).getClass(),
                PropertyFileConfigManager.class);
    }

    @Test(groups = { "integration" })
    public void testGetConnectionById() throws Exception {
        Properties config = new Properties();
        PropertyFileConfigManager.OPTION_BASE_DIR.setJdbcxValue(config, "target/test-classes/config");
        final ConfigManager manager = ConfigManager.newInstance(ConfigManager.PROPERTY_FILE_PROVIDER, config);
        try (Connection conn = JdbcInterpreter.getConnectionByConfig(manager.getConfig("db", "my-sqlite"), null);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("select 5")) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getInt(1), 5);
            Assert.assertFalse(rs.next());
        }

        Properties props = new Properties();
        ConfigManager.OPTION_CACHE.setJdbcxValue(props, Constants.TRUE_EXPR);
        PropertyFileConfigManager.OPTION_BASE_DIR.setJdbcxValue(props, "non-existent-directory");
        manager.reload(props);
        Assert.assertThrows(IllegalArgumentException.class, () -> manager.getConfig("db", "my-sqlite"));

        PropertyFileConfigManager.OPTION_BASE_DIR.setJdbcxValue(props, "target/test-classes/config");
        manager.reload(props);
        try (Connection conn = JdbcInterpreter.getConnectionByConfig(manager.getConfig("db", "my-sqlite"), null);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("select 6")) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getInt(1), 6);
            Assert.assertFalse(rs.next());
        }
    }
}
