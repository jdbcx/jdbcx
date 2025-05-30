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
package io.github.jdbcx;

import java.util.Properties;

import org.junit.Assert;
import org.testng.annotations.Test;

public class ConfigManagerTest {
    public static final class TestConfigManager extends ConfigManager {
        public TestConfigManager(Properties props) {
            super(props);
        }
    }

    @Test(groups = { "unit" })
    public void testNewInstance() {
        Assert.assertNotNull(ConfigManager.newInstance(TestConfigManager.class.getName(), null));
        Assert.assertNotNull(ConfigManager.newInstance(TestConfigManager.class.getName(), new Properties()));

        Assert.assertEquals(ConfigManager.newInstance(TestConfigManager.class.getName(), null).getClass(),
                TestConfigManager.class);
    }

    @Test(groups = { "unit" })
    public void testParseGlobPattern() {
        Assert.assertFalse(ConfigManager.parseGlobPattern("a*").matcher("b").matches());
        Assert.assertTrue(ConfigManager.parseGlobPattern("a*").matcher("a").matches());
        Assert.assertTrue(ConfigManager.parseGlobPattern("a*").matcher("a1").matches());

        Assert.assertFalse(ConfigManager.parseGlobPattern("a?").matcher("a").matches());
        Assert.assertTrue(ConfigManager.parseGlobPattern("a?").matcher("a2").matches());

        Assert.assertFalse(ConfigManager.parseGlobPattern("a?*").matcher("a").matches());
        Assert.assertTrue(ConfigManager.parseGlobPattern("a?*").matcher("a123").matches());

        Assert.assertFalse(ConfigManager.parseGlobPattern("[a.*]").matcher("b").matches());
        Assert.assertTrue(ConfigManager.parseGlobPattern("[a.*]").matcher("a").matches());
        Assert.assertTrue(ConfigManager.parseGlobPattern("[a.*]").matcher(".").matches());
        Assert.assertTrue(ConfigManager.parseGlobPattern("[a.*]").matcher("*").matches());
        Assert.assertTrue(ConfigManager.parseGlobPattern("[ab-d]").matcher("c").matches());

        Assert.assertTrue(ConfigManager.parseGlobPattern("a-[bc]*").matcher("a-bb").matches());
        Assert.assertTrue(ConfigManager.parseGlobPattern("a-[bc]*").matcher("a-cdb").matches());
    }
}
