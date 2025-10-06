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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.security.Key;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ConfigManagerTest {
    public static final class TestConfigManager extends ConfigManager {
        public TestConfigManager(Properties props) {
            super(props);
        }
    }

    @Test(groups = { "unit" })
    public void testConstructor() throws IOException {
        final String associatedData = "datasourceId";
        final String plainText = "test content";

        String secretFile = "target/test-classes/secret.aes";
        Properties props = new Properties();
        ConfigManager.OPTION_SECRET_FILE.setJdbcxValue(props, secretFile);
        ConfigManager.OPTION_ALGORITHM.setJdbcxValue(props, ConfigManager.ALGORITHM_AES_GCM_NOPADDING);

        TestConfigManager manager = new TestConfigManager(props);
        Assert.assertEquals(Utils.toBase64(manager.loadKey().getEncoded()),
                Stream.readAllAsString(new FileInputStream(secretFile)));
        Assert.assertEquals(manager.decrypt(manager.encrypt(plainText)), plainText);
        Assert.assertEquals(manager.decrypt(manager.encrypt(plainText, associatedData, StandardCharsets.ISO_8859_1),
                associatedData, StandardCharsets.ISO_8859_1), plainText);

        if (Constants.JAVA_MAJOR_VERSION >= 17) {
            secretFile = "target/test-classes/secret.chacha";
            props = new Properties();
            ConfigManager.OPTION_SECRET_FILE.setJdbcxValue(props, secretFile);
            ConfigManager.OPTION_ALGORITHM.setJdbcxValue(props, ConfigManager.ALGORITHM_CHACHA20_POLY1305);
            manager = new TestConfigManager(props);
            Assert.assertEquals(Utils.toBase64(manager.loadKey().getEncoded()),
                    Stream.readAllAsString(new FileInputStream(secretFile)));
            Assert.assertEquals(manager.decrypt(manager.encrypt(plainText)), plainText);
            Assert.assertEquals(manager.decrypt(manager.encrypt(plainText, associatedData, StandardCharsets.ISO_8859_1),
                    associatedData, StandardCharsets.ISO_8859_1), plainText);
        }
    }

    @Test(groups = { "unit" })
    public void testConcatenate() {
        Assert.assertEquals(ConfigManager.concatenate(null, null), new byte[0]);
        Assert.assertEquals(ConfigManager.concatenate(null, new byte[0]), new byte[0]);
        Assert.assertEquals(ConfigManager.concatenate(new byte[0], null), new byte[0]);
        Assert.assertEquals(ConfigManager.concatenate(new byte[0], new byte[0]), new byte[0]);

        final byte[] arr = new byte[] { 1, 3, 5 };
        Assert.assertTrue(ConfigManager.concatenate(arr, null) == arr);
        Assert.assertTrue(ConfigManager.concatenate(arr, new byte[0]) == arr);
        Assert.assertTrue(ConfigManager.concatenate(null, arr) == arr);
        Assert.assertTrue(ConfigManager.concatenate(new byte[0], arr) == arr);

        Assert.assertEquals(ConfigManager.concatenate(new byte[0], new byte[0]), new byte[0]);
        Assert.assertEquals(ConfigManager.concatenate(new byte[] { 1, 2, 3 }, new byte[] { 4, 5 }),
                new byte[] { 1, 2, 3, 4, 5 });
    }

    @Test(groups = { "unit" })
    public void testEncryptAndDecrypt() {
        TestConfigManager manager = new TestConfigManager(null);
        Key secretKey = manager.generateKeySpec(null, 0);

        for (String aad : new String[] { null, "userid" }) {
            String encrypted = manager.encrypt(secretKey, "123", aad, StandardCharsets.ISO_8859_1);
            Assert.assertEquals(manager.decrypt(secretKey, encrypted, aad, StandardCharsets.ISO_8859_1), "123");

            encrypted = manager.encrypt(secretKey, "萌萌哒😂3", aad, null);
            Assert.assertEquals(manager.decrypt(secretKey, encrypted, aad, null), "萌萌哒😂3");
        }
    }

    @Test(groups = { "unit" })
    public void testDecryption() {
        TestConfigManager manager = new TestConfigManager(null);
        for (String aad : new String[] { null, "userid" }) {
            Properties props = null;
            manager.decrypt(props, aad);

            props = new Properties();
            manager.decrypt(props, aad);
            Assert.assertEquals(props, new Properties());

            props.setProperty("a", "b");
            manager.decrypt(props, aad);
            Assert.assertEquals(props.size(), 1);
            Assert.assertEquals(props.getProperty("a"), "b");

            final Key key = manager.generateKeySpec(null, 0);
            props.setProperty("a.encrypted", manager.encrypt(key, "1234", aad, null));
            manager.decrypt(key, props, aad, null);
            Assert.assertEquals(props.size(), 1);
            Assert.assertEquals(props.getProperty("a"), "1234");
        }
    }

    @Test(groups = { "unit" })
    public void testLoad() {
        Properties base = new Properties();
        ConfigManager.OPTION_SECRET_FILE.setJdbcxValue(base, "target/test-classes/secret.test");
        TestConfigManager manager = new TestConfigManager(base);
        Properties props = manager.load("target/test-classes/test-load.properties", base);
        Assert.assertEquals(props.size(), 2);
        Assert.assertEquals(props.getProperty("jdbcx.this"), "321");
        Assert.assertEquals(props.getProperty("jdbcx.that"), "123");

        TestConfigManager m = new TestConfigManager(new Properties());
        Assert.assertThrows(IllegalArgumentException.class,
                () -> m.load("target/test-classes/test-load.properties", base));
    }

    @Test(groups = { "unit" })
    public void testKeyGen() {
        TestConfigManager manager = new TestConfigManager(null);

        Assert.assertFalse(manager.generateKey().isEmpty(), "Shoud have key generated");
        Assert.assertFalse(manager.generateKey(null, 0).isEmpty(), "Shoud have key generated");
        Assert.assertFalse(manager.generateKey("AES", 192).isEmpty(), "Shoud have key generated");
    }

    @Test(groups = { "unit" })
    public void testLoadKey() throws IOException {
        TestConfigManager manager = new TestConfigManager(null);

        String key = manager.generateKey();
        File f = Utils.createTempFile();
        try (OutputStream out = new FileOutputStream(f)) {
            Stream.writeAll(out, key);
        }

        Assert.assertEquals(Utils.toBase64(manager.loadKey(f.toPath(), null).getEncoded()), key);
    }

    @Test(groups = { "unit" })
    public void testNewInstance() {
        Assert.assertNotNull(ConfigManager.newInstance(null));

        Properties props = new Properties();
        Assert.assertNotNull(ConfigManager.newInstance(props));
        ConfigManager.OPTION_CONFIG_PROVIDER.setJdbcxValue(props, TestConfigManager.class.getName());
        Assert.assertEquals(ConfigManager.newInstance(props).getClass(), TestConfigManager.class);
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
