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
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwt;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.lang.Collections;
import io.jsonwebtoken.security.Keys;
import io.jsonwebtoken.security.WeakKeyException;

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
        ConfigManager.OPTION_KEY_FILE.setJdbcxValue(props, secretFile);
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
            ConfigManager.OPTION_KEY_FILE.setJdbcxValue(props, secretFile);
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
    public void testCreateAlgorithm() {
        Assert.assertEquals(ConfigManager.createAlgorithm(null), new ConfigManager.JwtAlgorithm(Jwts.SIG.NONE, null));
        Assert.assertEquals(ConfigManager.createAlgorithm(""), new ConfigManager.JwtAlgorithm(Jwts.SIG.NONE, null));
        Assert.assertThrows(WeakKeyException.class, () -> ConfigManager.createAlgorithm(" "));

        String key;
        Assert.assertEquals(
                ConfigManager.createAlgorithm(key = " 123456789012345678901234567890123456789012345678901234567890 "),
                new ConfigManager.JwtAlgorithm(Jwts.SIG.HS256, Keys.hmacShaKeyFor(key.getBytes())));
        Assert.assertEquals(
                ConfigManager.createAlgorithm(key = "x123456789012345678901234567890123456789012345678901234567890"),
                new ConfigManager.JwtAlgorithm(Jwts.SIG.HS256, Keys.hmacShaKeyFor(key.getBytes())));

        Assert.assertEquals(
                ConfigManager.createAlgorithm(
                        "HS256:" + (key = "12:3123456789012345678901234567890123456789012345678901234567890")),
                new ConfigManager.JwtAlgorithm(Jwts.SIG.HS256, Keys.hmacShaKeyFor(key.getBytes())));
        Assert.assertEquals(
                ConfigManager.createAlgorithm(
                        "HS384:" + (key = "1: 2:3123456789012345678901234567890123456789012345678901234567890")),
                new ConfigManager.JwtAlgorithm(Jwts.SIG.HS384, Keys.hmacShaKeyFor(key.getBytes())));
        Assert.assertEquals(
                ConfigManager.createAlgorithm(
                        "HS512:" + (key = ": 1:2:3123456789012345678901234567890123456789012345678901234567890: ")),
                new ConfigManager.JwtAlgorithm(Jwts.SIG.HS512, Keys.hmacShaKeyFor(key.getBytes())));
    }

    @Test(groups = { "unit" })
    public void testJwt() {
        Properties props = new Properties();
        TestConfigManager manager = new TestConfigManager(props);
        String token = manager.generateToken("me", "you", "audience1,audience2", 5, null);
        Jwt<?, ?> jws = manager.getTokenVerifier("me").parse(token);
        Assert.assertEquals(jws.getHeader().getAlgorithm(), "none");
        Assert.assertEquals(((Claims) jws.getPayload()).getAudience(), Collections.setOf("audience1", "audience2"));
        Assert.assertEquals(((Claims) jws.getPayload()).size(), manager.verifyToken("me", token).size());
        Assert.assertEquals(((Claims) jws.getPayload()).getIssuer(), "me");
        Assert.assertEquals(((Claims) jws.getPayload()).getSubject(), "you");

        Option.SERVER_SECRET.setJdbcxValue(props,
                "HS512:2ZPaxiFaz7TFM1wgxfktxCaoGlZ2VptVo5XcD/11G4HwTnV4GpbFYxEnejylPuNM/39cT3Xg20VBzXCwElWORTQpkCdPalD7CtYT/l9UuK8JdYXWBTipxxVzzA6mYXgd59exEwnnmacxibHcVt0ZuxZCQiNok9bQhtVY/yoIa75mWLHOZnO5to0HAdjy7BS/CwmmCaia4sV6neZJjt8xPlAyARoT1FNniUERDh1vkjQOjB3oc5aocsF316u54uyIvnYBS0dKLITQpmKqflxhhKMkVINNk+FLF6fwe0o+Tc6Ps/5PrEABg+C3Y974O67yQQ6BKUms/NvvOiUj7CKSkReF8SrTVrTl4nQ6STbJDrusKBisOAqKA3NDfBbAi9K/hIF/TpALR7fwWdlUbP7hshYDvYN20hje9wcswqxXjqT115Jyw6v2+Bzv7q8FMulbEAw7p+7vYe6ma5zWToR33doJzqYOETu9787xLrrKpHfDgrdyDvZsB1EsuxNuExjOl+g3yYjHwC4JIorgGLkX+pyANVnD3JaCOhgd5b0lMDfBLWJmthraoiSwaThoY0xrPBUzJ3Q0zyYsn6mrxi/K1jSb52BXKL7yeqIiE8xp2Ychtgyvf/s6YU1f4gAXTDS/4ZjEBsHhp5f7op/MdeyvhhRSGqAdT/yBNMLOzvI0gak=");
        manager = new TestConfigManager(props);
        Map<String, String> claims = new HashMap<>();
        claims.put("a1", "b");
        claims.put("b2", "x");
        token = manager.generateToken("you", "me", null, 5, claims);
        jws = manager.getTokenVerifier("you").parse(token);
        Assert.assertEquals(jws.getHeader().getAlgorithm(), "HS512");
        Assert.assertEquals(((Claims) jws.getPayload()).getAudience(), null);
        Assert.assertEquals(((Claims) jws.getPayload()).size(), manager.verifyToken("you", token).size());
        Assert.assertEquals(((Claims) jws.getPayload()).getIssuer(), "you");
        Assert.assertEquals(((Claims) jws.getPayload()).getSubject(), "me");
        Assert.assertEquals(((Claims) jws.getPayload()).get("a1", String.class), "b");
        Assert.assertEquals(((Claims) jws.getPayload()).get("b2", String.class), "x");

        // Option.SERVER_SECRET.setJdbcxValue(props,
        // "HS512:QXk18EEgn9r2xf2NF6PXUf9L9kI7srS4L89lXJp9f+iVkp6ugZICjAu5iD4LOdFEyXHcXp/mnJ3K4wu85D77kV1w");
        // manager.generateToken("https://my.company.com", "my@email.address", null,
        // 1440, java.util.Collections.singletonMap("allowed_ips", "192.168.1.0/24"));
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
        ConfigManager.OPTION_KEY_FILE.setJdbcxValue(base, "target/test-classes/secret.test");
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

    @Test(groups = { "unit" })
    public void testTenants() {
        TestConfigManager manager = new TestConfigManager(null);
        // manager.tenants
    }
}
