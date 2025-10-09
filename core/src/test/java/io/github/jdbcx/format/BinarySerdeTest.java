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
package io.github.jdbcx.format;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.github.jdbcx.Result;

public class BinarySerdeTest {
    @Test(groups = { "unit" })
    public void testConstructor() {
        BinarySerde serde = new BinarySerde(null);
        Assert.assertEquals(serde.charset, StandardCharsets.UTF_8);

        Properties props = new Properties();
        serde = new BinarySerde(props);
        Assert.assertEquals(serde.charset, StandardCharsets.UTF_8);

        BinarySerde.OPTION_CHARSET.setValue(props, "gb18030");
        serde = new BinarySerde(props);
        Assert.assertEquals(serde.charset, Charset.forName("gb18030"));
    }

    @Test(groups = { "unit" })
    public void testSerde() throws IOException, SQLException {
        Properties config = new Properties();
        BinarySerde serde = new BinarySerde(config);
        Assert.assertThrows(UnsupportedOperationException.class,
                () -> serde.deserialize(new ByteArrayInputStream(new byte[0])));

        byte[] bytes = new byte[] { 5, 3, 1 };
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            serde.serialize(Result.of(new ByteArrayInputStream(bytes), null), out);
            Assert.assertEquals(out.toByteArray(), bytes);
        }

        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            serde.serialize(Result.of("123"), out);
            Assert.assertEquals(out.toByteArray(), new byte[] { 49, 50, 51 });
        }

        Properties props = new Properties();
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite::memory:", props);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("select '123'");
                ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            serde.serialize(Result.of(rs), out);
            Assert.assertFalse(rs.isClosed());
            Assert.assertFalse(rs.next());
            Assert.assertEquals(out.toByteArray(), new byte[] { 49, 50, 51 });
        }
    }
}
