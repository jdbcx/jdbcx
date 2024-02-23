/*
 * Copyright 2022-2024, Zhichun Wu
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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.testng.Assert;
import org.testng.annotations.Test;

public class CompressionTest {
    static final Compression defaultCompress = Compression.NONE;

    @Test(groups = { "unit" })
    public void testFromEncoding() {
        Assert.assertEquals(Compression.fromEncoding(null, null), defaultCompress);
        Assert.assertEquals(Compression.fromEncoding("", null), defaultCompress);
        Assert.assertEquals(Compression.fromEncoding("unknown", null), defaultCompress);

        Assert.assertEquals(Compression.fromEncoding("*", null), Compression.GZIP);
        Assert.assertEquals(Compression.fromEncoding("unknown, *", null), Compression.GZIP);
        Assert.assertEquals(Compression.fromEncoding("lz4, unknown, *", null), Compression.LZ4);

        for (Compression c : Compression.values()) {
            Assert.assertEquals(Compression.fromEncoding(c.encoding(), null), c);
            Assert.assertEquals(Compression.fromEncoding(c.encoding(), Compression.LZ4), c);
        }
    }

    @Test(groups = { "unit" })
    public void testFromFileExtension() {
        Assert.assertNull(Compression.fromFileExtension(null, null));
        Assert.assertNull(Compression.fromFileExtension("unknown", null));

        Assert.assertEquals(Compression.fromFileExtension("", null), Compression.NONE);
        Assert.assertEquals(Compression.fromFileExtension(null, defaultCompress), defaultCompress);
        Assert.assertEquals(Compression.fromFileExtension("", defaultCompress), defaultCompress);
        Assert.assertEquals(Compression.fromFileExtension("unknown", defaultCompress), defaultCompress);

        for (Compression c : Compression.values()) {
            Assert.assertEquals(Compression.fromFileExtension(c.fileExtension(false), null), c);
        }
    }

    @Test(groups = { "unit" })
    public void testFromFileName() {
        Assert.assertNull(Compression.fromFileName(null));
        Assert.assertNull(Compression.fromFileName(""));
        Assert.assertNull(Compression.fromFileName("unknown"));
        Assert.assertNull(Compression.fromFileName("my.json"));

        Assert.assertEquals(Compression.fromFileName("my.csv.gz"), Compression.GZIP);

        for (Compression c : Compression.values()) {
            if (c == Compression.NONE) {
                continue;
            }
            Assert.assertEquals(Compression.fromFileName("a" + c.fileExtension()), c);
        }
    }

    @Test(groups = { "unit" })
    public void testFromMagicNumber() {
        Assert.assertNull(Compression.fromMagicNumber(null));
        Assert.assertNull(Compression.fromMagicNumber(new byte[0]));
        Assert.assertNull(Compression.fromMagicNumber(new byte[1]));
        Assert.assertNull(Compression.fromMagicNumber(new byte[2]));

        Assert.assertEquals(
                Compression.fromMagicNumber(new byte[] { (byte) 0x04, (byte) 0x22, (byte) 0x4D, (byte) 0x18 }),
                Compression.LZ4);
    }

    @Test(groups = { "unit" })
    public void testFromMimeType() {
        Assert.assertEquals(Compression.fromMimeType(null), defaultCompress);
        Assert.assertEquals(Compression.fromMimeType(""), defaultCompress);
        Assert.assertEquals(Compression.fromMimeType("*"), defaultCompress);

        for (Compression c : Compression.values()) {
            Assert.assertEquals(Compression.fromMimeType(c.mimeType()), c);
        }

        Assert.assertEquals(Compression.fromMimeType("Application/GZIP"), Compression.GZIP);
        Assert.assertEquals(Compression.fromMimeType("unknown,application/x-xz;q=0.6,identity;q=0.4"), Compression.XZ);
        Assert.assertEquals(Compression.fromMimeType("application/x-bzip2"), Compression.BZIP2);
    }

    @Test(groups = { "unit" })
    public void testGetProvider() throws IOException {
        for (Compression c : Compression.values()) {
            Assert.assertNotNull(Compression.getProvider(c), c.name());
            byte[] bytes = Constants.EMPTY_BYTE_ARRAY;
            int bufferSize = 0;
            int level = -1;
            try (ByteArrayOutputStream out = new ByteArrayOutputStream();
                    OutputStream compressedOut = c.provider().compress(out, bufferSize, level)) {
                compressedOut.write(new byte[] { 1, 2, 3 });
                compressedOut.close();
                bytes = out.toByteArray();
            }

            if (c.hasMagicNumber()) {
                Assert.assertEquals(Compression.fromMagicNumber(bytes), c);
            } else {
                Assert.assertNull(Compression.fromMagicNumber(bytes), c.name());
            }

            try (ByteArrayInputStream in = new ByteArrayInputStream(bytes);
                    InputStream decompressedIn = c.provider().decompress(in, bufferSize, level)) {
                Assert.assertEquals(decompressedIn.read(), 1);
                Assert.assertEquals(decompressedIn.read(), 2);
                Assert.assertEquals(decompressedIn.read(), 3);
                Assert.assertEquals(decompressedIn.read(), -1);
            }
        }
    }
}
