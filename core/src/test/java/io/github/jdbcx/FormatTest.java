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

import org.testng.Assert;
import org.testng.annotations.Test;

public class FormatTest {
    static final Format defaultFormat = Format.CSV;

    @Test(groups = { "unit" })
    public void testFromFileExtension() {
        Assert.assertNull(Format.fromFileExtension(null, null));
        Assert.assertNull(Format.fromFileExtension("", null));
        Assert.assertNull(Format.fromFileExtension("unknown", null));

        Assert.assertEquals(Format.fromFileExtension(null, defaultFormat), defaultFormat);
        Assert.assertEquals(Format.fromFileExtension("", defaultFormat), defaultFormat);
        Assert.assertEquals(Format.fromFileExtension("unknown", defaultFormat), defaultFormat);

        for (Format f : Format.values()) {
            Assert.assertEquals(Format.fromFileExtension(f.fileExtension(false), null), f);
        }
    }

    @Test(groups = { "unit" })
    public void testFromFileName() {
        Assert.assertNull(Format.fromFileName(null));
        Assert.assertNull(Format.fromFileName(""));
        Assert.assertNull(Format.fromFileName("unknown"));

        Assert.assertEquals(Format.fromFileName("my.json"), Format.JSON);
        Assert.assertNull(Format.fromFileName("my.csv.gz"));

        for (Format f : Format.values()) {
            Assert.assertEquals(Format.fromFileName("a" + f.fileExtension()), f);
        }
    }

    @Test(groups = { "unit" })
    public void testFromMimeType() {
        Assert.assertEquals(Format.fromMimeType(null, null), defaultFormat);
        Assert.assertEquals(Format.fromMimeType("", null), defaultFormat);
        Assert.assertEquals(Format.fromMimeType("*/csv", null), defaultFormat);
        Assert.assertEquals(Format.fromMimeType("*/*", null), defaultFormat);
        Assert.assertEquals(Format.fromMimeType("text/*", null), defaultFormat);
        Assert.assertEquals(Format.fromMimeType("unknown", null), defaultFormat);

        final Format format = Format.BSON;
        Assert.assertEquals(Format.fromMimeType(null, format), format);
        Assert.assertEquals(Format.fromMimeType("", format), format);
        Assert.assertEquals(Format.fromMimeType("*/csv", format), format);
        Assert.assertEquals(Format.fromMimeType("*/*", format), format);
        Assert.assertEquals(Format.fromMimeType("text/*", format), defaultFormat);
        Assert.assertEquals(Format.fromMimeType("unknown", format), format);

        for (Format f : Format.values()) {
            Assert.assertEquals(Format.fromMimeType(f.mimeType(), null), f);
            Assert.assertEquals(Format.fromMimeType(f.mimeType(), format), f);
        }

        Assert.assertEquals(Format.fromMimeType("Application/JSON", null), Format.JSON);
        Assert.assertEquals(Format.fromMimeType("application/bson;q=0.6,text/csv;q=0.4", null), Format.BSON);
        Assert.assertEquals(Format.fromMimeType("text/csv", null), Format.CSV);
        Assert.assertEquals(
                Format.fromMimeType("text/html, application/unknown, application/vnd.apache.parquet;q=0.9, image/webp",
                        null),
                Format.PARQUET);

        Assert.assertEquals(Format.fromMimeType("Application/JSON", format), Format.JSON);
        Assert.assertEquals(Format.fromMimeType("application/bson;q=0.6,text/csv;q=0.4", format), Format.BSON);
        Assert.assertEquals(Format.fromMimeType("text/csv", format), Format.CSV);
        Assert.assertEquals(
                Format.fromMimeType("text/html, application/unknown, application/vnd.apache.parquet;q=0.9, image/webp",
                        format),
                Format.PARQUET);
    }

    @Test(groups = { "unit" })
    public void testNormalizeAvroField() {
        Assert.assertEquals(Format.normalizeAvroField(0, null), "f1");
        Assert.assertEquals(Format.normalizeAvroField(0, "f"), "f");
        Assert.assertEquals(Format.normalizeAvroField(0, "ff"), "ff");
        Assert.assertEquals(Format.normalizeAvroField(0, "?"), "f1");
        Assert.assertEquals(Format.normalizeAvroField(0, "??"), "f1");
        Assert.assertEquals(Format.normalizeAvroField(0, "123"), "f1_123");
    }
}
