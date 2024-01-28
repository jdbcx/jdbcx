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
package io.github.jdbcx.data;

import java.io.StringReader;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.github.jdbcx.Row;

public class IterableReaderTest {
    @Test(groups = { "unit" })
    public void testRead() {
        String str = "\na1,a2,a3\n,a2,,a5\n";
        int index = 0;
        for (Row r : new IterableReader(DefaultRow.defaultFields, new StringReader(str))) {
            Assert.assertEquals(r.value(0).asString(), str);
            index++;
        }
        Assert.assertEquals(index, 1);

        String[] arr = str.split("\n");
        index = 0;
        for (Row r : new IterableReader(DefaultRow.defaultFields, new StringReader(str), "\n")) {
            Assert.assertEquals(r.value(0).asString(), arr[index++]);
        }
        Assert.assertEquals(index, arr.length);

        arr = str.split(",");
        index = 0;
        for (Row r : new IterableReader(DefaultRow.defaultFields, new StringReader(str), ",")) {
            Assert.assertEquals(r.value(0).asString(), arr[index++]);
        }
        Assert.assertEquals(index, arr.length);

        arr = str.split(",a");
        index = 0;
        for (Row r : new IterableReader(DefaultRow.defaultFields, new StringReader(str), ",a")) {
            Assert.assertEquals(r.value(0).asString(), arr[index++]);
        }
        Assert.assertEquals(index, arr.length);

        arr = str.split(",a2");
        index = 0;
        for (Row r : new IterableReader(DefaultRow.defaultFields, new StringReader(str), ",a2")) {
            Assert.assertEquals(r.value(0).asString(), arr[index++]);
        }
        Assert.assertEquals(index, arr.length);

        index = 0;
        for (Row r : new IterableReader(DefaultRow.defaultFields, new StringReader(str), str)) {
            Assert.assertEquals(r.value(0).asString(), "");
            index++;
        }
        Assert.assertEquals(index, 1);

        index = 0;
        for (Row r : new IterableReader(DefaultRow.defaultFields, new StringReader(str), "1" + str)) {
            Assert.assertEquals(r.value(0).asString(), str);
            index++;
        }
        Assert.assertEquals(index, 1);
    }
}
