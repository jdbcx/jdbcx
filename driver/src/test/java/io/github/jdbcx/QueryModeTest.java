/*
 * Copyright 2022-2026, Zhichun Wu
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

import java.util.Locale;

import org.testng.Assert;
import org.testng.annotations.Test;

public class QueryModeTest {
    @Test(groups = { "unit" })
    public void testFromCode() {
        Assert.assertNull(QueryMode.fromCode('?', null));
        Assert.assertEquals(QueryMode.fromCode('a', QueryMode.DIRECT), QueryMode.ASYNC);
        Assert.assertEquals(QueryMode.fromCode('B'), QueryMode.BATCH);

        Assert.assertEquals(QueryMode.fromCode('?'), QueryMode.SUBMIT);
        Assert.assertEquals(QueryMode.fromCode('q'), QueryMode.DIRECT);
        Assert.assertEquals(QueryMode.fromCode('?', QueryMode.MUTATION), QueryMode.MUTATION);
    }

    @Test(groups = { "unit" })
    public void testFromPath() {
        Assert.assertNull(QueryMode.fromPath("?", null));
        Assert.assertNull(QueryMode.fromPath("mycsv", null));
        Assert.assertNull(QueryMode.fromPath("my.csv", null));

        Assert.assertEquals(QueryMode.fromPath("async", null), QueryMode.ASYNC);
        Assert.assertEquals(QueryMode.fromPath("async", QueryMode.DIRECT), QueryMode.ASYNC);

        Assert.assertEquals(QueryMode.fromPath("?"), QueryMode.SUBMIT);
        Assert.assertEquals(QueryMode.fromPath("query"), QueryMode.DIRECT);
        Assert.assertEquals(QueryMode.fromPath("batch"), QueryMode.BATCH);
        Assert.assertEquals(QueryMode.fromPath("?", QueryMode.MUTATION), QueryMode.MUTATION);
    }

    @Test(groups = { "unit" })
    public void testOf() {
        Assert.assertEquals(QueryMode.of(null), QueryMode.SUBMIT);
        Assert.assertEquals(QueryMode.of(""), QueryMode.SUBMIT);
        for (QueryMode m : QueryMode.values()) {
            final String str = new String(new char[] { m.code() });
            Assert.assertEquals(QueryMode.of(str), m);
            Assert.assertEquals(QueryMode.of(str.toLowerCase(Locale.ROOT)), m);
            Assert.assertEquals(QueryMode.of(str.toUpperCase(Locale.ROOT)), m);
            Assert.assertEquals(QueryMode.of(m.path()), m);
            Assert.assertEquals(QueryMode.of(m.name()), m);
        }

        Assert.assertThrows(IllegalArgumentException.class, () -> QueryMode.of(" "));
        Assert.assertThrows(IllegalArgumentException.class, () -> QueryMode.of("123"));
    }
}
