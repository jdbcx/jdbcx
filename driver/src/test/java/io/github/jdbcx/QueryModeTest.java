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

import java.util.Locale;

import org.testng.Assert;
import org.testng.annotations.Test;

public class QueryModeTest {
    @Test(groups = { "unit" })
    public void testConstructor() {
        Assert.assertEquals(QueryMode.of(null), QueryMode.SUBMIT);
        Assert.assertEquals(QueryMode.of(""), QueryMode.SUBMIT);
        for (QueryMode m : QueryMode.values()) {
            Assert.assertEquals(QueryMode.of(m.code()), m);
            Assert.assertEquals(QueryMode.of(m.name()), m);
            Assert.assertEquals(QueryMode.of(m.name().toLowerCase()), m);
            Assert.assertEquals(QueryMode.of(m.code().toLowerCase(Locale.ROOT)), m);
            Assert.assertEquals(QueryMode.of(m.code().toUpperCase(Locale.ROOT)), m);
        }

        Assert.assertThrows(IllegalArgumentException.class, () -> QueryMode.of(" "));
        Assert.assertThrows(IllegalArgumentException.class, () -> QueryMode.of("?"));
        Assert.assertThrows(IllegalArgumentException.class, () -> QueryMode.of("123"));
    }
}
