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
package io.github.jdbcx.data;

import java.util.Arrays;
import java.util.Collections;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.github.jdbcx.DeferredValue;
import io.github.jdbcx.Row;

public class IterableRowTest {
    @Test(groups = { "unit" })
    public void testRead() {
        int index = 0;
        for (Row r : new IterableRow(DefaultRow.defaultFields,
                Arrays.asList(DeferredValue.of(Collections::emptyList)))) {
            index++;
            Assert.fail("Should have no row");
        }
        Assert.assertEquals(index, 0);

        index = 0;
        for (Row r : new IterableRow(DefaultRow.defaultFields,
                Arrays.asList(DeferredValue.of(Collections::emptyList), DeferredValue.of(Collections::emptyList)))) {
            index++;
            Assert.fail("Should have no row");
        }
        Assert.assertEquals(index, 0);

        index = 0;
        for (Row r : new IterableRow(DefaultRow.defaultFields,
                Arrays.asList(DeferredValue.of(() -> Arrays.asList(Row.of(1L)))))) {
            Assert.assertEquals(r.value(0).asLong(), 1L);
            index++;
        }
        Assert.assertEquals(index, 1);

        index = 0;
        for (Row r : new IterableRow(DefaultRow.defaultFields,
                Arrays.asList(DeferredValue.of(Collections::emptyList),
                        DeferredValue.of(() -> Arrays.asList(Row.of(1L), Row.of(2L)))))) {
            Assert.assertEquals(r.value(0).asInt(), ++index);
        }
        Assert.assertEquals(index, 2);

        index = 0;
        for (Row r : new IterableRow(DefaultRow.defaultFields,
                Arrays.asList(DeferredValue.of(Collections::emptyList), DeferredValue.of(Collections::emptyList),
                        DeferredValue.of(() -> Arrays.asList(Row.of(1L), Row.of(2L))),
                        DeferredValue.of(Collections::emptyList)))) {
            Assert.assertEquals(r.value(0).asInt(), ++index);
        }
        Assert.assertEquals(index, 2);
    }
}
