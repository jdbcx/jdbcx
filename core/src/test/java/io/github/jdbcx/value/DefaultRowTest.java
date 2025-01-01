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
package io.github.jdbcx.value;

import java.util.Arrays;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.github.jdbcx.Field;
import io.github.jdbcx.data.DefaultRow;

public class DefaultRowTest {
    @Test(groups = { "unit" })
    public void testConstructor() {
        Assert.assertEquals(DefaultRow.EMPTY.size(), new DefaultRow().size());

        List<Field> fields = Arrays.asList(Field.of("a"), Field.of("b"));
        Assert.assertEquals(DefaultRow.EMPTY.size(), new DefaultRow(fields).size());

        DefaultRow row = new DefaultRow(fields, new StringValue(null, true, 0, "a1"));
        Assert.assertEquals(row.size(), 1);
        row = new DefaultRow(fields, new StringValue(null, true, 0, "a1"), new StringValue(null, true, 0, "b1"));
        Assert.assertEquals(row.size(), 2);
        row = new DefaultRow(fields, new StringValue(null, true, 0, "a1"), new StringValue(null, true, 0, "b1"),
                new StringValue(null, true, 0, "3"));
        Assert.assertEquals(row.size(), 2);
    }

    @Test(groups = { "unit" })
    public void testGetFieldAndValue() {
        List<Field> fields = Arrays.asList(Field.of("a"), Field.of("b"));
        DefaultRow row = new DefaultRow(fields, new StringValue(null, true, 0, "a1"));
        Assert.assertEquals(row.size(), 1);
        Assert.assertEquals(row.field(0), fields.get(0));
        Assert.assertEquals(row.field(1), fields.get(1));
        Assert.assertEquals(row.value(0).asString(), "a1");
        Assert.assertThrows(IndexOutOfBoundsException.class, () -> row.value(1));
    }

    @Test(groups = { "unit" })
    public void testIndex() {
        List<Field> fields = Arrays.asList(Field.of("aa"), Field.of("bb"));
        DefaultRow row = new DefaultRow(fields, new StringValue(null, true, 0, "a1"),
                new StringValue(null, true, 0, "b1"), new StringValue(null, true, 0, "c"));
        Assert.assertEquals(row.size(), 2);
        Assert.assertEquals(row.index("bB"), 1);
        Assert.assertEquals(row.index("Aa"), 0);
        Assert.assertEquals(row.index("c"), -1);
        Assert.assertEquals(row.value(2).asString(), "c");
    }
}
