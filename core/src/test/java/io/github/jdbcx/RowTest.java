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

import java.util.Arrays;
import java.util.Collections;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.github.jdbcx.data.LongValue;
import io.github.jdbcx.data.StringValue;

public class RowTest {
    @Test(groups = "unit")
    public void testConstructor() {
        Row r = Row.of((String) null);
        Assert.assertEquals(r.values().size(), 1);
        Assert.assertEquals(r.value(0).asString(), "");

        r = Row.of((Long) null);
        Assert.assertEquals(r.values().size(), 1);
        Assert.assertEquals(r.value(0).asString(), "");

        r = Row.of(Collections.emptyList(), new String[] { "2", "1" });
        Assert.assertEquals(r.size(), 0);
        Assert.assertEquals(r.value(0).asString(), "2");
        Assert.assertEquals(r.value(1).asString(), "1");
        Assert.assertEquals(r.values().size(), 2);

        r = Row.of(Collections.emptyList(), Arrays.asList(1, 2));
        Assert.assertEquals(r.size(), 0);
        Assert.assertEquals(r.value(0).asString(), "1");
        Assert.assertEquals(r.value(1).asString(), "2");
        Assert.assertEquals(r.values().size(), 2);

        r = Row.of(Collections.singletonList(Field.of("name")), new StringValue("3"));
        Assert.assertEquals(r.size(), 1);
        Assert.assertEquals(r.value(0).asString(), "3");
        Assert.assertEquals(r.values().size(), 1);

        r = Row.of(Collections.singletonList(Field.of("name")), new StringValue("3"), new LongValue(4L));
        Assert.assertEquals(r.size(), 1);
        Assert.assertEquals(r.value(0).asString(), "3");
        Assert.assertEquals(r.value(1).asString(), "4");
        Assert.assertEquals(r.values().size(), 2);
    }
}
