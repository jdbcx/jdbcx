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

import java.util.Arrays;
import java.util.Collections;

import org.testng.Assert;
import org.testng.annotations.Test;

public class QueryGroupTest {
    @Test(groups = { "unit" })
    public void testConstructor() {
        QueryGroup group = new QueryGroup(-1, null, null);
        Assert.assertEquals(group.getIndex(), 0);
        Assert.assertEquals(group.getDescription(), "");
        Assert.assertEquals(group.getQuery(), "");
        Assert.assertEquals(group, new QueryGroup(0, "", ""));
    }

    @Test(groups = { "unit" })
    public void testEquals() {
        QueryGroup group = new QueryGroup(7, "q2", "select 5");
        Assert.assertEquals(group, new QueryGroup(7, new StringBuilder().append('q').append(2).toString(),
                new StringBuilder().append("select ").append(5).toString()));

        Assert.assertNotEquals(group, new QueryGroup(6, "q2", "select 5"));
        Assert.assertNotEquals(group, new QueryGroup(7, "q1", "select 5"));
        Assert.assertNotEquals(group, new QueryGroup(7, "q2", "select 2"));
    }

    @Test(groups = { "unit" })
    public void testSplit() {
        Assert.assertEquals(QueryGroup.of(null), Collections.emptyList());
        Assert.assertEquals(QueryGroup.of(""), Collections.emptyList());

        Assert.assertEquals(QueryGroup.of("select 1"), Collections.singletonList(new QueryGroup(0, null, "select 1")));
        Assert.assertEquals(QueryGroup.of("select 1;\nselect 2\n"),
                Collections.singletonList(new QueryGroup(0, null, "select 1;\nselect 2")));

        Assert.assertEquals(QueryGroup.of("--;; q1\nselect 1\n--;; q2\nselect 2"),
                Arrays.asList(new QueryGroup(0, "q1", "select 1"), new QueryGroup(1, "q2", "select 2")));
        Assert.assertEquals(QueryGroup.of("select 1\n--;; q2\nselect 2"),
                Arrays.asList(new QueryGroup(0, "", "select 1"), new QueryGroup(1, "q2", "select 2")));
    }
}
