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
package io.github.jdbcx.driver;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ParsedQueryTest {
    @Test(groups = { "unit" })
    public void testConstructor() {
        Assert.assertThrows(IllegalArgumentException.class, () -> new ParsedQuery(null, null));
        Assert.assertThrows(IllegalArgumentException.class, () -> new ParsedQuery(Collections.emptyList(), null));
        Assert.assertThrows(IllegalArgumentException.class, () -> new ParsedQuery(null, Collections.emptyList()));

        Assert.assertEquals(new ParsedQuery(Collections.emptyList(), Collections.emptyList()),
                new ParsedQuery(new ArrayList<>(), new ArrayList<>()));
        Assert.assertEquals(new ParsedQuery(Arrays.asList("a", "b", "c"), Collections.emptyList()),
                new ParsedQuery(Arrays.asList("a", "b", "c"), new ArrayList<>()));
        Assert.assertEquals(
                new ParsedQuery(Collections.emptyList(),
                        Collections.singletonList(new ExecutableBlock(1, "2", new Properties(), "3", false))),
                new ParsedQuery(new ArrayList<>(),
                        Arrays.asList(new ExecutableBlock(1, "2", new Properties(), "3", false))));

        Assert.assertEquals(
                new ParsedQuery(Arrays.asList("a", "b", "c"),
                        Collections.singletonList(new ExecutableBlock(1, "2", new Properties(), "3", false))),
                new ParsedQuery(Arrays.asList("a", "b", "c"),
                        Arrays.asList(new ExecutableBlock(1, "2", new Properties(), "3", false))));
    }

    @Test(groups = { "unit" })
    public void testIsDirectQuery() {
        Assert.assertEquals(new ParsedQuery(Collections.emptyList(), Collections.emptyList()).isDirectQuery(), true);
        Assert.assertEquals(new ParsedQuery(new ArrayList<>(), new ArrayList<>()).isDirectQuery(), true);

        Assert.assertEquals(
                new ParsedQuery(Arrays.asList("   ", "", "\r\n\t"),
                        Arrays.asList(new ExecutableBlock(1, "", new Properties(), "", false))).isDirectQuery(),
                true);
        Assert.assertEquals(
                new ParsedQuery(Arrays.asList("   ", "", "\r\n\t", "--", "/*/**/*/", " /*-- test */"),
                        Arrays.asList(new ExecutableBlock(1, "", new Properties(), "", true))).isDirectQuery(),
                true);
    }

    @Test(groups = { "unit" })
    public void testQueryCategory() {
        ParsedQuery query = new ParsedQuery(Arrays.asList("1", "2", "3"),
                Arrays.asList(new ExecutableBlock(0, "db", new Properties(), "select 1", true)));
        Assert.assertEquals(query.isDirectQuery(), false);
        Assert.assertEquals(query.isStaticQuery(), false);

        query = new ParsedQuery(Arrays.asList("--1", "--2", "--3"),
                Arrays.asList(new ExecutableBlock(0, "db", new Properties(), "select 1", true)));
        Assert.assertEquals(query.isDirectQuery(), true);
        Assert.assertEquals(query.isStaticQuery(), false);

        query = new ParsedQuery(Arrays.asList("1", "--2", "--3"),
                Arrays.asList(new ExecutableBlock(0, "db", new Properties(), "select 1", false)));
        Assert.assertEquals(query.isDirectQuery(), false);
        Assert.assertEquals(query.isStaticQuery(), true);

        query = new ParsedQuery(Collections.emptyList(),
                Arrays.asList(new ExecutableBlock(0, "db", new Properties(), "select 1", true)));
        Assert.assertEquals(query.isDirectQuery(), true);
        Assert.assertEquals(query.isStaticQuery(), false);

        query = new ParsedQuery(Collections.emptyList(), Collections.emptyList());
        Assert.assertEquals(query.isDirectQuery(), true);
        Assert.assertEquals(query.isStaticQuery(), true);
    }
}
