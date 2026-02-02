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
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

public class QueryTaskTest {
    @Test(groups = { "unit" })
    public void testOfQuery() {
        Assert.assertEquals(QueryTask.ofQuery(null), Collections.emptyList());
        Assert.assertEquals(QueryTask.ofQuery(""), Collections.emptyList());

        final String select1 = "select 1";
        Assert.assertEquals(QueryTask.ofQuery(select1), Collections
                .singletonList(new QueryTask("", Collections.singletonList(new QueryGroup(0, null, select1)))));
        Assert.assertEquals(QueryTask.ofQuery("--;; q1\n" + select1), Collections
                .singletonList(new QueryTask("", Collections.singletonList(new QueryGroup(0, "q1", select1)))));
        Assert.assertEquals(QueryTask.ofQuery("--;; q1\n" + select1 + "\n--;; q2\n" + select1),
                Arrays.asList(new QueryTask("",
                        Collections.singletonList(new QueryGroup(0, "q1", select1))),
                        new QueryTask("", Collections.singletonList(new QueryGroup(1, "q2", select1)))));
    }

    @Test(groups = { "unit" })
    public void testOfFile() {
        Assert.assertEquals(QueryTask.ofFile(null, null), Collections.emptyList());
        Assert.assertEquals(QueryTask.ofFile("", null), Collections.emptyList());

        Assert.assertEquals(QueryTask.ofFile("target/test-classes/queries/empty_query.sql", null),
                Collections.emptyList());
        List<QueryTask> list = QueryTask.ofFile("target/test-classes/queries/select_7.sql", null);
        Assert.assertEquals(list, Collections
                .singletonList(new QueryTask(list.get(0).getSource(),
                        Collections.singletonList(new QueryGroup(0, null, "select 7")))));
        list = QueryTask.ofFile("target/test-classes/queries/two_queries.sql", null);
        Assert.assertEquals(list, Arrays.asList(new QueryTask(list.get(0).getSource(),
                Collections.singletonList(new QueryGroup(0, "query #1", "select 1"))),
                new QueryTask(list.get(0).getSource(),
                        Collections.singletonList(new QueryGroup(1, "query #2", "select 2")))));
    }

    @Test(groups = { "unit" })
    public void testOfPath() {
        Assert.assertEquals(QueryTask.ofPath(null, null), Collections.emptyList());
        Assert.assertEquals(QueryTask.ofPath("", null), Collections.emptyList());

        Assert.assertEquals(QueryTask.ofPath("target/test-classes/queries/empty_query.?ql", null),
                Collections.emptyList());
        List<QueryTask> list = QueryTask.ofPath("target/test-classes/queries/select_7.*", null);
        Assert.assertEquals(list, Collections
                .singletonList(new QueryTask(list.get(0).getSource(),
                        Collections.singletonList(new QueryGroup(0, null, "select 7")))));
        list = QueryTask.ofPath("target/test-classes/queries/two_querie?.*", null);
        Assert.assertEquals(list, Arrays.asList(new QueryTask(list.get(0).getSource(),
                Collections.singletonList(new QueryGroup(0, "query #1", "select 1"))),
                new QueryTask(list.get(0).getSource(),
                        Collections.singletonList(new QueryGroup(1, "query #2", "select 2")))));
    }
}
