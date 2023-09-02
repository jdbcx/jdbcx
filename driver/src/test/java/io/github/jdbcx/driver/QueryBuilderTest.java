/*
 * Copyright 2022-2023, Zhichun Wu
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

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.github.jdbcx.QueryContext;
import io.github.jdbcx.WrappedDriver;

public class QueryBuilderTest {
    @Test(groups = { "unit" })
    public void testBuild() throws SQLException {
        String url = "jdbcx:derby:memory:x;create=true";
        Properties props = new Properties();
        try (WrappedConnection conn = (WrappedConnection) new WrappedDriver().connect(url, props);
                WrappedStatement stmt = conn.createStatement()) {
            Assert.assertTrue(conn instanceof WrappedConnection, "Should return wrapped connection");
            try (WrappedConnection c = (WrappedConnection) conn; QueryContext context = QueryContext.newContext()) {
                ParsedQuery pq = QueryParser.parse("select {{ rhino: ['1','2','3']}}", props);
                QueryBuilder builder = new QueryBuilder(context, pq, conn.manager, stmt.queryResult);
                Assert.assertEquals(builder.build(), Arrays.asList("select 1", "select 2", "select 3"));
            }

            try (WrappedConnection c = (WrappedConnection) conn; QueryContext context = QueryContext.newContext()) {
                ParsedQuery pq = QueryParser
                        .parse("select {{ rhino: ['1','2','3']}} + {{ rhino: ['4', '5']}}", props);
                QueryBuilder builder = new QueryBuilder(context, pq, conn.manager, stmt.queryResult);
                Assert.assertEquals(builder.build(), Arrays.asList("select 1 + 4", "select 2 + 4", "select 3 + 4",
                        "select 1 + 5", "select 2 + 5", "select 3 + 5"));
            }

            try (WrappedConnection c = (WrappedConnection) conn; QueryContext context = QueryContext.newContext()) {
                ParsedQuery pq = QueryParser
                        .parse("select {{ rhino: ['1','2','3']}} + {{ rhino: ['4', '5']}} as {{ rhino: 'a' }}",
                                props);
                QueryBuilder builder = new QueryBuilder(context, pq, conn.manager, stmt.queryResult);
                Assert.assertEquals(builder.build(),
                        Arrays.asList("select 1 + 4 as a", "select 2 + 4 as a", "select 3 + 4 as a",
                                "select 1 + 5 as a", "select 2 + 5 as a", "select 3 + 5 as a"));
            }
        }
    }
}
