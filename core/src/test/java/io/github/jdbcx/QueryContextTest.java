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
package io.github.jdbcx;

import java.util.Properties;
import java.util.UUID;

import org.testng.Assert;
import org.testng.annotations.Test;

public class QueryContextTest {
    @Test(groups = { "unit" })
    public void testConstructor() {
        try (QueryContext context = QueryContext.newContextForThread()) {
            Assert.assertEquals(context.getScope(), Constants.SCOPE_THREAD);
            Assert.assertEquals(context.getParent().getScope(), Constants.SCOPE_GLOBAL);
        }

        try (QueryContext context = QueryContext.newContext()) {
            Assert.assertEquals(context.getScope(), Constants.SCOPE_QUERY);
            Assert.assertEquals(context.getParent().getScope(), Constants.SCOPE_GLOBAL);
        }
    }

    @Test(groups = { "unit" })
    public void testVariable() {
        final String uuid = UUID.randomUUID().toString();
        try (QueryContext context = QueryContext.newContextForThread(new QueryContext(Constants.SCOPE_GLOBAL, null))) {
            Assert.assertEquals(context.getVariable(uuid), null);
            Assert.assertEquals(context.getVariable(uuid, "x"), "x");
            context.setVariable(uuid, uuid);
            Assert.assertEquals(context.getVariable(uuid), uuid);
            context.removeVariable(uuid);
            Assert.assertEquals(context.getVariable(uuid), null);
            Assert.assertEquals(context.getVariable(uuid, "x"), "x");

            Assert.assertNull(context.getParent().setVariable(uuid, "1"));
            Assert.assertEquals(context.getVariable(uuid), "1");
            Assert.assertNull(context.setVariable(uuid, "3"));
            Assert.assertEquals(context.setVariable(uuid, "2"), "3");
            Assert.assertEquals(context.getVariable(uuid), "2");
            Assert.assertEquals(context.getVariableInScope(Constants.SCOPE_GLOBAL, uuid), "1");
            Assert.assertEquals(context.getVariableInScope(Constants.SCOPE_THREAD, uuid), "2");
            Assert.assertThrows(IllegalArgumentException.class,
                    () -> context.getVariableInScope(Constants.SCOPE_QUERY, uuid));

            Properties props = new Properties();
            Assert.assertEquals(context.getParent().getVariables(), props);
            props.setProperty(uuid, "1");
            Assert.assertEquals(context.getParent().getMergedVariables(), props);

            props.clear();
            Assert.assertEquals(context.getVariables(), props);
            props.setProperty(uuid, "2");
            Assert.assertEquals(context.getMergedVariables(), props);
        }
    }

    @Test(groups = { "unit" })
    public void testScopedVariable() {
        final String uuid = UUID.randomUUID().toString();
        try (QueryContext context = QueryContext.newContextForThread(new QueryContext(Constants.SCOPE_GLOBAL, null))) {
            Assert.assertNull(context.setVariableInScope(Constants.SCOPE_GLOBAL, uuid, "1"));
            Assert.assertEquals(context.getVariable(uuid), "1");
            Assert.assertEquals(context.setVariableInScope(Constants.SCOPE_GLOBAL, uuid, "2"), "1");
            Assert.assertEquals(context.getVariable(uuid), "2");

            Assert.assertNull(context.setVariableInScope(Constants.SCOPE_THREAD, uuid, "t1"));
            Assert.assertEquals(context.getVariable(uuid), "t1");
            Assert.assertEquals(context.setVariableInScope(Constants.SCOPE_THREAD, uuid, "t2"), "t1");
            Assert.assertEquals(context.getVariable(uuid), "t2");
        }
    }
}
