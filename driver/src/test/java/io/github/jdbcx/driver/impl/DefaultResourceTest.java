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
package io.github.jdbcx.driver.impl;

import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.Arrays;
import java.util.Collections;

import org.testng.Assert;
import org.testng.annotations.Test;

public class DefaultResourceTest {
    static final class TestResource extends DefaultResource {
        protected TestResource(DefaultResource parent) {
            super(parent);
        }
    }

    static final class CustomResource extends DefaultResource {
        protected CustomResource(DefaultResource parent) {
            super(parent);
        }
    }

    @Test(groups = { "unit" })
    public void testConstructor() throws SQLException {
        try (DefaultResource resource = new TestResource(null)) {
            Assert.assertEquals(resource.parent, null);
            Assert.assertFalse(resource.closed.get(), "Should have not closed");
            Assert.assertNull(resource.warning.get(), "Should not have any warning");
            Assert.assertEquals(resource.children, Collections.emptyList());
            Assert.assertFalse(resource.isClosed(), "Should have not closed");
            Assert.assertNull(resource.getWarnings(), "Should not have any warning");

            try (DefaultResource child = new TestResource(resource)) {
                Assert.assertEquals(child.parent, resource);
                Assert.assertEquals(resource.children, Collections.singletonList(child));
            }
            Assert.assertFalse(resource.isClosed(), "Should have not closed");
        }
    }

    @Test(groups = { "unit" })
    public void testOperations() throws SQLException {
        try (DefaultResource resource = new TestResource(null);
                DefaultResource child = new TestResource(resource);
                DefaultResource test1 = new CustomResource(null);
                DefaultResource test2 = new TestResource(test1)) {
            Assert.assertEquals(resource.children, Collections.singletonList(child));
            Assert.assertEquals(child.parent, resource);
            Assert.assertEquals(test1.children, Collections.singletonList(test2));
            Assert.assertEquals(test2.parent, test1);
            Assert.assertEquals(resource.get(DefaultResource.class), child);
            Assert.assertEquals(resource.get(TestResource.class), child);
            Assert.assertEquals(resource.get(CustomResource.class), null);

            Assert.assertEquals(resource.add(test1), test1);
            Assert.assertEquals(resource.children, Arrays.asList(child, test1));
            Assert.assertEquals(resource.get(DefaultResource.class), child);
            Assert.assertEquals(resource.get(AutoCloseable.class, r -> r == child), child);
            Assert.assertEquals(resource.get(TestResource.class), child);
            Assert.assertEquals(resource.get(CustomResource.class), test1);
            Assert.assertEquals(resource.get(AutoCloseable.class, r -> r == test1), test1);
            Assert.assertEquals(resource.get(AutoCloseable.class, r -> r == resource), null);

            Assert.assertTrue(resource.remove(child), "Should have removed successfully");
            Assert.assertEquals(resource.children, Collections.singletonList(test1));
            Assert.assertEquals(resource.get(DefaultResource.class), test1);
            Assert.assertEquals(resource.get(TestResource.class), null);
            Assert.assertEquals(resource.get(CustomResource.class), test1);
        }
    }

    @Test(groups = { "unit" })
    public void testClose() throws SQLException {
        try (DefaultResource resource = new TestResource(null);
                DefaultResource child = new TestResource(resource);
                DefaultResource test1 = new TestResource(child);
                DefaultResource test2 = new TestResource(null)) {
            Assert.assertEquals(resource.children, Collections.singletonList(child));
            Assert.assertEquals(child.parent, resource);
            Assert.assertEquals(child.children, Collections.singletonList(test1));
            Assert.assertEquals(test1.parent, child);
            Assert.assertEquals(resource.add(test2), test2);
            resource.close();
            Assert.assertEquals(resource.children, Collections.emptyList());
            Assert.assertEquals(child.children, Collections.emptyList());
            Assert.assertTrue(test2.isClosed(), "Should have been closed");
            Assert.assertTrue(test1.isClosed(), "Should have been closed");
            Assert.assertTrue(child.isClosed(), "Should have been closed");
            Assert.assertTrue(resource.isClosed(), "Should have been closed");
        }
    }

    @Test(groups = { "unit" })
    public void testReset() throws SQLException {
        try (DefaultResource resource = new TestResource(null);
                DefaultResource child = new TestResource(resource);
                DefaultResource test = new TestResource(null)) {
            Assert.assertEquals(resource.children, Collections.singletonList(child));
            resource.add(test);
            Assert.assertEquals(resource.children, Arrays.asList(child, test));

            child.add(test);
            Assert.assertEquals(child.children, Collections.singletonList(test));

            final SQLWarning testWarning = new SQLWarning("Test warning");
            test.warning.set(testWarning);
            Assert.assertEquals(test.getWarnings(), testWarning);
            Assert.assertFalse(test.isClosed(), "Should have not closed");
            test.reset();
            Assert.assertNull(test.getWarnings(), "Should not have any warning");
            Assert.assertFalse(test.isClosed(), "Should have not closed");

            child.warning.set(testWarning);
            test.warning.set(testWarning);
            Assert.assertEquals(child.getWarnings(), testWarning);
            Assert.assertFalse(child.isClosed(), "Should have not closed");
            child.reset();
            Assert.assertEquals(child.children, Collections.emptyList());
            Assert.assertNull(child.getWarnings(), "Should not have any warning");
            Assert.assertFalse(child.isClosed(), "Should have not closed");
            Assert.assertTrue(test.isClosed(), "Should have been closed");
            Assert.assertNull(test.warning.get(), "Should not have any warning");
            Assert.assertThrows(SQLException.class, test::getWarnings);

            resource.warning.set(testWarning);
            child.warning.set(testWarning);
            test.warning.set(testWarning);
            test.closed.set(false);
            Assert.assertEquals(resource.getWarnings(), testWarning);
            Assert.assertEquals(child.getWarnings(), testWarning);
            Assert.assertEquals(test.getWarnings(), testWarning);
            Assert.assertFalse(resource.isClosed(), "Should have not closed");
            Assert.assertFalse(child.isClosed(), "Should have not closed");
            Assert.assertFalse(test.isClosed(), "Should have not closed");
            resource.reset();
            Assert.assertEquals(resource.children, Collections.emptyList());
            Assert.assertNull(resource.getWarnings(), "Should not have any warning");
            Assert.assertNull(child.warning.get(), "Should not have any warning");
            Assert.assertNull(test.warning.get(), "Should not have any warning");
            Assert.assertThrows(SQLException.class, child::getWarnings);
            Assert.assertThrows(SQLException.class, test::getWarnings);
            Assert.assertFalse(resource.isClosed(), "Should have not closed");
            Assert.assertTrue(test.isClosed(), "Should have been closed");
            Assert.assertTrue(test.isClosed(), "Should have been closed");
        }
    }
}
