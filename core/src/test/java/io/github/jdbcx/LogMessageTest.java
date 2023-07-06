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
package io.github.jdbcx;

import org.testng.Assert;
import org.testng.annotations.Test;

public class LogMessageTest {
    @Test(groups = { "unit" })
    public void testMessageWithNoArgument() {
        String message = "test %s";
        LogMessage msg = LogMessage.of(message);
        Assert.assertEquals(message, msg.getMessage());
        Assert.assertNull(msg.getThrowable());

        msg = LogMessage.of(1);
        Assert.assertEquals("1", msg.getMessage());
        Assert.assertNull(msg.getThrowable());
    }

    @Test(groups = { "unit" })
    public void testMessageWithArguments() {
        LogMessage msg = LogMessage.of("test %s - %s", "test", 1);
        Assert.assertEquals("test test - 1", msg.getMessage());
        Assert.assertNull(msg.getThrowable());

        msg = LogMessage.of("test", "test", 1);
        Assert.assertEquals("test", msg.getMessage());
        Assert.assertNull(msg.getThrowable());
    }

    @Test(groups = { "unit" })
    public void testMessageWithThrowable() {
        Throwable t = new Exception();
        LogMessage msg = LogMessage.of("test", t);
        Assert.assertEquals("test", msg.getMessage());
        Assert.assertEquals(t, msg.getThrowable());

        msg = LogMessage.of("test %s", 1, t);
        Assert.assertEquals("test 1", msg.getMessage());
        Assert.assertEquals(t, msg.getThrowable());

        msg = LogMessage.of("test %d %s", 1, t);
        Assert.assertEquals("test 1 java.lang.Exception", msg.getMessage());
        Assert.assertEquals(t, msg.getThrowable());

        msg = LogMessage.of("test %d %s", 1, t, null);
        Assert.assertEquals("test 1 java.lang.Exception", msg.getMessage());
        Assert.assertEquals(msg.getThrowable(), null);
    }
}
