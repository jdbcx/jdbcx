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
package io.github.jdbcx.logging;

import java.util.Collections;
import org.testng.annotations.Test;

import io.github.jdbcx.Logger;
import io.github.jdbcx.LoggerTest;

public class JdkLoggerTest extends LoggerTest {
    private final JdkLoggerFactory factory = new JdkLoggerFactory();

    @Override
    protected Logger getLogger(Class<?> clazz) {
        return factory.get(clazz);
    }

    @Override
    protected Logger getLogger(String name) {
        return factory.get(name);
    }

    @Test(groups = { "unit" })
    public void testInstantiation() {
        checkInstance(JdkLogger.class);
    }

    @Test(groups = { "unit" })
    public void testLogMessage() {
        logMessage(Collections.singletonMap("key", "value"));
    }

    @Test(groups = { "unit" })
    public void testLogWithFormat() {
        logWithFormat("msg %s %s %s %s", 1, 2.2, "3", new Object());
    }

    @Test(groups = { "unit" })
    public void testLogWithFunction() {
        logWithFunction(() -> Collections.singleton(1));
    }

    @Test(groups = { "unit" })
    public void testLogThrowable() {
        logThrowable("msg", new Exception("test exception"));
    }

    @Test(groups = { "unit" })
    public void testLogWithFormatAndThrowable() {
        logWithFormatAndThrowable("msg %s %s %s %s", 1, 2.2, "3", new Object(), new Exception("test exception"));
    }
}
