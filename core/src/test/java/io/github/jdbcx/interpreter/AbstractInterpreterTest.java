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
package io.github.jdbcx.interpreter;

import java.io.StringReader;
import java.util.Properties;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.github.jdbcx.Constants;
import io.github.jdbcx.Interpreter;
import io.github.jdbcx.Option;
import io.github.jdbcx.QueryContext;
import io.github.jdbcx.Result;
import io.github.jdbcx.Row;

public class AbstractInterpreterTest {
    static class FakeResource implements AutoCloseable {
        final AtomicInteger counter = new AtomicInteger(0);
        final AtomicBoolean state = new AtomicBoolean(false);

        FakeResource reset() {
            counter.set(0);
            state.set(false);
            return this;
        }

        @Override
        public void close() {
            counter.incrementAndGet();
            state.compareAndSet(false, true);
        }
    }

    static class TestInterpreter extends AbstractInterpreter {
        TestInterpreter(QueryContext context) {
            super(context);
        }

        @Override
        public Result<String> interpret(String query, Properties props) {
            return Result.of(query);
        }
    }

    @Test(groups = { "unit" })
    public void testConstructor() {
        try (QueryContext context = QueryContext.newContext()) {
            Interpreter i = new TestInterpreter(context);
            Assert.assertEquals(i.getContext(), context);
            Assert.assertEquals(i.interpret("a", null).get(), "a");
            for (Row row : i.interpret("a", null).rows()) {
                Assert.assertEquals(row.size(), 1);
                Assert.assertEquals(row.value(0).asString(), "a");
            }
            Assert.assertEquals(i.interpret("a", null).get(), "a");
        }
    }

    @Test(groups = { "unit" })
    public void testHandleError() {
        final TestInterpreter i = new TestInterpreter(QueryContext.newContext());
        Assert.assertThrows(NullPointerException.class, () -> i.handleError(null, null, null, null));
        Assert.assertThrows(CompletionException.class, () -> i.handleError(new Exception(), null, null, null));

        Properties props = new Properties();
        Option.EXEC_ERROR.setValue(props, "ignore");
        Assert.assertEquals(i.handleError(new Exception(), null, props, null).get(), "");
        Assert.assertEquals(i.handleError(new Exception(), "", props, null).get(), "");
        Assert.assertEquals(i.handleError(new Exception(), "my query", props, null).get(), "my query");

        FakeResource res1 = new FakeResource();
        FakeResource res2 = new FakeResource();
        Assert.assertEquals(i.handleError(new Exception(), null, props, res1).get(), "");
        Assert.assertEquals(res1.counter.get(), 1);
        Assert.assertEquals(res1.state.get(), true);
        Assert.assertEquals(i.handleError(new Exception(), "", props, res1.reset(), res2.reset()).get(), "");
        Assert.assertEquals(res1.counter.get(), 1);
        Assert.assertEquals(res1.state.get(), true);
        Assert.assertEquals(res1.counter.get(), 1);
        Assert.assertEquals(res1.state.get(), true);
        Assert.assertEquals(i.handleError(new Exception(), "my query", props, res1.reset(), res2.reset()).get(),
                "my query");
        Assert.assertEquals(res1.counter.get(), 1);
        Assert.assertEquals(res1.state.get(), true);
        Assert.assertEquals(res1.counter.get(), 1);
        Assert.assertEquals(res1.state.get(), true);

        Option.EXEC_ERROR.setValue(props, "return");
        Assert.assertEquals(i.handleError(new Exception("1"), null, props, null).get(), "1");
        Assert.assertEquals(i.handleError(new Exception("2"), "", props, null).get(), "2");
        Assert.assertEquals(i.handleError(new Exception("3"), "my query", props, null).get(), "3");
    }

    @Test(groups = { "unit" })
    public void testProcessJsonResult() {
        final TestInterpreter i = new TestInterpreter(QueryContext.newContext());
        Properties props = new Properties();
        Option.RESULT_JSON_PATH.setValue(props, "value");
        int count = 0;
        Result<?> result = i.process("test", new StringReader("{}"), props);
        for (Row r : result.rows()) {
            Assert.assertEquals(r.size(), 1);
            Assert.assertEquals(r.value(0).asString(), "");
            count++;
        }
        Assert.assertEquals(count, 1);
        Assert.assertEquals(result.get(String.class), "");

        Option.RESULT_STRING_TRIM.setValue(props, Constants.TRUE_EXPR);
        count = 0;
        result = i.process("test", new StringReader("{\"value\": [\" 1\", \"2 \", \"\"]}"), props);
        for (Row r : result.rows()) {
            Assert.assertEquals(r.size(), 1);
            Assert.assertEquals(r.value(0).asInt(), ++count);
        }
        Assert.assertEquals(count, 2);
        Assert.assertEquals(result.get(String.class), "1\n2");
    }
}
