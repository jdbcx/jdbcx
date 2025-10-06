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
package io.github.jdbcx.interpreter;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.github.jdbcx.ByteArrayClassLoader;
import io.github.jdbcx.Constants;
import io.github.jdbcx.Option;
import io.github.jdbcx.QueryContext;
import io.github.jdbcx.Result;
import io.github.jdbcx.Stream;

public class ShellInterpreterTets {
    @Test(groups = "unit")
    public void testBinaryOutput() throws IOException, TimeoutException {
        final String cli = (Constants.IS_WINDOWS ? "type" : "cat") + " target/classes/"
                + Constants.class.getName().replace('.', '/') + ".class";
        Properties config = new Properties();
        Properties props = new Properties();
        try (QueryContext context = QueryContext.newContext()) {
            ShellInterpreter interpreter = new ShellInterpreter(context, config);
            try (Result<?> result = interpreter.interpret(cli, props)) {
                Assert.assertEquals(result.type(), String.class);
            }
        }

        Option.RESULT_TYPE.setValue(config, Option.TYPE_BINARY);
        try (QueryContext context = QueryContext.newContext()) {
            ShellInterpreter interpreter = new ShellInterpreter(context, config);
            try (Result<?> result = interpreter.interpret(cli, props)) {
                Assert.assertEquals(result.type(), ByteArrayInputStream.class);
                ByteArrayClassLoader loader = new ByteArrayClassLoader();
                Class<?> clazz = loader.loadClassFromBytes(Constants.class.getName(),
                        Stream.readAllBytes((InputStream) result.get()));
                Assert.assertEquals(clazz.getName(), Constants.class.getName());
            }
        }

        Option.RESULT_TYPE.setValue(props, Option.TYPE_AUTO);
        try (QueryContext context = QueryContext.newContext()) {
            ShellInterpreter interpreter = new ShellInterpreter(context, config);
            try (Result<?> result = interpreter.interpret(cli, props)) {
                Assert.assertEquals(result.type(), String.class);
            }
        }

        Option.RESULT_TYPE.setValue(props, Option.TYPE_BINARY);
        try (QueryContext context = QueryContext.newContext()) {
            ShellInterpreter interpreter = new ShellInterpreter(context, config);
            try (Result<?> result = interpreter.interpret(cli, props)) {
                Assert.assertEquals(result.type(), ByteArrayInputStream.class);
                ByteArrayClassLoader loader = new ByteArrayClassLoader();
                Class<?> clazz = loader.loadClassFromBytes(Constants.class.getName(),
                        Stream.readAllBytes((InputStream) result.get()));
                Assert.assertEquals(clazz.getName(), Constants.class.getName());
            }
        }

        config.clear();
        try (QueryContext context = QueryContext.newContext()) {
            ShellInterpreter interpreter = new ShellInterpreter(context, config);
            try (Result<?> result = interpreter.interpret(cli, props)) {
                Assert.assertEquals(result.type(), ByteArrayInputStream.class);
                ByteArrayClassLoader loader = new ByteArrayClassLoader();
                Class<?> clazz = loader.loadClassFromBytes(Constants.class.getName(),
                        Stream.readAllBytes((InputStream) result.get()));
                Assert.assertEquals(clazz.getName(), Constants.class.getName());
            }
        }
    }
}
