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
package io.github.jdbcx.logging;

import io.github.jdbcx.Logger;
import io.github.jdbcx.LoggerFactory;

/**
 * Adaptor of slf4j logger factory.
 */
public class Slf4jLoggerFactory extends LoggerFactory {
    @Override
    public Logger get(Class<?> clazz) {
        return new Slf4jLogger(org.slf4j.LoggerFactory.getLogger(clazz));
    }

    @Override
    public Logger get(String name) {
        return new Slf4jLogger(org.slf4j.LoggerFactory.getLogger(name));
    }
}
