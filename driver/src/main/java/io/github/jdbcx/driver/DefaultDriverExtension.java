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
package io.github.jdbcx.driver;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import io.github.jdbcx.DriverExtension;
import io.github.jdbcx.Option;
import io.github.jdbcx.executor.CommandLineExecutor;

public final class DefaultDriverExtension implements DriverExtension {
    private static final List<Option> options = Collections.unmodifiableList(
            Arrays.asList(Option.SERVER_URL, Option.CONFIG_PATH, Option.CUSTOM_CLASSPATH,
                    CommandLineExecutor.OPTION_DOCKER_PATH, Option.PROXY, Option.TAG));

    private static final DriverExtension instance = new DefaultDriverExtension();

    public static final DriverExtension getInstance() {
        return instance;
    }

    @Override
    public List<Option> getDefaultOptions() {
        return options;
    }

    private DefaultDriverExtension() {
    }
}
