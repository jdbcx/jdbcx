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
package io.github.jdbcx.prql;

import java.sql.Connection;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import io.github.jdbcx.CommandLine;
import io.github.jdbcx.ConnectionListener;
import io.github.jdbcx.DriverExtension;
import io.github.jdbcx.Option;

public class PrqlDriverExtension implements DriverExtension {
    private static final List<Option> options = Collections
            .unmodifiableList(Arrays.asList(PrqlConnectionListener.OPTION_COMPILE_ERROR,
                    PrqlConnectionListener.OPTION_COMPILE_TARGET,
                    CommandLine.OPTION_CLI_PATH.update().defaultValue(PrqlConnectionListener.DEFAULT_COMMAND).build(),
                    CommandLine.OPTION_CLI_TIMEOUT.update().defaultValue("5000").build(),
                    CommandLine.OPTION_CLI_TEST_ARGS.update().defaultValue("-V").build(),
                    CommandLine.OPTION_INPUT_CHARSET, CommandLine.OPTION_OUTPUT_CHARSET));

    @Override
    public List<Option> getOptions(Properties props) {
        return options;
    }

    @Override
    public ConnectionListener createListener(Connection conn, String url, Properties props) {
        return new PrqlConnectionListener(getConfig(props));
    }
}
