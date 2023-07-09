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
package io.github.jdbcx.shell;

import java.sql.Connection;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import io.github.jdbcx.CommandLine;
import io.github.jdbcx.ConnectionListener;
import io.github.jdbcx.DriverExtension;
import io.github.jdbcx.Option;

public class ShellDriverExtension implements DriverExtension {
    private static final List<Option> options = Collections
            .unmodifiableList(Arrays.asList(ShellConnectionListener.OPTION_PATH, CommandLine.OPTION_CLI_ERROR,
                    ShellConnectionListener.OPTION_TIMEOUT, ShellConnectionListener.OPTION_TEST_ARGS,
                    CommandLine.OPTION_INPUT_CHARSET, CommandLine.OPTION_OUTPUT_CHARSET));

    @Override
    public List<Option> getOptions(Properties props) {
        return options;
    }

    @Override
    public ConnectionListener createListener(Connection conn, String url, Properties props) {
        return new ShellConnectionListener(getConfig(props));
    }
}
