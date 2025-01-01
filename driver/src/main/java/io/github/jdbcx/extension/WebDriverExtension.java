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
package io.github.jdbcx.extension;

import java.sql.Connection;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import io.github.jdbcx.ConfigManager;
import io.github.jdbcx.DriverExtension;
import io.github.jdbcx.JdbcActivityListener;
import io.github.jdbcx.Option;
import io.github.jdbcx.QueryContext;
import io.github.jdbcx.interpreter.WebInterpreter;

public class WebDriverExtension implements DriverExtension {
    static class ActivityListener extends AbstractActivityListener {
        ActivityListener(QueryContext context, Properties config) {
            super(new WebInterpreter(context, config), config);
        }
    }

    @Override
    public List<String> getAliases() {
        return Collections.unmodifiableList(Arrays.asList("http", "https", "rest", "graphql", "gql"));
    }

    @Override
    public List<Option> getDefaultOptions() {
        return WebInterpreter.OPTIONS;
    }

    @Override
    public JdbcActivityListener createListener(QueryContext context, Connection conn, Properties props) {
        return new ActivityListener(context, getConfig((ConfigManager) context.get(QueryContext.KEY_CONFIG), props));
    }

    @Override
    public String getDescription() {
        return "Extension for accessing resources via http/https.";
    }

    @Override
    public String getUsage() {
        return "{{ web(base.url='https://explorer@play.clickhouse.com'): select 5 }}";
    }
}
