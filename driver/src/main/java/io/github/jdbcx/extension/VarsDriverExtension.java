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
package io.github.jdbcx.extension;

import java.sql.Connection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import io.github.jdbcx.DriverExtension;
import io.github.jdbcx.JdbcActivityListener;
import io.github.jdbcx.QueryContext;
import io.github.jdbcx.interpreter.VarsInterpreter;

public class VarsDriverExtension implements DriverExtension {
    static final class ActivityListener extends AbstractActivityListener {
        ActivityListener(QueryContext context, Properties config) {
            super(new VarsInterpreter(context, config), config);
        }
    }

    @Override
    public List<String> getAliases() {
        return Collections.singletonList("var");
    }

    @Override
    public JdbcActivityListener createListener(QueryContext context, Connection conn, Properties props) {
        return new ActivityListener(context, getConfig(props));
    }
}
