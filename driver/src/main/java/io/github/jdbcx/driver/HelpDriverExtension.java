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
package io.github.jdbcx.driver;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

import io.github.jdbcx.Checker;
import io.github.jdbcx.DriverExtension;
import io.github.jdbcx.Field;
import io.github.jdbcx.JdbcActivityListener;
import io.github.jdbcx.Option;
import io.github.jdbcx.QueryContext;
import io.github.jdbcx.Result;
import io.github.jdbcx.Row;
import io.github.jdbcx.Utils;
import io.github.jdbcx.data.StringValue;

public final class HelpDriverExtension implements DriverExtension {
    static final class ActivityListener implements JdbcActivityListener {
        static final Option OPTION_NAME = Option.of(new String[] { "name", "Extension name or alias" });

        static final DriverInfo defaultDriverInfo = new DriverInfo();

        private static final List<Field> summaryFields = Collections.unmodifiableList(
                Arrays.asList(Field.of("name"), Field.of("alias"), Field.of("description"), Field.of("usage")));

        private final Properties config;

        protected ActivityListener(Properties config) {
            this.config = config;
        }

        @Override
        public Result<?> onQuery(String query) throws SQLException {
            final String nameOrAlias = OPTION_NAME.getValue(config);

            Map<String, DriverExtension> map = defaultDriverInfo.getExtensions();
            if (Checker.isNullOrBlank(nameOrAlias)) {
                Set<DriverExtension> exts = new TreeSet<>(map.values());
                exts.remove(DefaultDriverExtension.getInstance());
                List<Row> rows = new ArrayList<>(exts.size());
                for (DriverExtension ext : exts) {
                    rows.add(Row.of(summaryFields, new StringValue(ext.getName()),
                            new StringValue(String.join(",", ext.getAliases())),
                            new StringValue(ext.getDescription()),
                            new StringValue(ext.getUsage())));
                }
                return Result.of(summaryFields, rows);
            }

            DriverExtension ext = map.get(nameOrAlias);
            if (ext == null) {
                throw new IllegalArgumentException(Utils.format("Name or alias [%s] does not exist", nameOrAlias));
            }

            return ConnectionManager.describe(ext, config);
        }
    }

    @Override
    public List<String> getAliases() {
        return Collections.singletonList("man");
    }

    @Override
    public String getDescription() {
        return "Extension for help.";
    }

    @Override
    public String getUsage() {
        return "{{ help(name=shell) }}";
    }

    @Override
    public JdbcActivityListener createListener(QueryContext context, Connection conn, Properties props) {
        return new ActivityListener(props);
    }

    @Override
    public boolean supportsNoArguments() {
        return true;
    }
}
