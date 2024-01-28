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
package io.github.jdbcx.extension;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import io.github.jdbcx.DriverExtension;
import io.github.jdbcx.JdbcActivityListener;
import io.github.jdbcx.QueryContext;
import io.github.jdbcx.Result;
import io.github.jdbcx.Utils;

public final class VersionDriverExtension implements DriverExtension, JdbcActivityListener {
    private final String version;

    public VersionDriverExtension() {
        this.version = Utils.getVersion();
    }

    @Override
    public List<String> getAliases() {
        return Collections.singletonList("ver");
    }

    @Override
    public JdbcActivityListener createListener(QueryContext context, Connection conn, Properties props) {
        return this;
    }

    @Override
    public String getDescription() {
        return "Extension to get JDBCX version";
    }

    @Override
    public String getUsage() {
        return "{{ version }}";
    }

    @Override
    public boolean supportsNoArguments() {
        return true;
    }

    @Override
    public Result<?> onQuery(String query) throws SQLException {
        return Result.of(version);
    }
}
