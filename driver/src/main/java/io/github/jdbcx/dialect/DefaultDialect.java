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
package io.github.jdbcx.dialect;

import java.sql.SQLException;

import io.github.jdbcx.Field;
import io.github.jdbcx.JdbcDialect;

public final class DefaultDialect implements JdbcDialect {
    private static final JdbcDialect instance = new DefaultDialect();

    public static JdbcDialect getInstance() {
        return instance;
    }

    @Override
    public void createTemporaryTable(String table, Field... fields) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'createTemporaryTable'");
    }

    private DefaultDialect() {
    }
}
