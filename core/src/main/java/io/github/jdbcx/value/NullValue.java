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
package io.github.jdbcx.value;

import java.nio.charset.Charset;
import java.sql.ResultSet;
import java.sql.SQLException;

import io.github.jdbcx.Constants;
import io.github.jdbcx.Value;
import io.github.jdbcx.ValueFactory;

public final class NullValue extends AbstractValue {
    public static final NullValue INSTANCE = new NullValue(null);

    public static NullValue of(ValueFactory factory) {
        return factory == null ? INSTANCE : new NullValue(factory);
    }

    @Override
    public boolean isNull() {
        return true;
    }

    @Override
    public String toJsonExpression() {
        return Constants.NULL_STR;
    }

    @Override
    public String toSqlExpression() {
        return Constants.NULL_EXPR;
    }

    @Override
    public Object asObject() {
        return null;
    }

    @Override
    public String asString(Charset charset) {
        return Constants.EMPTY_STRING;
    }

    @Override
    public Value resetToDefault() {
        return this;
    }

    @Override
    public Value resetToNull() {
        return this;
    }

    @Override
    public Value updateFrom(ResultSet rs, int index) throws SQLException {
        rs.getObject(index);
        return this;
    }

    private NullValue(ValueFactory factory) {
        super(factory, true);
    }
}
