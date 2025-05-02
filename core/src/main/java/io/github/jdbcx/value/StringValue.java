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
import io.github.jdbcx.Converter;
import io.github.jdbcx.ValueFactory;

public final class StringValue extends ObjectValue<String> {
    public static StringValue of(ValueFactory factory, boolean nullable, int length) {
        return new StringValue(factory, nullable, length, null);
    }

    public static StringValue of(ValueFactory factory, boolean nullable, int length, String value) {
        return new StringValue(factory, nullable, length, value);
    }

    public static StringValue of(ValueFactory factory, boolean nullable, int length, byte[] value) {
        final StringValue v = new StringValue(factory, nullable, length, null);
        if (value != null) {
            v.set(new String(value, v.factory.getCharset()));
        }
        return v;
    }

    public static StringValue of(String value) {
        return new StringValue(ValueFactory.getInstance(), true, 0, value);
    }

    public static StringValue of(Object value) {
        return of(value == null ? null : value.toString());
    }

    public static StringValue ofJson(Object value) {
        return of(value == null ? null : ValueFactory.toJson(value));
    }

    private final int length;

    @Override
    protected StringValue set(String value) {
        final int len;
        if (value == null) {
            super.set(nullable ? null : factory.getDefaultString(length));
        } else if (length == 0 || (len = value.length()) == length) {
            super.set(value);
        } else if (len > length) {
            super.set(value.substring(0, length));
        } else {
            StringBuilder builder = new StringBuilder(length).append(value);
            char ch = factory.getDefaultChar();
            for (int i = len; i < length; i++) {
                builder.append(ch);
            }
            super.set(builder.toString());
        }
        return this;
    }

    protected StringValue(ValueFactory factory, boolean nullable, int length, String value) {
        super(factory, nullable, value);
        this.length = length;
        set(value);
    }

    @Override
    public byte[] asBinary(Charset charset) {
        if (isNull()) {
            return Constants.EMPTY_BYTE_ARRAY;
        }
        return asObject().getBytes(charset != null ? charset : factory.getCharset());
    }

    @Override
    public String asString(Charset charset) {
        return isNull() ? factory.getDefaultString() : asObject();
    }

    @Override
    public String toJsonExpression() {
        return isNull() ? Constants.NULL_STR : Converter.toJsonExpression(asObject());
    }

    @Override
    public String toSqlExpression() {
        return isNull() ? Constants.NULL_EXPR : Converter.toSqlExpression(asObject());
    }

    @Override
    public StringValue resetToDefault() {
        set(factory.getDefaultString(length));
        return this;
    }

    @Override
    public StringValue updateFrom(ResultSet rs, int index) throws SQLException {
        set(rs.getString(index));
        return this;
    }
}
