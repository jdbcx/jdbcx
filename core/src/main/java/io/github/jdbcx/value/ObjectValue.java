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

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.sql.ResultSet;
import java.sql.SQLException;

import io.github.jdbcx.Constants;
import io.github.jdbcx.ValueFactory;

public abstract class ObjectValue<T extends Serializable> extends AbstractValue {
    private T value;

    protected ObjectValue<T> set(T value) {
        // no fancy stuff in base class
        this.value = value;
        return this;
    }

    protected ObjectValue(ValueFactory factory, boolean nullable, T value) {
        super(factory, nullable);
        this.value = value;
    }

    @Override
    public boolean isNull() {
        return value == null;
    }

    @Override
    public byte asByte() {
        if (value == null) {
            return factory.getDefaultByte();
        }
        return Byte.parseByte(asString());
    }

    @Override
    public short asShort() {
        if (value == null) {
            return factory.getDefaultShort();
        }
        return Short.parseShort(asString());
    }

    @Override
    public int asInt() {
        if (value == null) {
            return factory.getDefaultInt();
        }
        return Integer.parseInt(asString());
    }

    @Override
    public long asLong() {
        if (value == null) {
            return factory.getDefaultLong();
        }
        return Long.parseLong(asString());
    }

    @Override
    public float asFloat() {
        if (value == null) {
            return factory.getDefaultFloat();
        }
        return Float.parseFloat(asString());
    }

    @Override
    public double asDouble() {
        if (value == null) {
            return factory.getDefaultDouble();
        }
        return Double.parseDouble(asString());
    }

    @Override
    public BigInteger asBigInteger() {
        if (value == null) {
            return null;
        }
        return new BigInteger(asString());
    }

    @Override
    public BigDecimal asBigDecimal() {
        if (value == null) {
            return null;
        }
        return new BigDecimal(asString());
    }

    @Override
    public T asObject() {
        return value;
    }

    @Override
    public String asString(Charset charset) {
        return value == null ? factory.getDefaultString() : value.toString();
    }

    @Override
    public String toJsonExpression() {
        return value == null ? Constants.NULL_STR : value.toString();
    }

    @Override
    public String toSqlExpression() {
        return value == null ? Constants.NULL_EXPR : value.toString();
    }

    @Override
    public ObjectValue<T> resetToDefault() {
        throw new UnsupportedOperationException("Unsupport to reset to default value");
    }

    @Override
    public ObjectValue<T> resetToNull() {
        return set(null);
    }

    @SuppressWarnings("unchecked")
    @Override
    public ObjectValue<T> updateFrom(ResultSet rs, int index) throws SQLException {
        return set((T) rs.getObject(index));
    }
}
