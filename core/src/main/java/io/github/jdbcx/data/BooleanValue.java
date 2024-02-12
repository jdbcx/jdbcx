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
package io.github.jdbcx.data;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.sql.ResultSet;
import java.sql.SQLException;

import io.github.jdbcx.Constants;
import io.github.jdbcx.ValueFactory;

public final class BooleanValue extends AbstractValue {
    public static final BooleanValue of(ValueFactory factory, boolean nullable) {
        return new BooleanValue(factory, nullable);
    }

    public static final BooleanValue of(ValueFactory factory, boolean nullable, boolean value) {
        return new BooleanValue(factory, nullable, value);
    }

    private boolean isNull;
    private boolean value;

    protected BooleanValue(ValueFactory factory, boolean nullable) {
        super(factory, nullable);

        this.isNull = nullable;
        this.value = this.factory.getDefaultBoolean();
    }

    protected BooleanValue(ValueFactory factory, boolean nullable, boolean value) {
        super(factory, nullable);

        this.isNull = false;
        this.value = value;
    }

    @Override
    public boolean isNull() {
        return isNull;
    }

    @Override
    public boolean asBoolean() {
        return value;
    }

    @Override
    public byte asByte() {
        return value ? (byte) 1 : (byte) 0;
    }

    @Override
    public short asShort() {
        return value ? (short) 1 : (short) 0;
    }

    @Override
    public int asInt() {
        return value ? 1 : 0;
    }

    @Override
    public long asLong() {
        return value ? 1L : 0L;
    }

    @Override
    public float asFloat() {
        return value ? 1F : 0F;
    }

    @Override
    public double asDouble() {
        return value ? 1D : 0D;
    }

    @Override
    public BigInteger asBigInteger() {
        if (isNull) {
            return null;
        }
        return value ? BigInteger.ONE : BigInteger.ZERO;
    }

    @Override
    public BigDecimal asBigDecimal() {
        if (isNull) {
            return null;
        }
        return BigDecimal.valueOf(value ? 1L : 0L, factory.getDecimalScale());
    }

    @Override
    public BigDecimal asBigDecimal(int scale) {
        if (isNull) {
            return null;
        }
        return BigDecimal.valueOf(value ? 1L : 0L, scale);
    }

    @Override
    public String toJsonExpression() {
        return isNull ? Constants.NULL_STR : Boolean.toString(value);
    }

    @Override
    public String toSqlExpression() {
        if (isNull) {
            return Constants.NULL_EXPR;
        }
        return value ? Constants.ONE_EXPR : Constants.ZERO_EXPR;
    }

    @Override
    public Object asObject() {
        return isNull ? null : Boolean.valueOf(value);
    }

    @Override
    public String asString(Charset charset) {
        return isNull ? Constants.EMPTY_STRING : String.valueOf(value);
    }

    @Override
    public BooleanValue resetToDefault() {
        this.value = factory.getDefaultBoolean();
        this.isNull = false;
        return this;
    }

    @Override
    public BooleanValue resetToNull() {
        this.value = factory.getDefaultBoolean();
        this.isNull = nullable;
        return this;
    }

    @Override
    public BooleanValue updateFrom(ResultSet rs, int index) throws SQLException {
        this.value = rs.getBoolean(index);
        this.isNull = rs.wasNull();
        return this;
    }
}
