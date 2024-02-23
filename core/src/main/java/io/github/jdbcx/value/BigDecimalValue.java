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
package io.github.jdbcx.value;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;

import io.github.jdbcx.ValueFactory;

public class BigDecimalValue extends ObjectValue<BigDecimal> {
    public static BigDecimalValue of(ValueFactory factory, boolean nullable, BigDecimal value) {
        return new BigDecimalValue(factory, nullable, value);
    }

    public static BigDecimalValue of(ValueFactory factory, boolean nullable, int scale, BigDecimal value) {
        return new BigDecimalValue(factory, nullable, scale, value);
    }

    protected final int scale;

    @Override
    protected BigDecimalValue set(BigDecimal value) {
        if (nullable || value != null) {
            super.set(value);
        } else {
            super.set(factory.getDefaultBigDecimal());
        }
        return this;
    }

    protected BigDecimalValue(ValueFactory factory, boolean nullable, BigDecimal value) {
        super(factory, nullable, value);
        this.scale = this.factory.getDecimalScale();
        if (!nullable && value == null) {
            set(factory.getDefaultBigDecimal());
        }
    }

    protected BigDecimalValue(ValueFactory factory, boolean nullable, int scale, BigDecimal value) {
        super(factory, nullable, value);
        this.scale = scale;
        if (!nullable && value == null) {
            set(factory.getDefaultBigDecimal());
        }
    }

    @Override
    public boolean asBoolean() {
        return isNull() ? factory.getDefaultBoolean() : asObject().byteValue() != (byte) 0;
    }

    @Override
    public byte asByte() {
        return isNull() ? factory.getDefaultByte() : asObject().byteValue();
    }

    @Override
    public short asShort() {
        return isNull() ? factory.getDefaultShort() : asObject().shortValue();
    }

    @Override
    public int asInt() {
        return isNull() ? factory.getDefaultInt() : asObject().intValue();
    }

    @Override
    public long asLong() {
        return isNull() ? factory.getDefaultLong() : asObject().longValue();
    }

    @Override
    public float asFloat() {
        return isNull() ? factory.getDefaultFloat() : asObject().floatValue();
    }

    @Override
    public double asDouble() {
        return isNull() ? factory.getDefaultDouble() : asObject().doubleValue();
    }

    @Override
    public BigInteger asBigInteger() {
        if (isNull()) {
            return null;
        }

        BigDecimal value = asObject();
        if (value.remainder(BigDecimal.ONE) != BigDecimal.ZERO) {
            throw new IllegalArgumentException("Failed to convert BigDecimal to BigInteger: " + value);
        }
        return value.toBigIntegerExact();
    }

    @Override
    public BigDecimal asBigDecimal() {
        BigDecimal value = asObject();
        if (value != null && value.scale() != factory.getDecimalScale()) {
            value = value.setScale(factory.getDecimalScale(), factory.getRoundingMode());
        }
        return value;
    }

    @Override
    public BigDecimal asBigDecimal(int scale) {
        BigDecimal v = asObject();
        if (v != null && v.scale() != scale) {
            v = v.setScale(scale, factory.getRoundingMode());
        }
        return v;
    }

    @Override
    public BigDecimalValue updateFrom(ResultSet rs, int index) throws SQLException {
        set(rs.getBigDecimal(index));
        return this;
    }
}
