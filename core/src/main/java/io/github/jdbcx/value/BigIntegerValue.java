/*
 * Copyright 2022-2026, Zhichun Wu
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

public class BigIntegerValue extends ObjectValue<BigInteger> {
    public static BigIntegerValue of(ValueFactory factory, boolean nullable, BigInteger value) {
        return new BigIntegerValue(factory, nullable, value);
    }

    @Override
    protected BigIntegerValue set(BigInteger value) {
        if (nullable || value != null) {
            super.set(value);
        } else {
            super.set(factory.getDefaultBigInteger());
        }
        return this;
    }

    protected BigIntegerValue(ValueFactory factory, boolean nullable, BigInteger value) {
        super(factory, nullable, value);
        if (!nullable && value == null) {
            set(this.factory.getDefaultBigInteger());
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
        return asObject();
    }

    @Override
    public BigDecimal asBigDecimal() {
        return isNull() ? null : new BigDecimal(asObject(), factory.getDecimalScale());
    }

    @Override
    public BigDecimal asBigDecimal(int scale) {
        return isNull() ? null : new BigDecimal(asObject(), scale);
    }

    @Override
    public BigIntegerValue updateFrom(ResultSet rs, int index) throws SQLException {
        final BigDecimal value = rs.getBigDecimal(index);
        set(value != null ? value.toBigInteger() : null);
        return this;
    }
}
