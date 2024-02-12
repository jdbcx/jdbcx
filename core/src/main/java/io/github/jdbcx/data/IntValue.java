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

public class IntValue extends AbstractValue {
    /**
     * Unsigned version of {@code IntValue}.
     */
    static final class UnsignedIntValue extends IntValue {
        protected UnsignedIntValue(ValueFactory factory, boolean nullable) {
            super(factory, nullable);
        }

        protected UnsignedIntValue(ValueFactory factory, boolean nullable, int value) {
            super(factory, nullable, value);
        }

        @Override
        public long asLong() {
            return 0xFFFFFFFFL & asInt();
        }

        @Override
        public double asDouble() {
            return asLong();
        }

        @Override
        public BigInteger asBigInteger() {
            return isNull() ? null : BigInteger.valueOf(asLong());
        }

        @Override
        public BigDecimal asBigDecimal() {
            return isNull() ? null : BigDecimal.valueOf(asLong());
        }

        @Override
        public BigDecimal asBigDecimal(int scale) {
            return isNull() ? null : BigDecimal.valueOf(asLong(), scale);
        }

        @Override
        public Object asObject() {
            return isNull() ? null : UnsignedInt.valueOf(asInt());
        }

        @Override
        public String asString() {
            return isNull() ? Constants.EMPTY_STRING : Integer.toUnsignedString(asInt());
        }

        @Override
        public String toJsonExpression() {
            return isNull() ? Constants.NULL_STR : Integer.toUnsignedString(asInt());
        }

        @Override
        public String toSqlExpression() {
            return isNull() ? Constants.NULL_EXPR : Integer.toUnsignedString(asInt());
        }
    }

    public static final IntValue of(ValueFactory factory, boolean nullable, boolean signed) {
        return signed ? new IntValue(factory, nullable) : new UnsignedIntValue(factory, nullable);
    }

    public static final IntValue of(ValueFactory factory, boolean nullable, boolean signed, int value) {
        return signed ? new IntValue(factory, nullable, value) : new UnsignedIntValue(factory, nullable, value);
    }

    private boolean isNull;
    private int value;

    protected IntValue(ValueFactory factory, boolean nullable) {
        super(factory, nullable);
        this.isNull = nullable;
        this.value = this.factory.getDefaultInt();
    }

    protected IntValue(ValueFactory factory, boolean nullable, int value) {
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
        return value != 0;
    }

    @Override
    public byte asByte() {
        return (byte) value;
    }

    @Override
    public short asShort() {
        return (short) value;
    }

    @Override
    public int asInt() {
        return value;
    }

    @Override
    public long asLong() {
        return value;
    }

    @Override
    public float asFloat() {
        return value;
    }

    @Override
    public double asDouble() {
        return value;
    }

    @Override
    public BigInteger asBigInteger() {
        return isNull ? null : BigInteger.valueOf(value);
    }

    @Override
    public BigDecimal asBigDecimal() {
        return isNull ? null : BigDecimal.valueOf(asLong(), factory.getDecimalScale());
    }

    @Override
    public String toJsonExpression() {
        return isNull ? Constants.NULL_STR : Integer.toString(value);
    }

    @Override
    public String toSqlExpression() {
        return isNull ? Constants.NULL_EXPR : Integer.toString(value);
    }

    @Override
    public Object asObject() {
        return isNull ? null : value;
    }

    @Override
    public String asString(Charset charset) {
        return isNull ? Constants.EMPTY_STRING : Integer.toString(value);
    }

    @Override
    public IntValue resetToDefault() {
        this.value = factory.getDefaultInt();
        this.isNull = false;
        return this;
    }

    @Override
    public IntValue resetToNull() {
        this.value = factory.getDefaultInt();
        this.isNull = nullable;
        return this;
    }

    @Override
    public IntValue updateFrom(ResultSet rs, int index) throws SQLException {
        this.value = rs.getInt(index);
        this.isNull = rs.wasNull();
        return this;
    }
}
