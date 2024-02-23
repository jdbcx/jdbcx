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
import java.nio.charset.Charset;
import java.sql.ResultSet;
import java.sql.SQLException;

import io.github.jdbcx.Constants;
import io.github.jdbcx.ValueFactory;

public class ShortValue extends AbstractValue {
    /**
     * Unsigned version of {@code ShortValue}.
     */
    static final class UnsignedShortValue extends ShortValue {
        protected UnsignedShortValue(ValueFactory factory, boolean nullable) {
            super(factory, nullable);
        }

        protected UnsignedShortValue(ValueFactory factory, boolean nullable, short value) {
            super(factory, nullable, value);
        }

        @Override
        public int asInt() {
            return 0xFFFF & asShort();
        }

        @Override
        public long asLong() {
            return 0xFFFFL & asShort();
        }

        @Override
        public float asFloat() {
            return asInt();
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
        public BigDecimal asBigDecimal(int scale) {
            return isNull() ? null : BigDecimal.valueOf(asLong(), scale);
        }

        @Override
        public Object asObject() {
            return isNull() ? null : UnsignedShort.valueOf(asShort());
        }

        @Override
        public String asString() {
            return isNull() ? Constants.EMPTY_STRING : Integer.toString(asInt());
        }

        @Override
        public String toJsonExpression() {
            return isNull() ? Constants.NULL_STR : Integer.toString(asInt());
        }

        @Override
        public String toSqlExpression() {
            return isNull() ? Constants.NULL_EXPR : Integer.toString(asInt());
        }
    }

    public static final ShortValue of(ValueFactory factory, boolean nullable, boolean signed) {
        return signed ? new ShortValue(factory, nullable) : new UnsignedShortValue(factory, nullable);
    }

    public static final ShortValue of(ValueFactory factory, boolean nullable, boolean signed, short value) {
        return signed ? new ShortValue(factory, nullable, value) : new UnsignedShortValue(factory, nullable, value);
    }

    private boolean isNull;
    private short value;

    protected ShortValue(ValueFactory factory, boolean nullable) {
        super(factory, nullable);
        this.isNull = nullable;
        this.value = this.factory.getDefaultShort();
    }

    protected ShortValue(ValueFactory factory, boolean nullable, short value) {
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
        return value != (short) 0;
    }

    @Override
    public byte asByte() {
        return (byte) value;
    }

    @Override
    public short asShort() {
        return value;
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
        return isNull ? Constants.NULL_STR : Short.toString(value);
    }

    @Override
    public String toSqlExpression() {
        return isNull ? Constants.NULL_EXPR : Short.toString(value);
    }

    @Override
    public Object asObject() {
        return isNull ? null : value;
    }

    @Override
    public String asString(Charset charset) {
        return isNull ? Constants.EMPTY_STRING : Short.toString(value);
    }

    @Override
    public ShortValue resetToDefault() {
        this.value = factory.getDefaultShort();
        this.isNull = false;
        return this;
    }

    @Override
    public ShortValue resetToNull() {
        this.value = factory.getDefaultShort();
        this.isNull = nullable;
        return this;
    }

    @Override
    public ShortValue updateFrom(ResultSet rs, int index) throws SQLException {
        this.value = rs.getShort(index);
        this.isNull = rs.wasNull();
        return this;
    }
}
