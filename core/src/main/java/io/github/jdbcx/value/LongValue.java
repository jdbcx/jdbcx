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
import java.nio.charset.Charset;
import java.sql.ResultSet;
import java.sql.SQLException;

import io.github.jdbcx.Constants;
import io.github.jdbcx.ValueFactory;

public class LongValue extends AbstractValue {
    /**
     * Unsigned version of {@code LongValue}.
     */
    static final class UnsignedLongValue extends LongValue {
        protected UnsignedLongValue(ValueFactory factory, boolean nullable) {
            super(factory, nullable);
        }

        protected UnsignedLongValue(ValueFactory factory, boolean nullable, long value) {
            super(factory, nullable, value);
        }

        protected UnsignedLongValue(ValueFactory factory, boolean nullable, Long value) {
            super(factory, nullable, value);
        }

        @Override
        public BigInteger asBigInteger() {
            if (isNull()) {
                return null;
            }

            long l = asLong();
            BigInteger v = BigInteger.valueOf(l);
            if (l < 0L) {
                v = v.and(UnsignedLong.MASK);
            }
            return v;
        }

        @Override
        public BigDecimal asBigDecimal(int scale) {
            if (isNull()) {
                return null;
            }

            long l = asLong();
            return l < 0L ? new BigDecimal(BigInteger.valueOf(l).and(UnsignedLong.MASK), scale)
                    : BigDecimal.valueOf(l, scale);
        }

        @Override
        public Object asObject() {
            return isNull() ? null : UnsignedLong.valueOf(asLong());
        }

        @Override
        public String asString() {
            return isNull() ? Constants.EMPTY_STRING : Long.toUnsignedString(asLong());
        }

        @Override
        public String toJsonExpression() {
            return isNull() ? Constants.NULL_STR : Long.toUnsignedString(asLong());
        }

        @Override
        public String toSqlExpression() {
            return isNull() ? Constants.NULL_EXPR : Long.toUnsignedString(asLong());
        }
    }

    public static final LongValue of(ValueFactory factory, boolean nullable, boolean signed) {
        return signed ? new LongValue(factory, nullable) : new UnsignedLongValue(factory, nullable);
    }

    public static final LongValue of(ValueFactory factory, boolean nullable, boolean signed, long value) {
        return signed ? new LongValue(factory, nullable, value) : new UnsignedLongValue(factory, nullable, value);
    }

    public static final LongValue of(long value) {
        return new LongValue(ValueFactory.getInstance(), false, value);
    }

    public static final LongValue of(Long value) {
        return new LongValue(ValueFactory.getInstance(), true, value);
    }

    public static final LongValue ofUnsigned(long value) {
        return new UnsignedLongValue(ValueFactory.getInstance(), false, value);
    }

    public static final LongValue ofUnsigned(Long value) {
        return new UnsignedLongValue(ValueFactory.getInstance(), true, value);
    }

    private boolean isNull;
    private long value;

    protected LongValue(ValueFactory factory, boolean nullable) {
        super(factory, nullable);
        this.isNull = nullable;
        this.value = this.factory.getDefaultLong();
    }

    protected LongValue(ValueFactory factory, boolean nullable, long value) {
        super(factory, nullable);
        this.isNull = false;
        this.value = value;
    }

    protected LongValue(ValueFactory factory, boolean nullable, Long value) {
        super(factory, nullable);
        if (value == null) {
            this.isNull = true;
            this.value = factory.getDefaultLong();
        } else {
            this.isNull = false;
            this.value = value;
        }
    }

    @Override
    public boolean isNull() {
        return isNull;
    }

    @Override
    public boolean asBoolean() {
        return value != 0L;
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
        return (int) value;
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
        return isNull ? null : new BigDecimal(asBigInteger(), factory.getDecimalScale());
    }

    @Override
    public String toJsonExpression() {
        return isNull ? Constants.NULL_STR : Long.toString(value);
    }

    @Override
    public String toSqlExpression() {
        return isNull ? Constants.NULL_EXPR : Long.toString(value);
    }

    @Override
    public Object asObject() {
        return isNull ? null : value;
    }

    @Override
    public String asString(Charset charset) {
        return isNull ? Constants.EMPTY_STRING : Long.toString(value);
    }

    @Override
    public LongValue resetToDefault() {
        this.value = factory.getDefaultLong();
        this.isNull = false;
        return this;
    }

    @Override
    public LongValue resetToNull() {
        this.value = factory.getDefaultLong();
        this.isNull = nullable;
        return this;
    }

    @Override
    public LongValue updateFrom(ResultSet rs, int index) throws SQLException {
        this.value = rs.getLong(index);
        this.isNull = rs.wasNull();
        return this;
    }
}
