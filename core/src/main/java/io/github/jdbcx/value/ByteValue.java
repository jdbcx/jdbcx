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

public class ByteValue extends AbstractValue {
    /**
     * Unsigned version of {@code ByteValue}.
     */
    static final class UnsignedByteValue extends ByteValue {
        protected UnsignedByteValue(ValueFactory factory, boolean nullable) {
            super(factory, nullable);
        }

        protected UnsignedByteValue(ValueFactory factory, boolean nullable, byte value) {
            super(factory, nullable, value);
        }

        @Override
        public char asChar() {
            return isNull() ? factory.getDefaultChar() : (char) asShort();
        }

        @Override
        public short asShort() {
            return (short) (0xFF & asByte());
        }

        @Override
        public int asInt() {
            return 0xFF & asByte();
        }

        @Override
        public long asLong() {
            return 0xFFL & asByte();
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
            return isNull() ? null : UnsignedByte.valueOf(asByte());
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

    public static final ByteValue of(ValueFactory factory, boolean nullable, boolean signed) {
        return signed ? new ByteValue(factory, nullable) : new UnsignedByteValue(factory, nullable);
    }

    public static final ByteValue of(ValueFactory factory, boolean nullable, boolean signed, byte value) {
        return signed ? new ByteValue(factory, nullable, value) : new UnsignedByteValue(factory, nullable, value);
    }

    private boolean isNull;
    private byte value;

    protected ByteValue(ValueFactory factory, boolean nullable) {
        super(factory, nullable);
        this.isNull = nullable;
        this.value = this.factory.getDefaultByte();
    }

    protected ByteValue(ValueFactory factory, boolean nullable, byte value) {
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
        return value != (byte) 0;
    }

    @Override
    public byte asByte() {
        return value;
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
        return isNull ? Constants.NULL_STR : Byte.toString(value);
    }

    @Override
    public String toSqlExpression() {
        return isNull ? Constants.NULL_EXPR : Byte.toString(value);
    }

    @Override
    public Object asObject() {
        return isNull ? null : value;
    }

    @Override
    public String asString(Charset charset) {
        return isNull ? Constants.EMPTY_STRING : Byte.toString(value);
    }

    @Override
    public ByteValue resetToDefault() {
        this.value = factory.getDefaultByte();
        this.isNull = false;
        return this;
    }

    @Override
    public ByteValue resetToNull() {
        this.value = factory.getDefaultByte();
        this.isNull = nullable;
        return this;
    }

    @Override
    public ByteValue updateFrom(ResultSet rs, int index) throws SQLException {
        this.value = rs.getByte(index);
        this.isNull = rs.wasNull();
        return this;
    }
}
