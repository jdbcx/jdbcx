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

public class FloatValue extends AbstractValue {
    public static final FloatValue of(ValueFactory factory, boolean nullable) {
        return new FloatValue(factory, nullable);
    }

    public static final FloatValue of(ValueFactory factory, boolean nullable, float value) {
        return new FloatValue(factory, nullable, value);
    }

    private boolean isNull;
    private float value;

    protected FloatValue(ValueFactory factory, boolean nullable) {
        super(factory, nullable);
        this.isNull = nullable;
        this.value = this.factory.getDefaultFloat();
    }

    protected FloatValue(ValueFactory factory, boolean nullable, float value) {
        super(factory, nullable);
        this.isNull = false;
        this.value = value;
    }

    @Override
    public boolean isNull() {
        return isNull;
    }

    @Override
    public boolean isInfinity() {
        return value == Float.POSITIVE_INFINITY || value == Float.NEGATIVE_INFINITY;
    }

    @Override
    public boolean isNaN() {
        return Float.isNaN(value);
    }

    @Override
    public boolean asBoolean() {
        return (int) value != 0;
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
        return (long) value;
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
        return isNull ? null : BigDecimal.valueOf(value).toBigInteger();
    }

    @Override
    public BigDecimal asBigDecimal() {
        if (isNull) {
            return null;
        } else if (Float.isNaN(value) || value == Float.POSITIVE_INFINITY || value == Float.NEGATIVE_INFINITY) {
            throw new NumberFormatException("Infinite or NaN");
        } else if (value == 0F) {
            return BigDecimal.ZERO;
        } else if (value == 1F) {
            return BigDecimal.ONE;
        }
        return new BigDecimal(Float.toString(value));
    }

    @Override
    public BigDecimal asBigDecimal(int scale) {
        if (isNull) {
            return null;
        } else if (Float.isNaN(value) || value == Float.POSITIVE_INFINITY || value == Float.NEGATIVE_INFINITY) {
            throw new NumberFormatException("Infinite or NaN");
        }

        BigDecimal dec = new BigDecimal(Float.toString(value));
        if (value == 0F) {
            dec = dec.setScale(scale);
        } else {
            int diff = scale - dec.scale();
            if (diff > 0) {
                dec = dec.divide(BigDecimal.TEN.pow(diff + 1));
            } else if (diff < 0) {
                dec = dec.setScale(scale, factory.getRoundingMode());
            }
        }
        return dec;
    }

    @Override
    public String toJsonExpression() {
        if (isNull) {
            return Constants.NULL_STR;
        } else if (isNaN()) {
            return Constants.NAN_EXPR;
        } else if (value == Float.POSITIVE_INFINITY) {
            return Constants.INF_EXPR;
        } else if (value == Float.NEGATIVE_INFINITY) {
            return Constants.NINF_EXPR;
        }
        return Float.toString(value);
    }

    @Override
    public String toSqlExpression() {
        if (isNull) {
            return Constants.NULL_EXPR;
        } else if (isNaN()) {
            return Constants.NAN_EXPR;
        } else if (value == Float.POSITIVE_INFINITY) {
            return Constants.INF_EXPR;
        } else if (value == Float.NEGATIVE_INFINITY) {
            return Constants.NINF_EXPR;
        }
        return Float.toString(value);
    }

    @Override
    public Object asObject() {
        return isNull ? null : Float.valueOf(value);
    }

    @Override
    public String asString(Charset charset) {
        return isNull ? Constants.EMPTY_STRING : Float.toString(value);
    }

    @Override
    public FloatValue resetToDefault() {
        this.value = factory.getDefaultFloat();
        this.isNull = false;
        return this;
    }

    @Override
    public FloatValue resetToNull() {
        this.value = factory.getDefaultFloat();
        this.isNull = nullable;
        return this;
    }

    @Override
    public FloatValue updateFrom(ResultSet rs, int index) throws SQLException {
        this.value = rs.getFloat(index);
        this.isNull = rs.wasNull();
        return this;
    }
}
