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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.sql.ResultSet;
import java.sql.SQLException;

import io.github.jdbcx.Constants;
import io.github.jdbcx.ValueFactory;

public class DoubleValue extends AbstractValue {
    public static final DoubleValue of(ValueFactory factory, boolean nullable) {
        return new DoubleValue(factory, nullable);
    }

    public static final DoubleValue of(ValueFactory factory, boolean nullable, double value) {
        return new DoubleValue(factory, nullable, value);
    }

    private boolean isNull;
    private double value;

    protected DoubleValue(ValueFactory factory, boolean nullable) {
        super(factory, nullable);
        this.isNull = nullable;
        this.value = this.factory.getDefaultDouble();
    }

    protected DoubleValue(ValueFactory factory, boolean nullable, double value) {
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
        return value == Double.POSITIVE_INFINITY || value == Double.NEGATIVE_INFINITY;
    }

    @Override
    public boolean isNaN() {
        return Double.isNaN(value);
    }

    @Override
    public boolean asBoolean() {
        return (long) value != 0L;
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
        return (float) value;
    }

    @Override
    public double asDouble() {
        return value;
    }

    @Override
    public BigInteger asBigInteger() {
        return isNull ? null : asBigDecimal().toBigInteger();
    }

    @Override
    public BigDecimal asBigDecimal() {
        if (isNull) {
            return null;
        } else if (Double.isNaN(value) || value == Double.POSITIVE_INFINITY || value == Double.NEGATIVE_INFINITY) {
            throw new NumberFormatException("Infinite or NaN");
        } else if (value == 0D) {
            return BigDecimal.ZERO;
        } else if (value == 1D) {
            return BigDecimal.ONE;
        }
        return new BigDecimal(Double.toString(value));
    }

    @Override
    public BigDecimal asBigDecimal(int scale) {
        if (isNull) {
            return null;
        } else if (Double.isNaN(value) || value == Double.POSITIVE_INFINITY || value == Double.NEGATIVE_INFINITY) {
            throw new NumberFormatException("Infinite or NaN");
        }

        BigDecimal dec = new BigDecimal(Double.toString(value));
        if (value == 0D) {
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
        } else if (value == Double.POSITIVE_INFINITY) {
            return Constants.INF_EXPR;
        } else if (value == Double.NEGATIVE_INFINITY) {
            return Constants.NINF_EXPR;
        }
        return Double.toString(value);
    }

    @Override
    public String toSqlExpression() {
        if (isNull) {
            return Constants.NULL_EXPR;
        } else if (isNaN()) {
            return Constants.NAN_EXPR;
        } else if (value == Double.POSITIVE_INFINITY) {
            return Constants.INF_EXPR;
        } else if (value == Double.NEGATIVE_INFINITY) {
            return Constants.NINF_EXPR;
        }
        return Double.toString(value);
    }

    @Override
    public Object asObject() {
        return isNull ? null : Double.valueOf(value);
    }

    @Override
    public String asString(Charset charset) {
        return isNull ? Constants.EMPTY_STRING : Double.toString(value);
    }

    @Override
    public DoubleValue resetToDefault() {
        this.value = factory.getDefaultDouble();
        this.isNull = false;
        return this;
    }

    @Override
    public DoubleValue resetToNull() {
        this.value = factory.getDefaultDouble();
        this.isNull = nullable;
        return this;
    }

    @Override
    public DoubleValue updateFrom(ResultSet rs, int index) throws SQLException {
        this.value = rs.getDouble(index);
        this.isNull = rs.wasNull();
        return this;
    }
}
