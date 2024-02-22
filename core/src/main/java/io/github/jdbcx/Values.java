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
package io.github.jdbcx;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

import io.github.jdbcx.data.BigDecimalValue;
import io.github.jdbcx.data.BigIntegerValue;
import io.github.jdbcx.data.BooleanValue;
import io.github.jdbcx.data.ByteValue;
import io.github.jdbcx.data.DateTimeValue;
import io.github.jdbcx.data.DateValue;
import io.github.jdbcx.data.DoubleValue;
import io.github.jdbcx.data.FloatValue;
import io.github.jdbcx.data.IntValue;
import io.github.jdbcx.data.LongValue;
import io.github.jdbcx.data.NullValue;
import io.github.jdbcx.data.ShortValue;
import io.github.jdbcx.data.StringValue;
import io.github.jdbcx.data.TimeValue;

final class Values {
    static final Class<? extends Value> DEFAULT_VALUE_TYPE = StringValue.class;

    static Value of(boolean value) {
        return BooleanValue.of(null, true, value);
    }

    static Value of(byte value) {
        return ByteValue.of(null, true, true, value);
    }

    static Value of(short value) {
        return ShortValue.of(null, true, true, value);
    }

    static Value of(int value) {
        return IntValue.of(null, true, true, value);
    }

    static Value of(long value) {
        return LongValue.of(null, true, true, value);
    }

    static Value of(float value) {
        return FloatValue.of(null, true, value);
    }

    static Value of(double value) {
        return DoubleValue.of(null, true, value);
    }

    static Value of(BigInteger value) {
        return BigIntegerValue.of(null, true, value);
    }

    static Value of(BigDecimal value) {
        return BigDecimalValue.of(null, true, value);
    }

    static Value of(LocalDate value) {
        return DateValue.of(null, true, value);
    }

    static Value of(LocalTime value) {
        return TimeValue.of(null, true, value);
    }

    static Value of(LocalDateTime value) {
        return DateTimeValue.of(null, true, value);
    }

    static Value of(byte[] value) {
        return StringValue.of(null, true, 0, value);
    }

    static Value of(String value) {
        return StringValue.of(null, true, 0, value);
    }

    static Value of(Object value) {
        final Value v;
        if (value == null) {
            v = StringValue.of(null, true, 0, (String) null);
        } else if (value instanceof byte[]) {
            v = StringValue.of(null, true, 0, (byte[]) value);
        } else {
            v = StringValue.of(null, true, 0, value.toString());
        }
        return v;
    }

    static Value ofBoolean(ValueFactory factory, boolean nullable, boolean value) {
        return BooleanValue.of(factory, nullable, value);
    }

    static Value ofByte(ValueFactory factory, boolean nullable, boolean signed, byte value) {
        return ByteValue.of(factory, nullable, signed, value);
    }

    static Value ofChar(ValueFactory factory, boolean nullable, int precision, String value) {
        return StringValue.of(factory, nullable, precision, value);
    }

    static Value ofShort(ValueFactory factory, boolean nullable, boolean signed, short value) {
        return ShortValue.of(factory, nullable, signed, value);
    }

    static Value ofInt(ValueFactory factory, boolean nullable, boolean signed, int value) {
        return IntValue.of(factory, nullable, signed, value);
    }

    static Value ofLong(ValueFactory factory, boolean nullable, boolean signed, long value) {
        return LongValue.of(factory, nullable, signed, value);
    }

    static Value ofFloat(ValueFactory factory, boolean nullable, float value) {
        return FloatValue.of(factory, nullable, value);
    }

    static Value ofDouble(ValueFactory factory, boolean nullable, double value) {
        return DoubleValue.of(factory, nullable, value);
    }

    static Value ofBigInteger(ValueFactory factory, boolean nullable, BigInteger value) {
        return BigIntegerValue.of(factory, nullable, value);
    }

    static Value ofBigDecimal(ValueFactory factory, boolean nullable, int scale, BigDecimal value) {
        return BigDecimalValue.of(factory, nullable, scale, value);
    }

    static Value ofString(ValueFactory factory, boolean nullable, int length, String value) {
        return StringValue.of(factory, nullable, length, value);
    }

    static Value ofNull(ValueFactory factory) {
        return NullValue.of(factory);
    }

    static Value ofDate(ValueFactory factory, boolean nullable, LocalDate value) {
        return DateValue.of(factory, nullable, value);
    }

    static Value ofTime(ValueFactory factory, boolean nullable, int scale, LocalTime value) {
        return TimeValue.of(factory, nullable, scale, value);
    }

    static Value ofTimestamp(ValueFactory factory, boolean nullable, int scale, LocalDateTime value) {
        return DateTimeValue.of(factory, nullable, scale, value);
    }

    static Value of(ValueFactory factory, Class<? extends Value> valueType) {
        final Value value;
        if (valueType == StringValue.class) {
            value = StringValue.of(factory, true, 0);
        } else {
            throw new IllegalArgumentException("Unsupported type: " + valueType);
        }
        return value;
    }

    private Values() {
    }
}
