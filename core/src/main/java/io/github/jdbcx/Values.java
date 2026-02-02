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
package io.github.jdbcx;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

import io.github.jdbcx.value.BigDecimalValue;
import io.github.jdbcx.value.BigIntegerValue;
import io.github.jdbcx.value.BinaryValue;
import io.github.jdbcx.value.BooleanValue;
import io.github.jdbcx.value.ByteValue;
import io.github.jdbcx.value.DateTimeValue;
import io.github.jdbcx.value.DateValue;
import io.github.jdbcx.value.DoubleValue;
import io.github.jdbcx.value.FloatValue;
import io.github.jdbcx.value.IntValue;
import io.github.jdbcx.value.LongValue;
import io.github.jdbcx.value.NullValue;
import io.github.jdbcx.value.ShortValue;
import io.github.jdbcx.value.StringValue;
import io.github.jdbcx.value.TimeValue;

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
            v = BinaryValue.of(null, true, (byte[]) value);
        } else if (value instanceof char[]) {
            v = StringValue.of(null, true, 0, new String((char[]) value));
        } else if (value instanceof Byte) {
            v = ByteValue.of(null, true, true, (byte) value);
        } else if (value instanceof Short) {
            v = ShortValue.of(null, true, true, (short) value);
        } else if (value instanceof Integer) {
            v = IntValue.of(null, true, true, (int) value);
        } else if (value instanceof Long) {
            v = LongValue.of(null, true, true, (long) value);
        } else if (value instanceof Float) {
            v = FloatValue.of(null, true, (float) value);
        } else if (value instanceof Double) {
            v = DoubleValue.of(null, true, (double) value);
        } else if (value instanceof BigInteger) {
            v = BigIntegerValue.of(null, true, (BigInteger) value);
        } else if (value instanceof BigDecimal) {
            v = BigDecimalValue.of(null, true, (BigDecimal) value);
        } else if (value instanceof InputStream) {
            v = BinaryValue.of(null, true, (InputStream) value);
        } else if (value instanceof Reader) {
            try {
                v = StringValue.of(null, true, 0, Stream.readAllAsString((Reader) value));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        } else {
            v = StringValue.of(null, true, 0, value.toString());
        }
        return v;
    }

    static BinaryValue ofBinary(ValueFactory factory, boolean nullable, InputStream value) {
        return BinaryValue.of(factory, nullable, value);
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
