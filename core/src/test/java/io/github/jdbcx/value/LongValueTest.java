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
import java.time.format.DateTimeParseException;

import org.testng.annotations.Test;

import io.github.jdbcx.BaseValueTest;
import io.github.jdbcx.Format;
import io.github.jdbcx.ValueFactory;

public class LongValueTest extends BaseValueTest {
    @Test(groups = { "unit" })
    public void testNullValue() {
        ValueFactory factory = ValueFactory.getInstance();
        checkNull(factory, LongValue.of(ValueFactory.getInstance(), true, true));
        checkNull(factory, LongValue.of(ValueFactory.getInstance(), true, false));

        checkNull(factory, new LongValue(ValueFactory.getInstance(), true, 0L).resetToNull());
        checkNull(factory, new LongValue.UnsignedLongValue(ValueFactory.getInstance(), true, 0L).resetToNull());

        checkNull(factory, new LongValue(ValueFactory.getInstance(), true, 1L).resetToNull());
        checkNull(factory, new LongValue.UnsignedLongValue(ValueFactory.getInstance(), true, -1L).resetToNull());
    }

    @Test(groups = { "unit" })
    public void testSignedValue() {
        ValueFactory factory = ValueFactory.getInstance();
        checkValue(factory, new LongValue(ValueFactory.getInstance(), false, 0L),
                false, // isInfinity
                false, // isNan
                false, // isNull
                false, // boolean
                '\0', // char
                (byte) 0, // byte
                (short) 0, // short
                0, // int
                0L, // long
                0F, // float
                0D, // double
                BigInteger.ZERO, // BigInteger
                new BigDecimal(0), // BigDecimal
                new BigDecimal(BigInteger.ZERO, 3), // BigDecimal(3)
                Format.CSV, // Enum(Format)
                DateTimeParseException.class, // Date
                DateTimeParseException.class, // Time
                DateTimeParseException.class, // DateTime
                DateTimeParseException.class, // DateTime(9)
                0L, // Object
                "0", // String
                "0", // JSON expression
                "0" // SQL Expression
        );
        checkValue(factory, new LongValue(ValueFactory.getInstance(), false, 1L),
                false, // isInfinity
                false, // isNan
                false, // isNull
                true, // boolean
                '\1', // char
                (byte) 1, // byte
                (short) 1, // short
                1, // int
                1L, // long
                1F, // float
                1D, // double
                BigInteger.ONE, // BigInteger
                new BigDecimal(1), // BigDecimal
                new BigDecimal(BigInteger.ONE, 3), // BigDecimal(3)
                Format.TSV, // Enum(Format)
                DateTimeParseException.class, // Date
                DateTimeParseException.class, // Time
                DateTimeParseException.class, // DateTime
                DateTimeParseException.class, // DateTime(9)
                1L, // Object
                "1", // String
                "1", // JSON expression
                "1" // SQL Expression
        );
        checkValue(factory, new LongValue(ValueFactory.getInstance(), false, -2L),
                false, // isInfinity
                false, // isNan
                false, // isNull
                true, // boolean
                (char) -2, // char
                (byte) -2, // byte
                (short) -2, // short
                -2, // int
                -2L, // long
                -2F, // float
                -2D, // double
                BigInteger.valueOf(-2L), // BigInteger
                new BigDecimal(-2L), // BigDecimal
                new BigDecimal(BigInteger.valueOf(-2L), 3), // BigDecimal(3)
                IllegalArgumentException.class, // Enum(Format)
                DateTimeParseException.class, // Date
                DateTimeParseException.class, // Time
                DateTimeParseException.class, // DateTime
                DateTimeParseException.class, // DateTime(9)
                -2L, // Object
                "-2", // String
                "-2", // JSON expression
                "-2" // SQL Expression
        );
    }

    @Test(groups = { "unit" })
    public void testUnsignedValue() {
        ValueFactory factory = ValueFactory.getInstance();
        checkValue(factory, new LongValue.UnsignedLongValue(ValueFactory.getInstance(), false, 0L),
                false, // isInfinity
                false, // isNan
                false, // isNull
                false, // boolean
                '\0', // char
                (byte) 0, // byte
                (short) 0, // short
                0, // int
                0L, // long
                0F, // float
                0D, // double
                BigInteger.ZERO, // BigInteger
                new BigDecimal(0), // BigDecimal
                new BigDecimal(BigInteger.ZERO, 3), // BigDecimal(3)
                Format.CSV, // Enum(Format)
                DateTimeParseException.class, // Date
                DateTimeParseException.class, // Time
                DateTimeParseException.class, // DateTime
                DateTimeParseException.class, // DateTime(9)
                UnsignedLong.ZERO, // Object
                "0", // String
                "0", // JSON expression
                "0" // SQL Expression
        );
        checkValue(factory, new LongValue.UnsignedLongValue(ValueFactory.getInstance(), false, 1L),
                false, // isInfinity
                false, // isNan
                false, // isNull
                true, // boolean
                '\1', // char
                (byte) 1, // byte
                (short) 1, // short
                1, // int
                1L, // long
                1F, // float
                1D, // double
                BigInteger.ONE, // BigInteger
                new BigDecimal(1), // BigDecimal
                new BigDecimal(BigInteger.ONE, 3), // BigDecimal(3)
                Format.TSV, // Enum(Format)
                DateTimeParseException.class, // Date
                DateTimeParseException.class, // Time
                DateTimeParseException.class, // DateTime
                DateTimeParseException.class, // DateTime(9)
                UnsignedLong.ONE, // Object
                "1", // String
                "1", // JSON expression
                "1" // SQL Expression
        );
        checkValue(factory, new LongValue.UnsignedLongValue(ValueFactory.getInstance(), false, -2L),
                false, // isInfinity
                false, // isNan
                false, // isNull
                true, // boolean
                (char) -2, // char
                (byte) -2, // byte
                (short) -2, // short
                -2, // int
                -2L, // long
                -2F, // float
                -2D, // double
                new BigInteger("18446744073709551614"), // BigInteger
                new BigDecimal("18446744073709551614"), // BigDecimal
                new BigDecimal(new BigInteger("18446744073709551614"), 3), // BigDecimal(3)
                IllegalArgumentException.class, // Enum(Format)
                DateTimeParseException.class, // Date
                DateTimeParseException.class, // Time
                DateTimeParseException.class, // DateTime
                DateTimeParseException.class, // DateTime(9)
                UnsignedLong.valueOf(-2L), // Object
                "18446744073709551614", // String
                "18446744073709551614", // JSON expression
                "18446744073709551614" // SQL Expression
        );
    }
}
