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

public class ShortValueTest extends BaseValueTest {
    @Test(groups = { "unit" })
    public void testNullValue() {
        ValueFactory factory = ValueFactory.getInstance();
        checkNull(factory, ShortValue.of(ValueFactory.getInstance(), true, true));
        checkNull(factory, ShortValue.of(ValueFactory.getInstance(), true, false));

        checkNull(factory, new ShortValue(ValueFactory.getInstance(), true, (short) 0).resetToNull());
        checkNull(factory,
                new ShortValue.UnsignedShortValue(ValueFactory.getInstance(), true, (short) 0).resetToNull());

        checkNull(factory, new ShortValue(ValueFactory.getInstance(), true, (short) 1).resetToNull());
        checkNull(factory, new ShortValue.UnsignedShortValue(ValueFactory.getInstance(), true, (short) -1)
                .resetToNull());
    }

    @Test(groups = { "unit" })
    public void testSignedValue() {
        ValueFactory factory = ValueFactory.getInstance();
        checkValue(factory, new ShortValue(ValueFactory.getInstance(), false, (short) 0),
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
                (short) 0, // Object
                "0", // String
                "0", // JSON expression
                "0" // SQL Expression
        );
        checkValue(factory, new ShortValue(ValueFactory.getInstance(), false, (short) 1),
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
                (short) 1, // Object
                "1", // String
                "1", // JSON expression
                "1" // SQL Expression
        );
        checkValue(factory, new ShortValue(ValueFactory.getInstance(), false, (short) -2),
                false, // isInfinity
                false, // isNan
                false, // isNull
                true, // boolean
                (char) (0xFFFF & (short) -2), // char
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
                (short) -2, // Object
                "-2", // String
                "-2", // JSON expression
                "-2" // SQL Expression
        );
    }

    @Test(groups = { "unit" })
    public void testUnsignedValue() {
        ValueFactory factory = ValueFactory.getInstance();
        checkValue(factory, new ShortValue.UnsignedShortValue(ValueFactory.getInstance(), false, (short) 0),
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
                UnsignedShort.ZERO, // Object
                "0", // String
                "0", // JSON expression
                "0" // SQL Expression
        );
        checkValue(factory, new ShortValue.UnsignedShortValue(ValueFactory.getInstance(), false, (short) 1),
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
                UnsignedShort.ONE, // Object
                "1", // String
                "1", // JSON expression
                "1" // SQL Expression
        );
        checkValue(factory, new ShortValue.UnsignedShortValue(ValueFactory.getInstance(), false, (short) -2),
                false, // isInfinity
                false, // isNan
                false, // isNull
                true, // boolean
                (char) (0xFFFF & (short) -2), // char
                (byte) -2, // byte
                (short) -2, // short
                65534, // int
                65534L, // long
                65534F, // float
                65534D, // double
                BigInteger.valueOf(65534L), // BigInteger
                new BigDecimal(65534L), // BigDecimal
                new BigDecimal(BigInteger.valueOf(65534L), 3), // BigDecimal(3)
                IllegalArgumentException.class, // Enum(Format)
                DateTimeParseException.class, // Date
                DateTimeParseException.class, // Time
                DateTimeParseException.class, // DateTime
                DateTimeParseException.class, // DateTime(9)
                UnsignedShort.valueOf((short) -2), // Object
                "65534", // String
                "65534", // JSON expression
                "65534" // SQL Expression
        );
    }
}
