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
import java.time.format.DateTimeParseException;

import org.testng.annotations.Test;

import io.github.jdbcx.BaseValueTest;
import io.github.jdbcx.Format;
import io.github.jdbcx.ValueFactory;

public class FloatValueTest extends BaseValueTest {
    @Test(groups = { "unit" })
    public void testValue() {
        ValueFactory factory = ValueFactory.getInstance();
        // null value
        checkNull(factory, FloatValue.of(ValueFactory.getInstance(), true));
        checkNull(factory, new FloatValue(ValueFactory.getInstance(), true, 1.0F).resetToNull());

        // non-null
        checkValue(factory, FloatValue.of(ValueFactory.getInstance(), false),
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
                Float.valueOf(0F), // Object
                "0.0", // String
                "0.0", // JSON expression
                "0.0" // SQL Expression
        );
        checkValue(factory, new FloatValue(ValueFactory.getInstance(), false, 0F),
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
                Float.valueOf(0F), // Object
                "0.0", // String
                "0.0", // JSON expression
                "0.0" // SQL Expression
        );
        checkValue(factory, new FloatValue(ValueFactory.getInstance(), false, 1F),
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
                Float.valueOf(1F), // Object
                "1.0", // String
                "1.0", // JSON expression
                "1.0" // SQL Expression
        );
        checkValue(factory, new FloatValue(ValueFactory.getInstance(), false, -2F),
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
                BigInteger.valueOf(-2), // BigInteger
                new BigDecimal("-2.0"), // BigDecimal
                new BigDecimal(BigInteger.valueOf(-2), 3), // BigDecimal(3)
                IllegalArgumentException.class, // Enum(Format)
                DateTimeParseException.class, // Date
                DateTimeParseException.class, // Time
                DateTimeParseException.class, // DateTime
                DateTimeParseException.class, // DateTime(9)
                Float.valueOf(-2F), // Object
                "-2.0", // String
                "-2.0", // JSON expression
                "-2.0" // SQL Expression
        );
    }
}
