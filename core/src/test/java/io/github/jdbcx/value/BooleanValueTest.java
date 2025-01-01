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

public class BooleanValueTest extends BaseValueTest {
    @Test(groups = { "unit" })
    public void testValue() {
        ValueFactory factory = ValueFactory.getInstance();
        // null value
        checkNull(factory, BooleanValue.of(ValueFactory.getInstance(), true));

        // non-null
        checkValue(factory, BooleanValue.of(ValueFactory.getInstance(), false),
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
                Boolean.FALSE, // Object
                "false", // String
                "false", // JSON expression
                "0" // SQL Expression
        );
        checkValue(factory, new BooleanValue(ValueFactory.getInstance(), false, true),
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
                Boolean.TRUE, // Object
                "true", // String
                "true", // JSON expression
                "1" // SQL Expression
        );
    }
}
