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
import java.time.format.DateTimeParseException;

import org.testng.annotations.Test;

import io.github.jdbcx.BaseValueTest;
import io.github.jdbcx.Format;
import io.github.jdbcx.ValueFactory;

public class ByteValueTest extends BaseValueTest {
    @Test(groups = { "unit" })
    public void testNullValue() {
        ValueFactory factory = ValueFactory.getInstance();
        checkNull(factory, ByteValue.of(ValueFactory.getInstance(), true, true));
        checkNull(factory, ByteValue.of(ValueFactory.getInstance(), true, false));

        checkNull(factory, new ByteValue(ValueFactory.getInstance(), true, (byte) 0).resetToNull());
        checkNull(factory, new ByteValue.UnsignedByteValue(ValueFactory.getInstance(), true, (byte) 0).resetToNull());

        checkNull(factory, new ByteValue(ValueFactory.getInstance(), true, (byte) 1).resetToNull());
        checkNull(factory, new ByteValue.UnsignedByteValue(ValueFactory.getInstance(), true, (byte) -1).resetToNull());
    }

    @Test(groups = { "unit" })
    public void testSignedValue() {
        ValueFactory factory = ValueFactory.getInstance();
        checkValue(factory, new ByteValue(ValueFactory.getInstance(), false, (byte) 0),
                false, // isInfinity
                false, // isNan
                false, // isNull
                new byte[] { 0x30 }, // binary
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
                (byte) 0, // Object
                "0", // String
                "0", // JSON expression
                "0" // SQL Expression
        );
        checkValue(factory, new ByteValue(ValueFactory.getInstance(), false, (byte) 1),
                false, // isInfinity
                false, // isNan
                false, // isNull
                new byte[] { 0x31 }, // binary
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
                (byte) 1, // Object
                "1", // String
                "1", // JSON expression
                "1" // SQL Expression
        );
        checkValue(factory, new ByteValue(ValueFactory.getInstance(), false, (byte) -2),
                false, // isInfinity
                false, // isNan
                false, // isNull
                new byte[] { 0x2D, 0x32 }, // binary
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
                (byte) -2, // Object
                "-2", // String
                "-2", // JSON expression
                "-2" // SQL Expression
        );
    }

    @Test(groups = { "unit" })
    public void testUnsignedValue() {
        ValueFactory factory = ValueFactory.getInstance();
        checkValue(factory, new ByteValue.UnsignedByteValue(ValueFactory.getInstance(), false, (byte) 0),
                false, // isInfinity
                false, // isNan
                false, // isNull
                new byte[] { 0x30 }, // binary
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
                UnsignedByte.ZERO, // Object
                "0", // String
                "0", // JSON expression
                "0" // SQL Expression
        );
        checkValue(factory, new ByteValue.UnsignedByteValue(ValueFactory.getInstance(), false, (byte) 1),
                false, // isInfinity
                false, // isNan
                false, // isNull
                new byte[] { 0x31 }, // binary
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
                UnsignedByte.ONE, // Object
                "1", // String
                "1", // JSON expression
                "1" // SQL Expression
        );
        checkValue(factory, new ByteValue.UnsignedByteValue(ValueFactory.getInstance(), false, (byte) -2),
                false, // isInfinity
                false, // isNan
                false, // isNull
                new byte[] { 0x32, 0x35, 0x34 }, // binary
                true, // boolean
                (char) (0xFF & (byte) -2), // char
                (byte) -2, // byte
                (short) 254, // short
                254, // int
                254L, // long
                254F, // float
                254D, // double
                BigInteger.valueOf(254L), // BigInteger
                new BigDecimal(254L), // BigDecimal
                new BigDecimal(BigInteger.valueOf(254L), 3), // BigDecimal(3)
                IllegalArgumentException.class, // Enum(Format)
                DateTimeParseException.class, // Date
                DateTimeParseException.class, // Time
                DateTimeParseException.class, // DateTime
                DateTimeParseException.class, // DateTime(9)
                UnsignedByte.valueOf((byte) -2), // Object
                "254", // String
                "254", // JSON expression
                "254" // SQL Expression
        );
    }
}
