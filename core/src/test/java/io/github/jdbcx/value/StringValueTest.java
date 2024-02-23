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
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.LocalDateTime;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.github.jdbcx.BaseValueTest;
import io.github.jdbcx.Format;
import io.github.jdbcx.ValueFactory;

public class StringValueTest extends BaseValueTest {
    @Test(groups = { "unit" })
    public void testStringWithLength() {
        ValueFactory factory = ValueFactory.getInstance();
        StringValue value = StringValue.of(factory, true, 3, (String) null);
        Assert.assertEquals(value.asString(), factory.getDefaultString());
        value = StringValue.of(ValueFactory.getInstance(), false, 3, (String) null);
        Assert.assertEquals(value.asString(), factory.getDefaultString(3));
        value = StringValue.of(ValueFactory.getInstance(), true, 3, "1");
        Assert.assertEquals(value.asString(), "1\0\0");
        value = StringValue.of(ValueFactory.getInstance(), false, 3, "123");
        Assert.assertEquals(value.asString(), "123");
        value = StringValue.of(ValueFactory.getInstance(), false, 3, "1234");
        Assert.assertEquals(value.asString(), "123");
    }

    @Test(groups = { "unit" })
    public void testValue() {
        ValueFactory factory = ValueFactory.getInstance();
        // null value
        checkNull(factory, StringValue.of(ValueFactory.getInstance(), true, 0));
        checkNull(factory, StringValue.of(ValueFactory.getInstance(), true, 7, "1234567").resetToNull());

        // non-null
        checkValue(factory, StringValue.of(factory, false, 0, (String) null),
                NumberFormatException.class, // isInfinity
                NumberFormatException.class, // isNan
                false, // isNull
                false, // boolean
                NumberFormatException.class, // char
                NumberFormatException.class, // byte
                NumberFormatException.class, // short
                NumberFormatException.class, // int
                NumberFormatException.class, // long
                NumberFormatException.class, // float
                NumberFormatException.class, // double
                NumberFormatException.class, // BigInteger
                NumberFormatException.class, // BigDecimal
                NumberFormatException.class, // BigDecimal(3)
                NumberFormatException.class, // Enum(Format)

                DateTimeParseException.class, // Date
                DateTimeParseException.class, // Time
                DateTimeParseException.class, // DateTime
                DateTimeParseException.class, // DateTime(9)

                factory.getDefaultString(), // Object
                factory.getDefaultString(), // String
                "\"\"", // JSON expression
                "''" // SQL Expression
        );

        // numbers
        checkValue(factory, StringValue.of(factory, false, 0, "0"),
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
                BigDecimal.valueOf(0L), // BigDecimal
                new BigDecimal(BigInteger.ZERO, 3), // BigDecimal(3)
                Format.CSV, // Enum(Format)
                DateTimeParseException.class, // Date
                DateTimeParseException.class, // Time
                DateTimeParseException.class, // DateTime
                DateTimeParseException.class, // DateTime(9)
                "0", // Object
                "0", // String
                "\"0\"", // JSON Expression
                "'0'" // SQL Expression
        );
        checkValue(factory, StringValue.of(factory, false, 0, "1"),
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
                BigDecimal.valueOf(1L), // BigDecimal
                new BigDecimal(BigInteger.ONE, 3), // BigDecimal(3)
                Format.TSV, // Enum(Format)
                DateTimeParseException.class, // Date
                DateTimeParseException.class, // Time
                DateTimeParseException.class, // DateTime
                DateTimeParseException.class, // DateTime(9)
                "1", // Object
                "1", // String
                "\"1\"", // JSON Expression
                "'1'" // SQL Expression
        );
        checkValue(factory, StringValue.of(factory, false, 1, "2"),
                false, // isInfinity
                false, // isNan
                false, // isNull
                IllegalArgumentException.class, // boolean
                '\2', // char
                (byte) 2, // byte
                (short) 2, // short
                2, // int
                2L, // long
                2F, // float
                2D, // double
                BigInteger.valueOf(2L), // BigInteger
                BigDecimal.valueOf(2L), // BigDecimal
                new BigDecimal(BigInteger.valueOf(2L), 3), // BigDecimal(3)
                Format.TXT, // Enum(Format)
                DateTimeParseException.class, // Date
                DateTimeParseException.class, // Time
                DateTimeParseException.class, // DateTime
                DateTimeParseException.class, // DateTime(9)
                "2", // Object
                "2", // String
                "\"2\"", // JSON Expression
                "'2'" // SQL Expression
        );
        checkValue(factory, StringValue.of(factory, false, 2, "-1"),
                false, // isInfinity
                false, // isNan
                false, // isNull
                IllegalArgumentException.class, // boolean
                (char) (0xFFFF & (short) -1), // char
                (byte) -1, // byte
                (short) -1, // short
                -1, // int
                -1L, // long
                -1F, // float
                -1D, // double
                BigInteger.valueOf(-1L), // BigInteger
                BigDecimal.valueOf(-1L), // BigDecimal
                new BigDecimal(BigInteger.valueOf(-1L), 3), // BigDecimal(3)
                IllegalArgumentException.class, // Enum(Format)
                DateTimeParseException.class, // Date
                DateTimeParseException.class, // Time
                DateTimeParseException.class, // DateTime
                DateTimeParseException.class, // DateTime(9)
                "-1", // Object
                "-1", // String
                "\"-1\"", // JSON Expression
                "'-1'" // SQL Expression
        );

        // date, time, timestamp
        checkValue(factory, StringValue.of(factory, false, 10, "2024-02-11"),
                NumberFormatException.class, // isInfinity
                NumberFormatException.class, // isNan
                false, // isNull
                IllegalArgumentException.class, // boolean
                NumberFormatException.class, // char
                NumberFormatException.class, // byte
                NumberFormatException.class, // short
                NumberFormatException.class, // int
                NumberFormatException.class, // long
                NumberFormatException.class, // float
                NumberFormatException.class, // double
                NumberFormatException.class, // BigInteger
                NumberFormatException.class, // BigDecimal
                NumberFormatException.class, // BigDecimal(3)
                IllegalArgumentException.class, // Enum(Format)
                LocalDate.of(2024, 2, 11), // Date
                DateTimeParseException.class, // Time
                DateTimeParseException.class, // DateTime
                DateTimeParseException.class, // DateTime(9)
                "2024-02-11", // Object
                "2024-02-11", // String
                "\"2024-02-11\"", // JSON Expression
                "'2024-02-11'" // SQL Expression
        );
        checkValue(factory, StringValue.of(factory, false, 8, "08:54:50"),
                NumberFormatException.class, // isInfinity
                NumberFormatException.class, // isNan
                false, // isNull
                IllegalArgumentException.class, // boolean
                NumberFormatException.class, // char
                NumberFormatException.class, // byte
                NumberFormatException.class, // short
                NumberFormatException.class, // int
                NumberFormatException.class, // long
                NumberFormatException.class, // float
                NumberFormatException.class, // double
                NumberFormatException.class, // BigInteger
                NumberFormatException.class, // BigDecimal
                NumberFormatException.class, // BigDecimal(3)
                IllegalArgumentException.class, // Enum(Format)
                DateTimeParseException.class, // Date
                LocalTime.of(8, 54, 50), // Time
                DateTimeParseException.class, // DateTime
                DateTimeParseException.class, // DateTime(9)
                "08:54:50", // Object
                "08:54:50", // String
                "\"08:54:50\"", // JSON Expression
                "'08:54:50'" // SQL Expression
        );
        checkValue(factory, StringValue.of(factory, false, 0, "2024-02-11 08:54:50.123456789"),
                NumberFormatException.class, // isInfinity
                NumberFormatException.class, // isNan
                false, // isNull
                IllegalArgumentException.class, // boolean
                NumberFormatException.class, // char
                NumberFormatException.class, // byte
                NumberFormatException.class, // short
                NumberFormatException.class, // int
                NumberFormatException.class, // long
                NumberFormatException.class, // float
                NumberFormatException.class, // double
                NumberFormatException.class, // BigInteger
                NumberFormatException.class, // BigDecimal
                NumberFormatException.class, // BigDecimal(3)
                IllegalArgumentException.class, // Enum(Format)
                DateTimeParseException.class, // Date
                DateTimeParseException.class, // Time
                LocalDateTime.of(2024, 2, 11, 8, 54, 50), // DateTime
                LocalDateTime.of(2024, 2, 11, 8, 54, 50, 123456789), // DateTime(9)
                "2024-02-11 08:54:50.123456789", // Object
                "2024-02-11 08:54:50.123456789", // String
                "\"2024-02-11 08:54:50.123456789\"", // JSON Expression
                "'2024-02-11 08:54:50.123456789'" // SQL Expression
        );
    }
}
