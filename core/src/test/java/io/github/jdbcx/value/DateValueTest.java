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
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.github.jdbcx.BaseValueTest;
import io.github.jdbcx.Format;
import io.github.jdbcx.ValueFactory;

public class DateValueTest extends BaseValueTest {
    @Test(groups = { "unit" })
    public void testSet() {
        ValueFactory factory = ValueFactory.getInstance();
        DateValue value = DateValue.of(factory, false, null);
        Assert.assertEquals(value.asObject(), factory.getDefaultDate());
        Assert.assertEquals(value.set(LocalDate.ofEpochDay(567L)).asObject(), LocalDate.ofEpochDay(567L));
        Assert.assertEquals(value.set(null).asObject(), factory.getDefaultDate());

        Properties config = new Properties();
        ValueFactory.OPTION_DATE.setValue(config, "233");
        factory = ValueFactory.newInstance(config);
        value = DateValue.of(factory, false, null);
        Assert.assertEquals(value.asObject(), LocalDate.ofEpochDay(233L));
        Assert.assertEquals(value.set(LocalDate.ofEpochDay(332L)).asObject(), LocalDate.ofEpochDay(332L));
        Assert.assertEquals(value.set(null).asObject(), factory.getDefaultDate());
    }

    @Test(groups = { "unit" })
    public void testValue() {
        ValueFactory factory = ValueFactory.getInstance();
        // null value
        checkNull(factory, DateValue.of(ValueFactory.getInstance(), true, null));
        checkNull(factory, new DateValue(ValueFactory.getInstance(), true, LocalDate.ofEpochDay(321L)).resetToNull());

        // non-null
        checkValue(factory, DateValue.of(ValueFactory.getInstance(), false, LocalDate.ofEpochDay(0L)),
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
                BigDecimal.ZERO, // BigDecimal
                new BigDecimal(BigInteger.ZERO, 3), // BigDecimal(3)
                Format.CSV, // Enum(Format)
                LocalDate.ofEpochDay(0L), // Date
                factory.getDefaultTime(), // Time
                factory.getDefaultTimestamp(), // DateTime
                factory.getDefaultTimestamp(), // DateTime(9)
                LocalDate.ofEpochDay(0L), // Object
                "1970-01-01", // String
                "\"1970-01-01\"", // JSON expression
                "'1970-01-01'" // SQL Expression
        );
        checkValue(factory, new DateValue(ValueFactory.getInstance(), false, LocalDate.ofEpochDay(1L)),
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
                BigDecimal.ONE, // BigDecimal
                new BigDecimal(BigInteger.ONE, 3), // BigDecimal(3)
                Format.TSV, // Enum(Format)
                LocalDate.ofEpochDay(1L), // Date
                factory.getDefaultTime(), // Time
                LocalDateTime.of(LocalDate.ofEpochDay(1L), factory.getDefaultTime()), // DateTime
                LocalDateTime.of(LocalDate.ofEpochDay(1L), factory.getDefaultTime()), // DateTime(9)
                LocalDate.ofEpochDay(1L), // Object
                "1970-01-02", // String
                "\"1970-01-02\"", // JSON expression
                "'1970-01-02'" // SQL Expression
        );
        checkValue(factory, new DateValue(ValueFactory.getInstance(), false, LocalDate.ofEpochDay(-2L)),
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
                new BigDecimal("-2"), // BigDecimal
                new BigDecimal(BigInteger.valueOf(-2), 3), // BigDecimal(3)
                IllegalArgumentException.class, // Enum(Format)
                LocalDate.ofEpochDay(-2L), // Date
                factory.getDefaultTime(), // Time
                LocalDateTime.of(LocalDate.ofEpochDay(-2L), factory.getDefaultTime()), // DateTime
                LocalDateTime.of(LocalDate.ofEpochDay(-2L), factory.getDefaultTime()), // DateTime(9)
                LocalDate.ofEpochDay(-2L), // Object
                "1969-12-30", // String
                "\"1969-12-30\"", // JSON expression
                "'1969-12-30'" // SQL Expression
        );
    }
}
