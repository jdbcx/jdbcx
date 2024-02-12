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
package io.github.jdbcx.data;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.github.jdbcx.BaseValueTest;
import io.github.jdbcx.Format;
import io.github.jdbcx.ValueFactory;

public class DateTimeValueTest extends BaseValueTest {
    @Test(groups = { "unit" })
    public void testSet() {
        ValueFactory factory = ValueFactory.getInstance();
        DateTimeValue value = DateTimeValue.of(factory, false, 1, null);
        LocalDateTime dt = LocalDateTime.ofEpochSecond(567L, 0, factory.getZoneOffset());
        Assert.assertEquals(value.asObject(), factory.getDefaultTimestamp());
        Assert.assertEquals(value.set(dt).asObject(), dt);
        Assert.assertEquals(value.set(null).asObject(), factory.getDefaultTimestamp());

        Properties config = new Properties();
        ValueFactory.OPTION_TIME.setValue(config, "233");
        ValueFactory.OPTION_TIME_SCALE.setValue(config, "2");
        factory = ValueFactory.newInstance(config);
        dt = LocalDateTime.ofEpochSecond(233L, 100000000, factory.getZoneOffset());
        value = DateTimeValue.of(factory, false, 1, dt);
        Assert.assertEquals(value.asObject(), dt);
        dt = LocalDateTime.ofEpochSecond(332L, 200000000, factory.getZoneOffset());
        Assert.assertEquals(value.set(dt).asObject(), dt);
        Assert.assertEquals(value.set(null).asObject(), factory.getDefaultTimestamp());
    }

    @Test(groups = { "unit" })
    public void testScale() {
        final long s = 123L;
        final int n = 123456789;
        ValueFactory factory = ValueFactory.getInstance();
        DateTimeValue value = DateTimeValue.of(factory, false,
                LocalDateTime.ofEpochSecond(s, n, factory.getZoneOffset()));
        Assert.assertEquals(value.asInt(), s);
        Assert.assertEquals(value.asLong(), s);
        Assert.assertEquals(value.asTime(), LocalTime.ofSecondOfDay(s));
        Assert.assertEquals(value.asDateTime(), LocalDateTime.ofEpochSecond(s, 0, factory.getZoneOffset()));

        Properties config = new Properties();
        ValueFactory.OPTION_TIMESTAMP_SCALE.setValue(config, "2");
        factory = ValueFactory.newInstance(config);
        value = DateTimeValue.of(factory, false,
                LocalDateTime.ofEpochSecond(s, n, factory.getZoneOffset()));
        Assert.assertEquals(value.asInt(), s);
        Assert.assertEquals(value.asLong(), s);
        Assert.assertEquals(value.asTime(), LocalTime.ofNanoOfDay(s * 1000000000L + n / 10000000 * 10000000));
        Assert.assertEquals(value.asDateTime(),
                LocalDateTime.ofEpochSecond(s, n / 10000000 * 10000000, factory.getZoneOffset()));
    }

    @Test(groups = { "unit" })
    public void testValue() {
        ValueFactory factory = ValueFactory.getInstance();
        // null value
        checkNull(factory, DateTimeValue.of(ValueFactory.getInstance(), true, null));
        checkNull(factory,
                DateTimeValue.of(ValueFactory.getInstance(), true,
                        LocalDateTime.ofEpochSecond(0L, 0, factory.getZoneOffset())).resetToNull());
        checkNull(factory,
                new DateTimeValue(ValueFactory.getInstance(), true,
                        LocalDateTime.ofEpochSecond(1L, 0, factory.getZoneOffset())).resetToNull());

        // non-null
        LocalDateTime dt = null;
        checkValue(factory,
                DateTimeValue.of(ValueFactory.getInstance(), false,
                        dt = LocalDateTime.ofEpochSecond(0L, 0, factory.getZoneOffset())),
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
                Format.TSV, // Enum(Format)
                factory.getDefaultDate(), // Date
                factory.getDefaultTime(), // Time
                factory.getDefaultTimestamp(), // DateTime
                factory.getDefaultTimestamp(), // DateTime(9)
                dt, // Object
                "1970-01-01 00:00:00", // String
                "\"1970-01-01 00:00:00\"", // JSON expression
                "'1970-01-01 00:00:00'" // SQL Expression
        );
        checkValue(factory,
                new DateTimeValue(ValueFactory.getInstance(), false,
                        dt = LocalDateTime.ofEpochSecond(1L, 0, factory.getZoneOffset())),
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
                new BigDecimal(BigInteger.valueOf(1000L), 3), // BigDecimal(3)
                Format.CSV, // Enum(Format)
                factory.getDefaultDate(), // Date
                LocalTime.ofSecondOfDay(1L), // Time
                LocalDateTime.of(factory.getDefaultDate(), LocalTime.ofSecondOfDay(1L)), // DateTime
                LocalDateTime.of(factory.getDefaultDate(), LocalTime.ofSecondOfDay(1L)), // DateTime(9)
                dt, // Object
                "1970-01-01 00:00:01", // String
                "\"1970-01-01 00:00:01\"", // JSON expression
                "'1970-01-01 00:00:01'" // SQL Expression
        );
        checkValue(factory,
                new DateTimeValue(ValueFactory.getInstance(), false,
                        dt = LocalDateTime.ofEpochSecond(-2L, 0, factory.getZoneOffset())),
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
                new BigDecimal("-2"), // BigDecimal
                new BigDecimal(BigInteger.valueOf(-2000L), 3), // BigDecimal(3)
                IllegalArgumentException.class, // Enum(Format)
                LocalDate.ofEpochDay(-1L), // Date
                LocalTime.ofSecondOfDay(86398), // Time
                dt, // DateTime
                dt, // DateTime(9)
                dt, // Object
                "1969-12-31 23:59:58", // String
                "\"1969-12-31 23:59:58\"", // JSON expression
                "'1969-12-31 23:59:58'" // SQL Expression
        );

        checkValue(factory,
                new DateTimeValue(ValueFactory.getInstance(), false, 2,
                        dt = LocalDateTime.ofEpochSecond(-2L, 123456789, factory.getZoneOffset())),
                false, // isInfinity
                false, // isNan
                false, // isNull
                true, // boolean
                (char) -2, // char
                (byte) -2, // byte
                (short) -2, // short
                -2, // int
                -2L, // long
                -2.12F, // float
                -2.12D, // double
                BigInteger.valueOf(-2L), // BigInteger
                new BigDecimal("-2"), // BigDecimal
                new BigDecimal(BigInteger.valueOf(-2120L), 3), // BigDecimal(3)
                IllegalArgumentException.class, // Enum(Format)
                LocalDate.ofEpochDay(-1L), // Date
                LocalTime.ofNanoOfDay(86398120000000L), // Time
                dt.withNano(120000000), // DateTime
                dt.withNano(120000000), // DateTime(9)
                dt.withNano(120000000), // Object
                "1969-12-31 23:59:58.12", // String
                "\"1969-12-31 23:59:58.12\"", // JSON expression
                "'1969-12-31 23:59:58.12'" // SQL Expression
        );
    }
}
