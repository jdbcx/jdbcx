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
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.github.jdbcx.BaseValueTest;
import io.github.jdbcx.Format;
import io.github.jdbcx.ValueFactory;

public class TimeValueTest extends BaseValueTest {
    @Test(groups = { "unit" })
    public void testSet() {
        ValueFactory factory = ValueFactory.getInstance();
        TimeValue value = TimeValue.of(factory, false, 1, null);
        Assert.assertEquals(value.asObject(), factory.getDefaultTime());
        Assert.assertEquals(value.set(LocalTime.ofSecondOfDay(567L)).asObject(), LocalTime.ofSecondOfDay(567L));
        Assert.assertEquals(value.set(null).asObject(), factory.getDefaultTime());

        Properties config = new Properties();
        ValueFactory.OPTION_TIME.setValue(config, "233");
        ValueFactory.OPTION_TIME_SCALE.setValue(config, "2");
        factory = ValueFactory.newInstance(config);
        value = TimeValue.of(factory, false, 1, null);
        Assert.assertEquals(value.asObject(), LocalTime.ofSecondOfDay(233L));
        Assert.assertEquals(value.set(LocalTime.ofSecondOfDay(332L)).asObject(), LocalTime.ofSecondOfDay(332L));
        Assert.assertEquals(value.set(null).asObject(), factory.getDefaultTime());
    }

    @Test(groups = { "unit" })
    public void testScale() {
        final long l = 123123456789L;
        ValueFactory factory = ValueFactory.getInstance();
        TimeValue value = TimeValue.of(factory, false, LocalTime.ofNanoOfDay(l));
        Assert.assertEquals(value.asInt(), (int) (l / 1000000000L));
        Assert.assertEquals(value.asLong(), l / 1000000000L);
        Assert.assertEquals(value.asTime(), LocalTime.ofSecondOfDay(123));

        Properties config = new Properties();
        ValueFactory.OPTION_TIME_SCALE.setValue(config, "2");
        factory = ValueFactory.newInstance(config);
        value = TimeValue.of(factory, false, LocalTime.ofNanoOfDay(l));
        Assert.assertEquals(value.asInt(), (int) (l / 1000000000L));
        Assert.assertEquals(value.asLong(), l / 10000000L);
        Assert.assertEquals(value.asTime(), LocalTime.ofNanoOfDay((long) (123.12 * 1000000000L)));
    }

    @Test(groups = { "unit" })
    public void testValue() {
        ValueFactory factory = ValueFactory.getInstance();
        // null value
        checkNull(factory, TimeValue.of(ValueFactory.getInstance(), true, null));
        checkNull(factory, TimeValue.of(ValueFactory.getInstance(), true, LocalTime.ofSecondOfDay(1)).resetToNull());
        checkNull(factory, new TimeValue(ValueFactory.getInstance(), true, LocalTime.ofNanoOfDay(1L)).resetToNull());

        // non-null
        checkValue(factory, TimeValue.of(ValueFactory.getInstance(), false, LocalTime.ofSecondOfDay(0)),
                false, // isInfinity
                false, // isNan
                false, // isNull
                "00:00:00".getBytes(), // binary
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
                factory.getDefaultDate(), // Date
                factory.getDefaultTime(), // Time
                factory.getDefaultTimestamp(), // DateTime
                factory.getDefaultTimestamp(), // DateTime(9)
                LocalTime.ofNanoOfDay(0L), // Object
                "00:00:00", // String
                "\"00:00:00\"", // JSON expression
                "'00:00:00'" // SQL Expression
        );
        checkValue(factory, new TimeValue(ValueFactory.getInstance(), false, LocalTime.ofSecondOfDay(1)),
                false, // isInfinity
                false, // isNan
                false, // isNull
                "00:00:01".getBytes(), // binary
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
                new BigDecimal(BigInteger.valueOf(1L), 3), // BigDecimal(3)
                Format.TSV, // Enum(Format)
                factory.getDefaultDate(), // Date
                LocalTime.ofSecondOfDay(1L), // Time
                LocalDateTime.of(factory.getDefaultDate(), LocalTime.ofSecondOfDay(1L)), // DateTime
                LocalDateTime.of(factory.getDefaultDate(), LocalTime.ofSecondOfDay(1L)), // DateTime(9)
                LocalTime.ofSecondOfDay(1L), // Object
                "00:00:01", // String
                "\"00:00:01\"", // JSON expression
                "'00:00:01'" // SQL Expression
        );
        // Cannot use -2 to test due to java.time.DateTimeException:
        // Invalid value for SecondOfDay (valid values 0 - 86399): -2
        checkValue(factory, new TimeValue(ValueFactory.getInstance(), false, LocalTime.ofSecondOfDay(86398)),
                false, // isInfinity
                false, // isNan
                false, // isNull
                "23:59:58".getBytes(), // binary
                true, // boolean
                (char) 86398, // char
                (byte) 86398, // byte
                (short) 86398, // short
                86398, // int
                86398L, // long
                86398F, // float
                86398D, // double
                BigInteger.valueOf(86398), // BigInteger
                new BigDecimal("86398"), // BigDecimal
                new BigDecimal(BigInteger.valueOf(86398L), 3), // BigDecimal(3)
                IllegalArgumentException.class, // Enum(Format)
                factory.getDefaultDate(), // Date
                LocalTime.ofSecondOfDay(86398), // Time
                LocalDateTime.of(factory.getDefaultDate(), LocalTime.ofSecondOfDay(86398)), // DateTime
                LocalDateTime.of(factory.getDefaultDate(), LocalTime.ofSecondOfDay(86398)), // DateTime(9)
                LocalTime.ofSecondOfDay(86398), // Object
                "23:59:58", // String
                "\"23:59:58\"", // JSON expression
                "'23:59:58'" // SQL Expression
        );

        checkValue(factory, new TimeValue(ValueFactory.getInstance(), false, 2, LocalTime.ofNanoOfDay(86398123456789L)),
                false, // isInfinity
                false, // isNan
                false, // isNull
                "23:59:58.12".getBytes(), // binary
                true, // boolean
                (char) 86398, // char
                (byte) 86398, // byte
                (short) 86398, // short
                86398, // int
                8639812L, // long
                8639812F, // float
                8639812D, // double
                BigInteger.valueOf(8639812), // BigInteger
                new BigDecimal("86398.12"), // BigDecimal
                new BigDecimal(BigInteger.valueOf(8639812L), 3), // BigDecimal(3)
                IllegalArgumentException.class, // Enum(Format)
                factory.getDefaultDate(), // Date
                LocalTime.ofNanoOfDay(86398120000000L), // Time
                LocalDateTime.of(factory.getDefaultDate(), LocalTime.ofNanoOfDay(86398120000000L)), // DateTime
                LocalDateTime.of(factory.getDefaultDate(), LocalTime.ofNanoOfDay(86398120000000L)), // DateTime(9)
                LocalTime.ofNanoOfDay(86398120000000L), // Object
                "23:59:58.12", // String
                "\"23:59:58.12\"", // JSON expression
                "'23:59:58.12'" // SQL Expression
        );
    }
}
