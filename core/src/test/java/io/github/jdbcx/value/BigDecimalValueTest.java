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
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.github.jdbcx.BaseValueTest;
import io.github.jdbcx.Format;
import io.github.jdbcx.ValueFactory;

public class BigDecimalValueTest extends BaseValueTest {
    @Test(groups = { "unit" })
    public void testSet() {
        ValueFactory factory = ValueFactory.getInstance();
        BigDecimalValue value = BigDecimalValue.of(factory, false, 1, null);
        Assert.assertEquals(value.asObject(), factory.getDefaultBigDecimal());
        Assert.assertEquals(value.set(BigDecimal.valueOf(567L)).asObject(), BigDecimal.valueOf(567L));
        Assert.assertEquals(value.set(null).asObject(), factory.getDefaultBigDecimal());

        Properties config = new Properties();
        ValueFactory.OPTION_DECIMAL.setValue(config, "233.332");
        ValueFactory.OPTION_DECIMAL_SCALE.setValue(config, "2");
        factory = ValueFactory.newInstance(config);
        value = BigDecimalValue.of(factory, false, 1, null);
        Assert.assertEquals(value.asObject(), new BigDecimal("233.332"));
        Assert.assertEquals(value.set(new BigDecimal("123.332")).asObject(), new BigDecimal("123.332"));
        Assert.assertEquals(value.set(null).asObject(), factory.getDefaultBigDecimal());
    }

    @Test(groups = { "unit" })
    public void testScale() {
        final double d = 123.456789D;
        ValueFactory factory = ValueFactory.getInstance();
        BigDecimalValue value = BigDecimalValue.of(factory, false, BigDecimal.valueOf(d));
        Assert.assertEquals(value.asBigDecimal(),
                BigDecimal.valueOf(d).setScale(factory.getDecimalScale(), factory.getRoundingMode()));

        Properties config = new Properties();
        ValueFactory.OPTION_DECIMAL_SCALE.setValue(config, "2");
        ValueFactory.OPTION_ROUNDING_MODE.setValue(config, "UP");
        factory = ValueFactory.newInstance(config);
        value = BigDecimalValue.of(factory, false, BigDecimal.valueOf(d));
        Assert.assertEquals(value.asBigDecimal(),
                BigDecimal.valueOf(d).setScale(factory.getDecimalScale(), factory.getRoundingMode()));
    }

    @Test(groups = { "unit" })
    public void testValue() {
        ValueFactory factory = ValueFactory.getInstance();
        // null value
        checkNull(factory, BigDecimalValue.of(ValueFactory.getInstance(), true, null));
        checkNull(factory, new BigDecimalValue(ValueFactory.getInstance(), true, BigDecimal.ONE).resetToNull());

        // non-null
        checkValue(factory, BigDecimalValue.of(ValueFactory.getInstance(), false, BigDecimal.ZERO),
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
                BigDecimal.ZERO, // BigDecimal
                new BigDecimal(BigInteger.ZERO, 3), // BigDecimal(3)
                Format.CSV, // Enum(Format)
                DateTimeParseException.class, // Date
                DateTimeParseException.class, // Time
                DateTimeParseException.class, // DateTime
                DateTimeParseException.class, // DateTime(9)
                BigDecimal.ZERO, // Object
                "0", // String
                "0", // JSON expression
                "0" // SQL Expression
        );
        checkValue(factory, new BigDecimalValue(ValueFactory.getInstance(), false, BigDecimal.ONE),
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
                BigDecimal.ONE, // BigDecimal
                new BigDecimal(BigInteger.valueOf(1000L), 3), // BigDecimal(3)
                Format.TSV, // Enum(Format)
                DateTimeParseException.class, // Date
                DateTimeParseException.class, // Time
                DateTimeParseException.class, // DateTime
                DateTimeParseException.class, // DateTime(9)
                BigDecimal.ONE, // Object
                "1", // String
                "1", // JSON expression
                "1" // SQL Expression
        );
        checkValue(factory, new BigDecimalValue(ValueFactory.getInstance(), false, BigDecimal.valueOf(-2L)),
                false, // isInfinity
                false, // isNan
                false, // isNull
                new byte[] { 0x2D, 0x32 }, // binary
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
                new BigDecimal(BigInteger.valueOf(-2000L), 3), // BigDecimal(3)
                IllegalArgumentException.class, // Enum(Format)
                DateTimeParseException.class, // Date
                DateTimeParseException.class, // Time
                DateTimeParseException.class, // DateTime
                DateTimeParseException.class, // DateTime(9)
                BigDecimal.valueOf(-2L), // Object
                "-2", // String
                "-2", // JSON expression
                "-2" // SQL Expression
        );
    }
}
