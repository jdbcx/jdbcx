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
import java.time.format.DateTimeParseException;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.github.jdbcx.BaseValueTest;
import io.github.jdbcx.Format;
import io.github.jdbcx.ValueFactory;

public class BigIntegerValueTest extends BaseValueTest {
    @Test(groups = { "unit" })
    public void testSet() {
        ValueFactory factory = ValueFactory.getInstance();
        BigIntegerValue value = BigIntegerValue.of(factory, false, null);
        Assert.assertEquals(value.asObject(), factory.getDefaultBigInteger());
        Assert.assertEquals(value.set(BigInteger.valueOf(567L)).asObject(), BigInteger.valueOf(567L));
        Assert.assertEquals(value.set(null).asObject(), factory.getDefaultBigInteger());

        Properties config = new Properties();
        ValueFactory.OPTION_BIGINT.setValue(config, "233");
        factory = ValueFactory.newInstance(config);
        value = BigIntegerValue.of(factory, false, null);
        Assert.assertEquals(value.asObject(), new BigInteger("233"));
        Assert.assertEquals(value.set(new BigInteger("332")).asObject(), new BigInteger("332"));
        Assert.assertEquals(value.set(null).asObject(), factory.getDefaultBigInteger());
    }

    @Test(groups = { "unit" })
    public void testValue() {
        ValueFactory factory = ValueFactory.getInstance();
        // null value
        checkNull(factory, BigIntegerValue.of(ValueFactory.getInstance(), true, null));
        checkNull(factory, new BigIntegerValue(ValueFactory.getInstance(), true, BigInteger.ONE).resetToNull());

        // non-null
        checkValue(factory, BigIntegerValue.of(ValueFactory.getInstance(), false, BigInteger.ZERO),
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
                DateTimeParseException.class, // Date
                DateTimeParseException.class, // Time
                DateTimeParseException.class, // DateTime
                DateTimeParseException.class, // DateTime(9)
                BigInteger.ZERO, // Object
                "0", // String
                "0", // JSON expression
                "0" // SQL Expression
        );
        checkValue(factory, new BigIntegerValue(ValueFactory.getInstance(), false, BigInteger.ONE),
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
                DateTimeParseException.class, // Date
                DateTimeParseException.class, // Time
                DateTimeParseException.class, // DateTime
                DateTimeParseException.class, // DateTime(9)
                BigInteger.ONE, // Object
                "1", // String
                "1", // JSON expression
                "1" // SQL Expression
        );
        checkValue(factory, new BigIntegerValue(ValueFactory.getInstance(), false, BigInteger.valueOf(-2L)),
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
                DateTimeParseException.class, // Date
                DateTimeParseException.class, // Time
                DateTimeParseException.class, // DateTime
                DateTimeParseException.class, // DateTime(9)
                BigInteger.valueOf(-2L), // Object
                "-2", // String
                "-2", // JSON expression
                "-2" // SQL Expression
        );
    }
}
