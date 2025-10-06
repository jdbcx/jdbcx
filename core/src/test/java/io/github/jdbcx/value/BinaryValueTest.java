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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.format.DateTimeParseException;

import org.junit.Assert;
import org.testng.annotations.Test;

import com.mysql.cj.Constants;

import io.github.jdbcx.BaseValueTest;
import io.github.jdbcx.Format;
import io.github.jdbcx.ValueFactory;

public class BinaryValueTest extends BaseValueTest {
    @Test(groups = { "unit" })
    public void testNullValue() {
        ValueFactory factory = ValueFactory.getInstance();
        checkNull(factory, BinaryValue.of(ValueFactory.getInstance(), true, (byte[]) null));
        checkNull(factory, BinaryValue.of(ValueFactory.getInstance(), true, (String) null));
        checkNull(factory, BinaryValue.of(ValueFactory.getInstance(), true, (InputStream) null));

        checkNull(factory, BinaryValue.of(ValueFactory.getInstance(), true, new byte[] { 1, 3, 5 }).resetToNull());
        checkNull(factory, BinaryValue.of(ValueFactory.getInstance(), true, "xyz").resetToNull());
        checkNull(factory, BinaryValue
                .of(ValueFactory.getInstance(), true, new ByteArrayInputStream(new byte[] { 1, 3, 5 })).resetToNull());

        checkNull(factory,
                new BinaryValue(ValueFactory.getInstance(), true, new ByteArrayInputStream(Constants.EMPTY_BYTE_ARRAY))
                        .resetToNull());
    }

    @Test(groups = { "unit" })
    public void testValue() {
        ValueFactory factory = ValueFactory.getInstance();
        byte[] bytes;
        ByteArrayInputStream is = new ByteArrayInputStream(bytes = Constants.EMPTY_BYTE_ARRAY);
        checkValue(factory,
                new BinaryValue(ValueFactory.getInstance(), false, is),
                false, // isInfinity
                false, // isNan
                false, // isNull
                bytes, // binary
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
                is, // Object
                "", // String
                "[]", // JSON expression
                "''" // SQL Expression
        );

        Assert.assertArrayEquals(
                BinaryValue.of(ValueFactory.getInstance(), false,
                        is = new ByteArrayInputStream(bytes = new byte[] { 0x31 })).asBinary(),
                bytes);
        Assert.assertEquals(
                BinaryValue.of(ValueFactory.getInstance(), false, is = new ByteArrayInputStream(bytes)).isInfinity(),
                false);
        Assert.assertEquals(
                BinaryValue.of(ValueFactory.getInstance(), false, is = new ByteArrayInputStream(bytes)).isNaN(), false);
        Assert.assertEquals(
                BinaryValue.of(ValueFactory.getInstance(), false, is = new ByteArrayInputStream(bytes)).isNull(),
                false);
        Assert.assertEquals(
                BinaryValue.of(ValueFactory.getInstance(), false, is = new ByteArrayInputStream(bytes)).asBoolean(),
                true);
        Assert.assertEquals(
                BinaryValue.of(ValueFactory.getInstance(), false, is = new ByteArrayInputStream(bytes)).asByte(),
                (byte) 1);
        Assert.assertEquals(
                BinaryValue.of(ValueFactory.getInstance(), false, is = new ByteArrayInputStream(bytes)).asShort(),
                (short) 1);
        Assert.assertEquals(
                BinaryValue.of(ValueFactory.getInstance(), false, is = new ByteArrayInputStream(bytes)).asInt(), 1);
        Assert.assertEquals(
                BinaryValue.of(ValueFactory.getInstance(), false, is = new ByteArrayInputStream(bytes)).asLong(), 1L);
        Assert.assertEquals(
                BinaryValue.of(ValueFactory.getInstance(), false, is = new ByteArrayInputStream(bytes)).asFloat(), 1F,
                0F);
        Assert.assertEquals(
                BinaryValue.of(ValueFactory.getInstance(), false, is = new ByteArrayInputStream(bytes)).asDouble(), 1D,
                0D);
        Assert.assertEquals(
                BinaryValue.of(ValueFactory.getInstance(), false, is = new ByteArrayInputStream(bytes)).asBigInteger(),
                BigInteger.ONE);
        Assert.assertEquals(
                BinaryValue.of(ValueFactory.getInstance(), false, is = new ByteArrayInputStream(bytes)).asBigDecimal(),
                new BigDecimal(1));
        Assert.assertEquals(
                BinaryValue.of(ValueFactory.getInstance(), false, is = new ByteArrayInputStream(bytes)).asBigDecimal(3),
                new BigDecimal(BigInteger.ONE, 3));
        Assert.assertEquals(BinaryValue.of(ValueFactory.getInstance(), false, is = new ByteArrayInputStream(bytes))
                .asEnum(Format.class), Format.TSV);
        Assert.assertEquals(
                BinaryValue.of(ValueFactory.getInstance(), false, is = new ByteArrayInputStream(bytes)).asObject(), is);
        Assert.assertEquals(
                BinaryValue.of(ValueFactory.getInstance(), false, is = new ByteArrayInputStream(bytes)).asString(),
                "1");
        Assert.assertEquals(
                BinaryValue.of(ValueFactory.getInstance(), false, is = new ByteArrayInputStream(bytes))
                        .toJsonExpression(),
                "[49]");
        Assert.assertEquals(
                BinaryValue.of(ValueFactory.getInstance(), false, is = new ByteArrayInputStream(bytes))
                        .toSqlExpression(),
                "'31'");

        Assert.assertArrayEquals(
                BinaryValue.of(ValueFactory.getInstance(), false,
                        is = new ByteArrayInputStream(bytes = new byte[] { 0x2D, 0x32 })).asBinary(),
                bytes);
        Assert.assertEquals(
                BinaryValue.of(ValueFactory.getInstance(), false, is = new ByteArrayInputStream(bytes)).asString(),
                "-2");
        Assert.assertEquals(
                BinaryValue.of(ValueFactory.getInstance(), false, is = new ByteArrayInputStream(bytes))
                        .toJsonExpression(),
                "[45, 50]");
        Assert.assertEquals(
                BinaryValue.of(ValueFactory.getInstance(), false, is = new ByteArrayInputStream(bytes))
                        .toSqlExpression(),
                "'2D32'");
    }
}
