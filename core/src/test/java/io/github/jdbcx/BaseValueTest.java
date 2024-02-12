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
package io.github.jdbcx;

import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.function.Supplier;

import org.testng.Assert;
import org.testng.annotations.DataProvider;

public abstract class BaseValueTest {
    @SuppressWarnings("unchecked")
    protected void checkValueOrException(Supplier<?> actual, Object expected, String name) {
        if (expected == null) {
            Assert.assertNull(actual.get(), name);
        } else if (expected instanceof Class && Throwable.class.isAssignableFrom((Class<?>) expected)) {
            Assert.assertThrows(Utils.format("%s should throw [%s]", name, expected), (Class<Throwable>) expected,
                    () -> actual.get());
        } else if (expected instanceof String) {
            Assert.assertEquals(String.valueOf(actual.get()), (String) expected, name);
        } else {
            Assert.assertEquals(actual.get(), expected, name);
        }
    }

    protected void checkNull(ValueFactory factory, Value v) {
        checkNull(factory, v, true, 3, 9);
    }

    protected void checkNull(ValueFactory factory, Value v, boolean nullable, int bigDecimalScale, int timestampScale) {
        Assert.assertEquals(v.getFactory(), factory);

        Assert.assertFalse(v.isInfinity(), "isInfinity");
        Assert.assertFalse(v.isNaN(), "isNaN");
        Assert.assertTrue(v.isNull(), "isNull");

        Assert.assertEquals(v.asBoolean(), false, "asBoolean");
        Assert.assertEquals(v.asChar(), '\0', "asChar");
        Assert.assertEquals(v.asByte(), (byte) 0, "asByte");
        Assert.assertEquals(v.asShort(), (short) 0, "asShort");
        Assert.assertEquals(v.asInt(), 0, "asInt");
        Assert.assertEquals(v.asLong(), 0L, "asLong");
        Assert.assertEquals(v.asFloat(), 0F, "asFloat");
        Assert.assertEquals(v.asDouble(), 0D, "asDouble");

        Assert.assertNull(v.asBigInteger(), "asBigInteger");
        Assert.assertNull(v.asBigDecimal(), "asBigDecimal");
        Assert.assertNull(v.asBigDecimal(bigDecimalScale), "asBigDecimal(" + bigDecimalScale + ")");

        Assert.assertNull(v.asEnum(Format.class), "asEnum(Format)");

        Assert.assertNull(v.asSqlDate(), "asSqlDate");
        Assert.assertNull(v.asSqlTime(), "asSqlTime");
        Assert.assertNull(v.asSqlTimestamp(), "asSqlTimestamp");
        Assert.assertNull(v.asDate(), "asDate");
        Assert.assertNull(v.asTime(), "asTime");
        Assert.assertNull(v.asTime(timestampScale), "asTime(" + timestampScale + ")");
        Assert.assertNull(v.asDateTime(), "asDateTime");
        Assert.assertNull(v.asDateTime(timestampScale), "asDateTime(" + timestampScale + ")");
        Assert.assertNull(v.asInstant(), "asInstant");
        Assert.assertNull(v.asInstant(timestampScale), "asInstant(" + timestampScale + ")");

        if (nullable) {
            Assert.assertNull(v.asObject(), "asObject");
            Assert.assertNull(v.asObject(Object.class), "asObject(Object)");
            Assert.assertEquals(v.toJsonExpression(), Constants.NULL_STR, "toJsonExpression");
            Assert.assertEquals(v.toSqlExpression(), Constants.NULL_EXPR, "toSqlExpression");
        } else {
            Assert.assertNotNull(v.asObject(), "asObject");
            Assert.assertNotNull(v.asObject(Object.class), "asObject(Object)");
            Assert.assertNotEquals(v.toJsonExpression(), Constants.NULL_STR, "toJsonExpression");
            Assert.assertNotEquals(v.toSqlExpression(), Constants.NULL_EXPR, "toSqlExpression");
        }
        Assert.assertNotNull(v.asString(), "asString");
        Assert.assertNotNull(v.asString(null), "asString(null)");
        Assert.assertNotNull(v.asString(StandardCharsets.ISO_8859_1), "asString(ISO_8859_1)");
        Assert.assertNotNull(v.asAsciiString(), "asAsciiString");
        Assert.assertNotNull(v.asUnicodeString(), "asUnicodeString");
        Assert.assertNotNull(v.toJsonExpression(), "toJsonExpression");
        Assert.assertNotNull(v.toSqlExpression(), "toSqlExpression");
    }

    protected void checkValue(ValueFactory factory, Value v, Object... expected) {
        checkValue(factory, v, 3, 9, expected);
    }

    protected void checkValue(ValueFactory factory, Value v, int bigDecimalScale, int timestampScale,
            Object... expected) {
        Assert.assertEquals(v.getFactory(), factory);

        int i = 0;
        checkValueOrException(v::isInfinity, expected[i++], "isInfinity");
        checkValueOrException(v::isNaN, expected[i++], "isNaN");
        checkValueOrException(v::isNull, expected[i++], "isNull");

        checkValueOrException(v::asBoolean, expected[i++], "asBoolean");
        checkValueOrException(v::asChar, expected[i++], "asChar");
        checkValueOrException(v::asByte, expected[i++], "asByte");
        checkValueOrException(v::asShort, expected[i++], "asShort");
        checkValueOrException(v::asInt, expected[i++], "asInt");
        checkValueOrException(v::asLong, expected[i++], "asLong");
        Object nanValue = expected[i++];
        if (nanValue != null) { // skip NaN
            checkValueOrException(v::asFloat, nanValue, "asFloat");
        }
        nanValue = expected[i++];
        if (nanValue != null) { // skip NaN
            checkValueOrException(v::asDouble, nanValue, "asDouble");
        }

        checkValueOrException(v::asBigInteger, expected[i++], "asBigInteger");
        checkValueOrException(v::asBigDecimal, expected[i++], "asBigDecimal");
        checkValueOrException(() -> v.asBigDecimal(bigDecimalScale), expected[i++],
                "asBigDecimal(" + bigDecimalScale + ")");

        checkValueOrException(() -> v.asEnum(Format.class), expected[i++], "asEnum(Format)");

        // checkValueOrException(v::asSqlDate, expected[i++], "asSqlDate");
        // checkValueOrException(v::asSqlTime, expected[i++], "asSqlTime");
        // checkValueOrException(v::asSqlTimestamp, expected[i++], "asSqlTimestamp");
        checkValueOrException(v::asDate, expected[i++], "asDate");
        checkValueOrException(v::asTime, expected[i++], "asTime");
        checkValueOrException(v::asDateTime, expected[i++], "asDateTime");
        checkValueOrException(() -> v.asDateTime(timestampScale), expected[i++], "asDateTime(" + timestampScale + ")");

        checkValueOrException(v::asObject, expected[i++], "asObject");
        checkValueOrException(v::asString, expected[i++], "asString");
        checkValueOrException(v::toJsonExpression, expected[i++], "toJsonExpression");
        checkValueOrException(v::toSqlExpression, expected[i++], "toSqlExpression");
    }

    protected Object getReturnValue(Supplier<?> func) {
        try {
            return func.get();
        } catch (Throwable t) {
            return t.getClass().getName() + ':' + t.getMessage();
        }
    }

    protected <T extends Enum<T>> void sameValue(ValueFactory factory, Value v1, Value v2, Class<T> enumType,
            int bigDecimalScale, int timestampScale) {
        Assert.assertFalse(v1 == v2, "v1 and v2 are supposed to be two different instances");

        Assert.assertEquals(v1.getFactory(), factory);
        Assert.assertEquals(v2.getFactory(), factory);

        Assert.assertEquals(getReturnValue(v1::isInfinity), getReturnValue(v2::isInfinity));
        Assert.assertEquals(getReturnValue(v1::isNaN), getReturnValue(v2::isNaN));
        Assert.assertEquals(getReturnValue(v1::isNull), getReturnValue(v2::isNull));

        Assert.assertEquals(getReturnValue(v1::asBoolean), getReturnValue(v2::asBoolean));
        Assert.assertEquals(getReturnValue(v1::asChar), getReturnValue(v2::asChar));
        Assert.assertEquals(getReturnValue(v1::asByte), getReturnValue(v2::asByte));
        Assert.assertEquals(getReturnValue(v1::asShort), getReturnValue(v2::asShort));
        Assert.assertEquals(getReturnValue(v1::asInt), getReturnValue(v2::asInt));
        Assert.assertEquals(getReturnValue(v1::asLong), getReturnValue(v2::asLong));
        Assert.assertEquals(getReturnValue(v1::asFloat), getReturnValue(v2::asFloat));
        Assert.assertEquals(getReturnValue(v1::asDouble), getReturnValue(v2::asDouble));

        Assert.assertEquals(getReturnValue(v1::asBigDecimal), getReturnValue(v2::asBigDecimal));
        Assert.assertEquals(getReturnValue(() -> v1.asBigDecimal(bigDecimalScale)),
                getReturnValue(() -> v2.asBigDecimal(bigDecimalScale)));
        Assert.assertEquals(getReturnValue(v1::asBigInteger), getReturnValue(v2::asBigInteger));

        Assert.assertEquals(getReturnValue(() -> v1.asEnum(enumType)), getReturnValue(() -> v2.asEnum(enumType)));

        Assert.assertEquals(getReturnValue(v1::asSqlDate), getReturnValue(v2::asSqlDate));
        Assert.assertEquals(getReturnValue(v1::asSqlTime), getReturnValue(v2::asSqlTime));
        Assert.assertEquals(getReturnValue(v1::asSqlTimestamp), getReturnValue(v2::asSqlTimestamp));
        Assert.assertEquals(getReturnValue(v1::asDate), getReturnValue(v2::asDate));
        Assert.assertEquals(getReturnValue(v1::asTime), getReturnValue(v2::asTime));
        Assert.assertEquals(getReturnValue(v1::asDateTime), getReturnValue(v2::asDateTime));
        Assert.assertEquals(getReturnValue(() -> v1.asDateTime(timestampScale)),
                getReturnValue(() -> v2.asDateTime(timestampScale)));

        Assert.assertEquals(getReturnValue(v1::asObject), getReturnValue(v2::asObject));
        Assert.assertEquals(getReturnValue(v1::asString), getReturnValue(v2::asString));
        Assert.assertEquals(getReturnValue(v1::toJsonExpression), getReturnValue(v2::toJsonExpression));
        Assert.assertEquals(getReturnValue(v1::toSqlExpression), getReturnValue(v2::toSqlExpression));
    }

    @DataProvider(name = "factoryProvider")
    public Object[][] getFactories() {
        Properties config = new Properties();
        ValueFactory.OPTION_CHARSET.setValue(config, null);
        ValueFactory.OPTION_ROUNDING_MODE.setValue(config, null);
        ValueFactory.OPTION_TIMEZONE.setValue(config, null);
        ValueFactory.OPTION_TIME_SCALE.setValue(config, null);
        ValueFactory.OPTION_TIMESTAMP_SCALE.setValue(config, null);
        ValueFactory.OPTION_DATE_FORMAT.setValue(config, null);
        ValueFactory.OPTION_TIME_FORMAT.setValue(config, null);
        ValueFactory.OPTION_TIMESTAMP_FORMAT.setValue(config, null);

        ValueFactory factory1 = ValueFactory.newInstance(config);
        return new Object[][] { new Object[] { ValueFactory.getInstance() }, new Object[] { "2", false, false, false },
                new Object[] { "NaN", false, true, false }, new Object[] { "-Infinity", false, false, true },
                new Object[] { "Infinity", false, false, true }, new Object[] { "+Infinity", false, false, true } };
    }
}
