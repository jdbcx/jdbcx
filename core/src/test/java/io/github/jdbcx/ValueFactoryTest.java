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
package io.github.jdbcx;

import java.io.StringReader;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.sql.JDBCType;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;

import io.github.jdbcx.value.StringValue;

public class ValueFactoryTest {
    @Test(groups = { "unit" })
    public void testGetInstance() {
        Assert.assertTrue(ValueFactory.getInstance() == ValueFactory.getInstance());

        Properties config = new Properties();
        ValueFactory.OPTION_CHARSET.setValue(config, null);
        ValueFactory.OPTION_ROUNDING_MODE.setValue(config, null);
        ValueFactory.OPTION_TIMEZONE.setValue(config, null);
        ValueFactory.OPTION_OFFSET.setValue(config, null);
        ValueFactory.OPTION_DECIMAL_SCALE.setValue(config, null);
        ValueFactory.OPTION_TIME_SCALE.setValue(config, null);
        ValueFactory.OPTION_TIMESTAMP_SCALE.setValue(config, null);
        ValueFactory.OPTION_DATE_FORMAT.setValue(config, null);
        ValueFactory.OPTION_TIME_FORMAT.setValue(config, null);
        ValueFactory.OPTION_TIMESTAMP_FORMAT.setValue(config, null);

        ValueFactory.OPTION_BOOLEAN.setValue(config, null);
        ValueFactory.OPTION_CHAR.setValue(config, null);
        ValueFactory.OPTION_BYTE.setValue(config, null);
        ValueFactory.OPTION_SHORT.setValue(config, null);
        ValueFactory.OPTION_INT.setValue(config, null);
        ValueFactory.OPTION_LONG.setValue(config, null);
        ValueFactory.OPTION_FLOAT.setValue(config, null);
        ValueFactory.OPTION_DOUBLE.setValue(config, null);
        ValueFactory.OPTION_BIGINT.setValue(config, null);
        ValueFactory.OPTION_DECIMAL.setValue(config, null);
        ValueFactory.OPTION_DATE.setValue(config, null);
        ValueFactory.OPTION_TIME.setValue(config, null);
        ValueFactory.OPTION_TIMESTAMP.setValue(config, null);
        ValueFactory.OPTION_STRING.setValue(config, null);

        ValueFactory factory = ValueFactory.newInstance(config);
        Assert.assertEquals(ValueFactory.getInstance().getCharset(), factory.getCharset());
        Assert.assertEquals(ValueFactory.getInstance().getRoundingMode(), factory.getRoundingMode());
        Assert.assertEquals(ValueFactory.getInstance().getTimeZone(), factory.getTimeZone());
        Assert.assertEquals(ValueFactory.getInstance().getZoneId(), factory.getZoneId());
        Assert.assertEquals(ValueFactory.getInstance().getZoneOffset(), factory.getZoneOffset());
        Assert.assertEquals(ValueFactory.getInstance().getDecimalScale(), factory.getDecimalScale());
        Assert.assertEquals(ValueFactory.getInstance().getTimeScale(), factory.getTimeScale());
        Assert.assertEquals(ValueFactory.getInstance().getTimestampScale(), factory.getTimestampScale());
        Assert.assertNotEquals(ValueFactory.getInstance().getDateFormatter(), factory.getDateFormatter());
        Assert.assertNotEquals(ValueFactory.getInstance().getTimeFormatter(), factory.getTimeFormatter());
        Assert.assertNotEquals(ValueFactory.getInstance().getTimestampFormatter(), factory.getTimestampFormatter());
        final LocalDate localDate = LocalDate.now();
        final LocalTime localTime = LocalTime.now();
        final LocalDateTime localDateTime = LocalDateTime.now();
        Assert.assertEquals(ValueFactory.getInstance().getDateFormatter().format(localDate),
                factory.getDateFormatter().format(localDate));
        Assert.assertEquals(ValueFactory.getInstance().getTimeFormatter().format(localTime),
                factory.getTimeFormatter().format(localTime));
        Assert.assertEquals(ValueFactory.getInstance().getTimestampFormatter().format(localDateTime),
                factory.getTimestampFormatter().format(localDateTime));

        Assert.assertEquals(ValueFactory.getInstance().getDefaultBoolean(), factory.getDefaultBoolean());
        Assert.assertEquals(ValueFactory.getInstance().getDefaultChar(), factory.getDefaultChar());
        Assert.assertEquals(ValueFactory.getInstance().getDefaultByte(), factory.getDefaultByte());
        Assert.assertEquals(ValueFactory.getInstance().getDefaultShort(), factory.getDefaultShort());
        Assert.assertEquals(ValueFactory.getInstance().getDefaultInt(), factory.getDefaultInt());
        Assert.assertEquals(ValueFactory.getInstance().getDefaultLong(), factory.getDefaultLong());
        Assert.assertEquals(ValueFactory.getInstance().getDefaultFloat(), factory.getDefaultFloat());
        Assert.assertEquals(ValueFactory.getInstance().getDefaultDouble(), factory.getDefaultDouble());
        Assert.assertEquals(ValueFactory.getInstance().getDefaultBigInteger(), factory.getDefaultBigInteger());
        Assert.assertEquals(ValueFactory.getInstance().getDefaultBigDecimal(), factory.getDefaultBigDecimal());
        Assert.assertEquals(ValueFactory.getInstance().getDefaultDate(), factory.getDefaultDate());
        Assert.assertEquals(ValueFactory.getInstance().getDefaultTime(), factory.getDefaultTime());
        Assert.assertEquals(ValueFactory.getInstance().getDefaultTimestamp(), factory.getDefaultTimestamp());
        Assert.assertEquals(ValueFactory.getInstance().getDefaultString(), factory.getDefaultString());

        Assert.assertEquals(ValueFactory.getInstance().getMappings(), factory.getMappings());
    }

    @Test(groups = { "unit" })
    public void testFlatMapFromJson() {
        Assert.assertEquals(ValueFactory.flatMapFromJson("null"), null);
        Assert.assertEquals(ValueFactory.flatMapFromJson("{}"), Collections.emptyMap());
        Assert.assertEquals(ValueFactory.flatMapFromJson("{\"a\":1}"), Collections.singletonMap("a", "1"));

        Assert.assertThrows(IllegalStateException.class, () -> ValueFactory.flatMapFromJson("{\"a\":{\"b\":2}}"));
    }

    @Test(groups = { "unit" })
    public void testJson() {
        Assert.assertEquals(ValueFactory.toJson(null), "null");
        Assert.assertEquals(ValueFactory.fromJson("null", JsonElement.class).isJsonNull(), true);
        Assert.assertEquals(ValueFactory.fromJson("null", JsonNull.class).isJsonNull(), true);

        Assert.assertEquals(ValueFactory.toJson(1), "1");
        Assert.assertEquals(ValueFactory.fromJson("1", JsonElement.class).getAsInt(), 1);

        final String json = "{\"name\":\"a\",\"value\":1}";
        Assert.assertEquals(ValueFactory.fromJson(json, JsonObject.class).get("name").getAsString(), "a");
        Assert.assertEquals(ValueFactory.fromJson(new StringReader(json), JsonObject.class).get("name").getAsString(),
                "a");

        final Type mapType = new TypeToken<Map<String, Object>>() {
        }.getType();
        Map<String, Object> map = new HashMap<>();
        map.put("name", "a");
        map.put("value", 1D);
        Assert.assertEquals(ValueFactory.fromJson(json, mapType), map);
        Assert.assertEquals(ValueFactory.fromJson(new StringReader(json), mapType), map);
    }

    @Test(groups = { "unit" })
    public void testNewFormatter() {
        Assert.assertEquals(ValueFactory.newFormatter("yyyy-MM-dd").format(LocalDate.of(2024, 5, 4)), "2024-05-04");

        Assert.assertEquals(ValueFactory.newFormatter("HH:mm:ss").format(LocalTime.of(12, 34, 56)), "12:34:56");
        Assert.assertEquals(ValueFactory.newFormatter("HH:mm:ss").format(LocalTime.of(12, 34, 56, 987654321)),
                "12:34:56.987654321");
        Assert.assertEquals(ValueFactory.newFormatter("HH:mm:ss.S").format(LocalTime.of(12, 34, 56, 987654321)),
                "12:34:56.9");

        Assert.assertEquals(
                ValueFactory.newFormatter("yyyy-MM-dd HH:mm:ss").format(LocalDateTime.of(2024, 5, 4, 12, 34, 56)),
                "2024-05-04 12:34:56");
        Assert.assertEquals(
                ValueFactory.newFormatter("yyyy-MM-dd HH:mm:ss")
                        .format(LocalDateTime.of(2024, 5, 4, 12, 34, 56, 987654321)),
                "2024-05-04 12:34:56.987654321");
        Assert.assertEquals(
                ValueFactory.newFormatter("yyyy-MM-dd HH:mm:ss.SS")
                        .format(LocalDateTime.of(2024, 5, 4, 12, 34, 56, 987654321)),
                "2024-05-04 12:34:56.98");
    }

    @Test(groups = { "unit" })
    public void testNewInstance() {
        final LocalDate localDate = LocalDate.of(2045, 1, 23);
        final LocalTime localTime = LocalTime.of(9, 10, 11, 12000);
        final LocalDateTime localDateTime = LocalDateTime.of(localDate, localTime);

        final TypeMapping[] mappings = new TypeMapping[] {
                new TypeMapping(null, "*", "String"),
                new TypeMapping(JDBCType.ARRAY, null, StringValue.class),
                new TypeMapping("BOOLEAN", "bool", "Boolean"),
        };

        Properties config = new Properties();
        ValueFactory.OPTION_CHARSET.setValue(config, StandardCharsets.ISO_8859_1.name());
        ValueFactory.OPTION_ROUNDING_MODE.setValue(config, RoundingMode.HALF_DOWN.name());
        ValueFactory.OPTION_TIMEZONE.setValue(config, "Asia/Chongqing");
        ValueFactory.OPTION_OFFSET.setValue(config, "+08:00");
        ValueFactory.OPTION_DECIMAL_SCALE.setValue(config, "5");
        ValueFactory.OPTION_TIME_SCALE.setValue(config, "2");
        ValueFactory.OPTION_TIMESTAMP_SCALE.setValue(config, "6");
        ValueFactory.OPTION_DATE_FORMAT.setValue(config, "MM/dd/yy");
        ValueFactory.OPTION_TIME_FORMAT.setValue(config, "HH:mm:ss.SS");
        ValueFactory.OPTION_TIMESTAMP_FORMAT.setValue(config, "MM/dd/yyyy HH:mm:ss.SSSSSS");

        ValueFactory.OPTION_BOOLEAN.setValue(config, "false");
        ValueFactory.OPTION_CHAR.setValue(config, "W");
        ValueFactory.OPTION_BYTE.setValue(config, "3");
        ValueFactory.OPTION_SHORT.setValue(config, "4");
        ValueFactory.OPTION_INT.setValue(config, "5");
        ValueFactory.OPTION_LONG.setValue(config, "6");
        ValueFactory.OPTION_FLOAT.setValue(config, "7.8");
        ValueFactory.OPTION_DOUBLE.setValue(config, "9.10");
        ValueFactory.OPTION_BIGINT.setValue(config, "1234567890");
        ValueFactory.OPTION_DECIMAL.setValue(config, "123456789.987654321");
        ValueFactory.OPTION_DATE.setValue(config, "2");
        ValueFactory.OPTION_TIME.setValue(config, "233");
        ValueFactory.OPTION_TIMESTAMP.setValue(config, "2333333");
        ValueFactory.OPTION_STRING.setValue(config, "<new>");

        ValueFactory factory = ValueFactory.newInstance(config, mappings);
        Assert.assertEquals(factory.getCharset(), StandardCharsets.ISO_8859_1);
        Assert.assertEquals(factory.getRoundingMode(), RoundingMode.HALF_DOWN);
        Assert.assertEquals(factory.getTimeZone(), TimeZone.getTimeZone("Asia/Chongqing"));
        Assert.assertEquals(factory.getZoneId(), TimeZone.getTimeZone("Asia/Chongqing").toZoneId());
        Assert.assertEquals(factory.getZoneOffset(), ZoneOffset.of("+08:00"));
        Assert.assertEquals(factory.getDecimalScale(), 5);
        Assert.assertEquals(factory.getTimeScale(), 2);
        Assert.assertEquals(factory.getTimestampScale(), 6);
        Assert.assertEquals(factory.getDateFormatter().format(localDate), "01/23/45");
        Assert.assertEquals(factory.getTimeFormatter().format(localTime),
                "09:10:11.00");
        Assert.assertEquals(factory.getTimestampFormatter().format(localDateTime),
                "01/23/2045 09:10:11.000012");

        Assert.assertEquals(factory.getDefaultBoolean(), false);
        Assert.assertEquals(factory.getDefaultChar(), 'W');
        Assert.assertEquals(factory.getDefaultByte(), (byte) 3);
        Assert.assertEquals(factory.getDefaultShort(), (short) 4);
        Assert.assertEquals(factory.getDefaultInt(), 5);
        Assert.assertEquals(factory.getDefaultLong(), 6L);
        Assert.assertEquals(factory.getDefaultFloat(), 7.8F);
        Assert.assertEquals(factory.getDefaultDouble(), 9.10D);
        Assert.assertEquals(factory.getDefaultBigInteger(), new BigInteger("1234567890"));
        Assert.assertEquals(factory.getDefaultBigDecimal(), new BigDecimal("123456789.987654321"));
        Assert.assertEquals(factory.getDefaultDate(), LocalDate.ofEpochDay(2L));
        Assert.assertEquals(factory.getDefaultTime(), LocalTime.ofSecondOfDay(233));
        Assert.assertEquals(factory.getDefaultTimestamp(), LocalDateTime.ofEpochSecond(2333333L, 0, ZoneOffset.UTC));
        Assert.assertEquals(factory.getDefaultString(), "<new>");

        Assert.assertEquals(factory.getMappings(), Arrays.asList(mappings));
    }
}
