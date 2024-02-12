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

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.gson.Gson;

public class ConverterTest {
    @Test(groups = { "unit" })
    public void testConvertToDateTime() {
        Assert.assertEquals(Converter.toDateTime(null), null);
        Assert.assertEquals(Converter.toDateTime(BigDecimal.valueOf(0L)),
                LocalDateTime.ofEpochSecond(0L, 0, ZoneOffset.UTC));
        Assert.assertEquals(Converter.toDateTime(BigDecimal.valueOf(1L)),
                LocalDateTime.ofEpochSecond(1L, 0, ZoneOffset.UTC));
        for (int i = 1; i < 9; i++) {
            BigDecimal d = BigDecimal.TEN.pow(i);
            Assert.assertEquals(
                    Converter.toDateTime(
                            BigDecimal.valueOf(1L).add(BigDecimal.valueOf(1L).divide(d))),
                    LocalDateTime.ofEpochSecond(1L, BigDecimal.TEN.pow(9 - i).intValue(),
                            ZoneOffset.UTC));
        }

        Assert.assertEquals(Converter.toDateTime(BigDecimal.valueOf(-1L)),
                LocalDateTime.ofEpochSecond(-1L, 0, ZoneOffset.UTC));
        for (int i = 1; i < 9; i++) {
            BigDecimal d = BigDecimal.TEN.pow(i);
            Assert.assertEquals(
                    Converter.toDateTime(
                            BigDecimal.valueOf(-1L).add(BigDecimal.valueOf(-1L).divide(d))),
                    LocalDateTime.ofEpochSecond(-1L, 0, ZoneOffset.UTC)
                            .minus(BigDecimal.TEN.pow(9 - i).longValue(),
                                    ChronoUnit.NANOS));
        }
    }

    @Test(groups = { "unit" })
    public void testConvertToJsonExpression() {
        Gson gson = new Gson();
        String value = null;
        Assert.assertEquals(Converter.toJsonExpression(value), gson.toJson(value));
        Assert.assertEquals(gson.fromJson(value, String.class), value);
        Assert.assertEquals(Converter.toJsonExpression(value = ""), gson.toJson(value));
        Assert.assertEquals(gson.fromJson("\"\"", String.class), value);
        Assert.assertEquals(Converter.toJsonExpression(value = "'"), "\"'\"");
        Assert.assertEquals(gson.fromJson("\"'\"", String.class), value);
        Assert.assertEquals(Converter.toJsonExpression(value = "\t\r\n\\"), gson.toJson(value));
        Assert.assertEquals(gson.fromJson("\"\\t\\r\\n\\\\\"", String.class), value);

        Assert.assertEquals(Converter.toJsonExpression(value = "123🤔一\b二三"), gson.toJson(value));
        Assert.assertEquals(gson.fromJson("\"123🤔一\b二三\"", String.class), value);
    }

    @Test(groups = { "unit" })
    public void testConvertToSqlExpression() {
        Assert.assertEquals(Converter.toSqlExpression(null), "NULL");
        Assert.assertEquals(Converter.toSqlExpression(""), "''");
        Assert.assertEquals(Converter.toSqlExpression("'"), "'\\''");
    }
}
