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
package io.github.jdbcx.dialect;

import java.sql.JDBCType;
import java.util.Arrays;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.github.jdbcx.Compression;
import io.github.jdbcx.Field;
import io.github.jdbcx.Format;
import io.github.jdbcx.Result;
import io.github.jdbcx.ResultMapperTest;

public class ClickHouseMapperTest extends ResultMapperTest {
    public ClickHouseMapperTest() {
        super(new ClickHouseMapper());
    }

    @Test(groups = { "unit" })
    @Override
    public void testQuoteIdentifier() {
        Assert.assertEquals(mapper.quoteIdentifier(null), "`null`");
        Assert.assertEquals(mapper.quoteIdentifier(""), "``");
        Assert.assertEquals(mapper.quoteIdentifier("123"), "`123`");
        Assert.assertEquals(mapper.quoteIdentifier("1`3"), "`1\\`3`");
        Assert.assertEquals(mapper.quoteIdentifier("1\\3"), "`1\\\\3`");
        Assert.assertEquals(mapper.quoteIdentifier("1\"3"), "`1\"3`");
    }

    @Test(groups = { "unit" })
    @Override
    public void testToColumnDefinition() {
        Assert.assertEquals(mapper.toColumnDefinition(), "");
        Assert.assertEquals(mapper.toColumnDefinition(Field.of("a`b", JDBCType.BLOB)), "`a\\`b` Nullable(String)");
        Assert.assertEquals(
                mapper.toColumnDefinition(Field.of("a", null, JDBCType.INTEGER, true, 24, 0, false),
                        Field.of("b", null, JDBCType.CHAR, false, 3, 0, false)),
                "`a` Nullable(UInt32),`b` FixedString(3)");
    }

    @Test(groups = { "unit" })
    @Override
    public void testToColumnType() {
        Assert.assertEquals(mapper.toColumnType(Field.of("f", JDBCType.CHAR, false)), "String");
        Assert.assertEquals(mapper.toColumnType(Field.of("f", JDBCType.CHAR)), "Nullable(String)");

        Assert.assertEquals(mapper.toColumnType(Field.of("f", null, JDBCType.TIMESTAMP, false, 0, 5, false)),
                "DateTime64(5)");
    }

    @Test(groups = { "unit" })
    @Override
    public void testToRemoteTable() {
        Assert.assertEquals(mapper.toRemoteTable(null, null, null, null), "url('null',LineAsString)");
        Assert.assertEquals(mapper.toRemoteTable("", null, null, null), "url('',LineAsString)");
        Assert.assertEquals(mapper.toRemoteTable("", null, Compression.NONE, null), "url('',LineAsString)");
        Assert.assertEquals(mapper.toRemoteTable("", Format.TSV, null, null),
                "url('',TSVWithNames,headers('accept'='text/tab-separated-values'))");
        Assert.assertEquals(mapper.toRemoteTable("", Format.TSV, Compression.NONE, null),
                "url('',TSVWithNames,headers('accept'='text/tab-separated-values'))");

        Result<?> result = Result.of(
                Arrays.asList(Field.of("a`b", JDBCType.BIT), Field.of("c", null, JDBCType.DECIMAL, false, 18, 3, true)),
                new Object[0]);
        Assert.assertEquals(mapper.toRemoteTable("", null, null, result),
                "url('',CSVWithNames,'`a\\\\`b` Nullable(Boolean),`c` Decimal(18,3)')");
        Assert.assertEquals(mapper.toRemoteTable("", null, Compression.NONE, result),
                "url('',CSVWithNames,'`a\\\\`b` Nullable(Boolean),`c` Decimal(18,3)')");

        Assert.assertEquals(mapper.toRemoteTable("123", Format.CSV, Compression.NONE, result),
                "url('123',CSVWithNames,'`a\\\\`b` Nullable(Boolean),`c` Decimal(18,3)',headers('accept'='text/csv'))");
        Assert.assertEquals(mapper.toRemoteTable("", Format.TSV, Compression.GZIP, result),
                "url('',TSVWithNames,'`a\\\\`b` Nullable(Boolean),`c` Decimal(18,3)',headers('accept'='text/tab-separated-values','accept-encoding'='gzip'))");

        Assert.assertEquals(mapper.toRemoteTable("", Format.AVRO, Compression.NONE, result),
                "url('',Avro,'`a\\\\`b` Nullable(Boolean),`c` Decimal(18,3)',headers('accept'='avro/binary'))");
        Assert.assertEquals(mapper.toRemoteTable("", Format.ARROW, Compression.NONE, result),
                "url('',Arrow,'`a\\\\`b` Nullable(Boolean),`c` Decimal(18,3)',headers('accept'='application/vnd.apache.arrow.file'))");
        Assert.assertEquals(mapper.toRemoteTable("", Format.ARROW_STREAM, Compression.NONE, result),
                "url('',ArrowStream,'`a\\\\`b` Nullable(Boolean),`c` Decimal(18,3)',headers('accept'='application/vnd.apache.arrow.stream'))");
    }
}
