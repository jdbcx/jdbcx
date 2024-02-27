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

public class DuckDBMapperTest extends ResultMapperTest {
    public DuckDBMapperTest() {
        super(new DuckDBMapper());
    }

    @Test(groups = { "unit" })
    public void testToStructDefinition() {
        Assert.assertEquals(((DuckDBMapper) mapper).toStructDefinition(), "{}");
        Assert.assertEquals(((DuckDBMapper) mapper).toStructDefinition(Field.of("a\"b", JDBCType.BLOB)),
                "{\"a\"\"b\":'BLOB'}");
        Assert.assertEquals(
                ((DuckDBMapper) mapper).toStructDefinition(Field.of("a", JDBCType.INTEGER, true, 24, 0, false),
                        Field.of("b", JDBCType.CHAR, false, 3, 0, false)),
                "{\"a\":'UINTEGER',\"b\":'VARCHAR(3) NOT NULL'}");
    }

    @Test(groups = { "unit" })
    @Override
    public void testToColumnDefinition() {
        Assert.assertEquals(mapper.toColumnDefinition(), "");
        Assert.assertEquals(mapper.toColumnDefinition(Field.of("a\"b", JDBCType.BLOB)), "\"a\"\"b\" BLOB");
        Assert.assertEquals(
                mapper.toColumnDefinition(Field.of("a", JDBCType.INTEGER, true, 24, 0, false),
                        Field.of("b", JDBCType.CHAR, false, 3, 0, false)),
                "\"a\" UINTEGER,\"b\" VARCHAR(3) NOT NULL");
    }

    @Test(groups = { "unit" })
    @Override
    public void testToColumnType() {
        Assert.assertEquals(mapper.toColumnType(Field.of("f", JDBCType.CHAR, false, 3, 0, false)),
                "VARCHAR(3) NOT NULL");
        Assert.assertEquals(mapper.toColumnType(Field.of("f", JDBCType.CHAR)), "VARCHAR");
        Assert.assertEquals(mapper.toColumnType(Field.of("f", JDBCType.TIMESTAMP, false, 0, 5, false)),
                "TIMESTAMP_MS NOT NULL");
    }

    @Test(groups = { "unit" })
    @Override
    public void testToRemoteTable() {
        Assert.assertEquals(mapper.toRemoteTable(null, null, null, null), "'null'");
        Assert.assertEquals(mapper.toRemoteTable("", null, null, null), "''");
        Assert.assertEquals(mapper.toRemoteTable("", null, Compression.NONE, null), "''");
        Assert.assertEquals(mapper.toRemoteTable("", Format.TSV, null, null), "''");
        Assert.assertEquals(mapper.toRemoteTable("", Format.TSV, Compression.NONE, null), "''");

        Result<?> result = Result.of(
                Arrays.asList(Field.of("a\"b", JDBCType.BIT), Field.of("c", JDBCType.DECIMAL, false, 18, 3, true)),
                new Object[0]);
        Assert.assertEquals(mapper.toRemoteTable("", null, null, result), "''");
        Assert.assertEquals(mapper.toRemoteTable("", null, Compression.NONE, result), "''");

        Assert.assertEquals(mapper.toRemoteTable("123", Format.CSV, Compression.NONE, result),
                "read_csv_auto('123',header=true,columns={\"a\"\"b\":'BOOLEAN',\"c\":'DECIMAL(18,3) NOT NULL'})");
        Assert.assertEquals(mapper.toRemoteTable("", Format.TSV, Compression.GZIP, result),
                "read_csv_auto('',delim='\\t',header=true,columns={\"a\"\"b\":'BOOLEAN',\"c\":'DECIMAL(18,3) NOT NULL'},compression='gzip')");
    }
}
