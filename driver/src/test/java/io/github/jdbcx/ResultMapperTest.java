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
package io.github.jdbcx;

import java.sql.JDBCType;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ResultMapperTest {
    static final class TestMapper implements ResultMapper {
        @Override
        public String toColumnType(Field f) {
            return f.type().name();
        }
    }

    protected final ResultMapper mapper;

    protected ResultMapperTest(ResultMapper mapper) {
        this.mapper = mapper;
    }

    public ResultMapperTest() {
        this(new TestMapper());
    }

    @Test(groups = { "unit" })
    public void testQuoteIdentifier() {
        Assert.assertEquals(mapper.quoteIdentifier(null), "\"null\"");
        Assert.assertEquals(mapper.quoteIdentifier(""), "\"\"");
        Assert.assertEquals(mapper.quoteIdentifier("123"), "\"123\"");
        Assert.assertEquals(mapper.quoteIdentifier("1\"3"), "\"1\"\"3\"");
        Assert.assertEquals(mapper.quoteIdentifier("1\\3"), "\"1\\3\"");
        Assert.assertEquals(mapper.quoteIdentifier("1`3"), "\"1`3\"");
    }

    @Test(groups = { "unit" })
    public void testToColumnType() {
        for (JDBCType t : JDBCType.values()) {
            Assert.assertEquals(mapper.toColumnType(Field.of("a\"b", t)), t.getName());
        }
    }

    @Test(groups = { "unit" })
    public void testToColumnDefinition() {
        Assert.assertEquals(mapper.toColumnDefinition(), "");
        Assert.assertEquals(mapper.toColumnDefinition(Field.of("a\"b", JDBCType.BLOB)), "\"a\"\"b\" BLOB");
        Assert.assertEquals(mapper.toColumnDefinition(Field.of("a", JDBCType.INTEGER), Field.of("b", JDBCType.VARCHAR)),
                "\"a\" INTEGER,\"b\" VARCHAR");
    }

    @Test(groups = { "unit" })
    public void testToRemoteTable() {
        Assert.assertEquals(mapper.toRemoteTable(null, null, null, null), "'null'");
        Assert.assertEquals(mapper.toRemoteTable("", null, null, null), "''");
        Assert.assertEquals(mapper.toRemoteTable("http://localhost:8080/", null, null, null),
                "'http://localhost:8080/'");
    }
}
