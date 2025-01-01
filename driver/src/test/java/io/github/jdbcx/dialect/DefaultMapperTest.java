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
package io.github.jdbcx.dialect;

import java.sql.JDBCType;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.github.jdbcx.Field;
import io.github.jdbcx.ResultMapperTest;

public class DefaultMapperTest extends ResultMapperTest {
    public DefaultMapperTest() {
        super(new DefaultMapper());
    }

    @Test(groups = { "unit" })
    @Override
    public void testToColumnDefinition() {
        Assert.assertEquals(mapper.toColumnDefinition(), "");
        Assert.assertEquals(mapper.toColumnDefinition(Field.of("a\"b", JDBCType.BLOB)), "\"a\"\"b\" VARCHAR NULL");
        Assert.assertEquals(
                mapper.toColumnDefinition(Field.of("a", null, JDBCType.INTEGER, true, 24, 0, false),
                        Field.of("b", null, JDBCType.CHAR, false, 3, 0, false)),
                "\"a\" INTEGER NULL,\"b\" CHAR(3) NOT NULL");
    }

    @Test(groups = { "unit" })
    @Override
    public void testToColumnType() {
        Assert.assertEquals(mapper.toColumnType(Field.of("f", null, JDBCType.CHAR, false, 3, 0, false)),
                "CHAR(3) NOT NULL");
        Assert.assertEquals(mapper.toColumnType(Field.of("f", JDBCType.CHAR)), "VARCHAR NULL");
        Assert.assertEquals(mapper.toColumnType(Field.of("f", null, JDBCType.TIMESTAMP, false, 0, 5, false)),
                "DATETIME NOT NULL");
    }
}
