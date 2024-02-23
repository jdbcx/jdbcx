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

import java.sql.JDBCType;
import java.sql.Types;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.github.jdbcx.value.LongValue;
import io.github.jdbcx.value.StringValue;

public class TypeMappingTest {
    static abstract class AbstractValue implements Value {
    }

    @Test(groups = { "unit" })
    public void testGetJdbcType() {
        Assert.assertNull(TypeMapping.getJdbcType(-999));
        Assert.assertEquals(TypeMapping.getJdbcType(1), JDBCType.CHAR);

        Assert.assertNull(TypeMapping.getJdbcType(null));
        Assert.assertNull(TypeMapping.getJdbcType(""));
        Assert.assertNull(TypeMapping.getJdbcType("xchar"));
        Assert.assertEquals(TypeMapping.getJdbcType("char"), JDBCType.CHAR);
    }

    @Test(groups = { "unit" })
    public void testGetValueType() {
        Assert.assertEquals(TypeMapping.getValueType(null), StringValue.class);
        Assert.assertEquals(TypeMapping.getValueType(""), StringValue.class);
        Assert.assertEquals(TypeMapping.getValueType("long"), StringValue.class);
        Assert.assertEquals(TypeMapping.getValueType(Value.class.getName()), StringValue.class);
        Assert.assertEquals(TypeMapping.getValueType(AbstractValue.class.getName()), StringValue.class);
        Assert.assertEquals(TypeMapping.getValueType(getClass().getName()), StringValue.class);

        Assert.assertEquals(TypeMapping.getValueType("Long"), LongValue.class);
        Assert.assertEquals(TypeMapping.getValueType("LongValue"), LongValue.class);
        Assert.assertEquals(TypeMapping.getValueType(LongValue.class.getName()), LongValue.class);
    }

    @Test(groups = { "unit" })
    public void testDefaultConstructor() {
        Assert.assertThrows(IllegalArgumentException.class,
                () -> new TypeMapping((JDBCType) null, (String) null, (Class<? extends Value>) null));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> new TypeMapping((JDBCType) null, "", (Class<? extends Value>) null));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> new TypeMapping(JDBCType.VARCHAR, "String", null));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> new TypeMapping(JDBCType.VARCHAR, "String", Value.class));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> new TypeMapping(JDBCType.VARCHAR, "String", AbstractValue.class));

        TypeMapping mapping = new TypeMapping(JDBCType.BLOB, null, LongValue.class);
        Assert.assertEquals(mapping.getSourceJdbcType(), JDBCType.BLOB);
        Assert.assertEquals(mapping.getSourceDatabaseType(), null);
        Assert.assertEquals(mapping.getMappedType(), LongValue.class);

        mapping = new TypeMapping(null, "Array", LongValue.class);
        Assert.assertEquals(mapping.getSourceJdbcType(), null);
        Assert.assertEquals(mapping.getSourceDatabaseType(), "Array");
        Assert.assertEquals(mapping.getMappedType(), LongValue.class);

        mapping = new TypeMapping(JDBCType.BLOB, "Array", LongValue.class);
        Assert.assertEquals(mapping.getSourceJdbcType(), JDBCType.BLOB);
        Assert.assertEquals(mapping.getSourceDatabaseType(), "Array");
        Assert.assertEquals(mapping.getMappedType(), LongValue.class);
    }

    @Test(groups = { "unit" })
    public void testOtherConstructors() {
        TypeMapping mapping = new TypeMapping(Types.BLOB, null, LongValue.class);
        Assert.assertEquals(mapping.getSourceJdbcType(), JDBCType.BLOB);
        Assert.assertEquals(mapping.getSourceDatabaseType(), null);
        Assert.assertEquals(mapping.getMappedType(), LongValue.class);

        mapping = new TypeMapping(Types.BLOB, "Array", LongValue.class);
        Assert.assertEquals(mapping.getSourceJdbcType(), JDBCType.BLOB);
        Assert.assertEquals(mapping.getSourceDatabaseType(), "Array");
        Assert.assertEquals(mapping.getMappedType(), LongValue.class);

        mapping = new TypeMapping("Blob", null, null);
        Assert.assertEquals(mapping.getSourceJdbcType(), JDBCType.BLOB);
        Assert.assertEquals(mapping.getSourceDatabaseType(), null);
        Assert.assertEquals(mapping.getMappedType(), StringValue.class);

        mapping = new TypeMapping((String) null, "Int8", null);
        Assert.assertEquals(mapping.getSourceJdbcType(), null);
        Assert.assertEquals(mapping.getSourceDatabaseType(), "Int8");
        Assert.assertEquals(mapping.getMappedType(), StringValue.class);

        mapping = new TypeMapping("INTEGER", "Int8", null);
        Assert.assertEquals(mapping.getSourceJdbcType(), JDBCType.INTEGER);
        Assert.assertEquals(mapping.getSourceDatabaseType(), "Int8");
        Assert.assertEquals(mapping.getMappedType(), StringValue.class);

        mapping = new TypeMapping("INTEGER", "Int8", "Long");
        Assert.assertEquals(mapping.getSourceJdbcType(), JDBCType.INTEGER);
        Assert.assertEquals(mapping.getSourceDatabaseType(), "Int8");
        Assert.assertEquals(mapping.getMappedType(), LongValue.class);

        mapping = new TypeMapping("INTEGER", "Int8", "LongValue");
        Assert.assertEquals(mapping.getSourceJdbcType(), JDBCType.INTEGER);
        Assert.assertEquals(mapping.getSourceDatabaseType(), "Int8");
        Assert.assertEquals(mapping.getMappedType(), LongValue.class);

        mapping = new TypeMapping("INTEGER", "Int8", LongValue.class.getName());
        Assert.assertEquals(mapping.getSourceJdbcType(), JDBCType.INTEGER);
        Assert.assertEquals(mapping.getSourceDatabaseType(), "Int8");
        Assert.assertEquals(mapping.getMappedType(), LongValue.class);

        mapping = new TypeMapping("INTEGER", "Int8", "special");
        Assert.assertEquals(mapping.getSourceJdbcType(), JDBCType.INTEGER);
        Assert.assertEquals(mapping.getSourceDatabaseType(), "Int8");
        Assert.assertEquals(mapping.getMappedType(), StringValue.class);
    }

    @Test(groups = { "unit" })
    public void testAccept() {
        TypeMapping mapping = new TypeMapping("VARCHAR", null, "String");
        Assert.assertFalse(mapping.accept(null, null));
        Assert.assertFalse(mapping.accept(JDBCType.ARRAY, null));
        Assert.assertFalse(mapping.accept(null, "*"));
        Assert.assertTrue(mapping.accept(JDBCType.VARCHAR, null));

        mapping = new TypeMapping(null, "Int8", "String");
        Assert.assertFalse(mapping.accept(null, null));
        Assert.assertFalse(mapping.accept(JDBCType.ARRAY, null));
        Assert.assertFalse(mapping.accept(null, "int8"));
        Assert.assertTrue(mapping.accept(null, "Int8"));
        Assert.assertTrue(mapping.accept(null, "*"));

        mapping = new TypeMapping("JAVA_OBJECT", "Int8", "String");
        Assert.assertFalse(mapping.accept(null, null));
        Assert.assertFalse(mapping.accept(JDBCType.ARRAY, null));
        Assert.assertFalse(mapping.accept(null, "int8"));
        Assert.assertFalse(mapping.accept(JDBCType.JAVA_OBJECT, null));
        Assert.assertTrue(mapping.accept(null, "Int8"));
        Assert.assertTrue(mapping.accept(JDBCType.ARRAY, "*"));
    }
}
