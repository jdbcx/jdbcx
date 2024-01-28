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

public class FieldTest {
    @Test(groups = { "unit" })
    public void testConstructor() {
        Assert.assertThrows(IllegalArgumentException.class, () -> Field.of(null));
        Assert.assertThrows(IllegalArgumentException.class, () -> Field.of(null, Types.VARCHAR));
        Assert.assertThrows(IllegalArgumentException.class, () -> Field.of(null, JDBCType.VARCHAR));
        Assert.assertThrows(IllegalArgumentException.class, () -> Field.of(null, JDBCType.VARCHAR, false));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> Field.of(null, JDBCType.VARCHAR, true, -1, -1, false));

        Assert.assertEquals(Field.of("f1"), Field.of("f1", Types.VARCHAR));

        Field f = Field.of("f2", JDBCType.SMALLINT, false, 1, 1, false);
        Assert.assertEquals(f.name(), "f2");
        Assert.assertEquals(f.type(), JDBCType.SMALLINT);
        Assert.assertEquals(f.isNullable(), false);
        Assert.assertEquals(f.precision(), 1);
        Assert.assertEquals(f.scale(), 0);
        Assert.assertEquals(f.isSigned(), false);

        f = Field.of("f3", JDBCType.LONGNVARCHAR, true, 100, 2, true);
        Assert.assertEquals(f.name(), "f3");
        Assert.assertEquals(f.type(), JDBCType.LONGNVARCHAR);
        Assert.assertEquals(f.isNullable(), true);
        Assert.assertEquals(f.precision(), 0);
        Assert.assertEquals(f.scale(), 0);
        Assert.assertEquals(f.isSigned(), false);
    }
}
