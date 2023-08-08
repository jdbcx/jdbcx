/*
 * Copyright 2022-2023, Zhichun Wu
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
package io.github.jdbcx.executor.jdbc;

import java.sql.JDBCType;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.github.jdbcx.Field;
import io.github.jdbcx.Row;
import io.github.jdbcx.data.StringValue;

public class ReadOnlyResultSetTest {
    @Test(groups = { "unit" })
    public void testEmptyResults() throws SQLException {
        ReadOnlyResultSet rs = new ReadOnlyResultSet(null, Collections.emptyList());
        Assert.assertFalse(rs.next(), "Should not have more rows");
        Assert.assertFalse(rs.isBeforeFirst(), "Should not be before the first");
        Assert.assertFalse(rs.isFirst(), "Should not be the first");
        Assert.assertFalse(rs.isLast(), "Should not be the last");
        Assert.assertTrue(rs.isAfterLast(), "Should be after the last");
        Assert.assertThrows(SQLException.class, () -> rs.getString(0));
        Assert.assertThrows(SQLException.class, () -> rs.getString(1));
        Assert.assertThrows(SQLException.class, () -> rs.getString(2));
        Assert.assertThrows(SQLException.class, () -> rs.getString(3));
    }

    @Test(groups = { "unit" })
    public void testMetaData() throws SQLException {
        Field[] fields = new Field[] { Field.of("a", JDBCType.VARCHAR), Field.of("b", JDBCType.VARCHAR) };
        List<Row> rows = Arrays.asList(new Row[] {
                Row.of(fields, new StringValue("a1"), new StringValue("b1")),
                Row.of(fields, new StringValue("a2"), new StringValue("b2")),
        });
        ReadOnlyResultSet rs = new ReadOnlyResultSet(null, rows);
        ResultSetMetaData md = rs.getMetaData();
        Assert.assertEquals(md.getColumnCount(), fields.length);
        Assert.assertEquals(md.getColumnName(1), fields[0].name());
        Assert.assertEquals(md.getColumnLabel(1), fields[0].name());
        Assert.assertEquals(md.getColumnType(1), fields[0].type().getVendorTypeNumber());
        Assert.assertTrue(rs.next(), "Should have at least one row");
        Assert.assertEquals(rs.getString(1), "a1");
        Assert.assertEquals(rs.getString(2), "b1");
        Assert.assertEquals(md, rs.getMetaData());
        Assert.assertTrue(rs.next(), "Should have at least two rows");
        Assert.assertEquals(rs.getString(1), "a2");
        Assert.assertEquals(rs.getString(2), "b2");
        Assert.assertEquals(md, rs.getMetaData());
        Assert.assertFalse(rs.next(), "Should not have more rows");
        Assert.assertEquals(md, rs.getMetaData());
    }

    @Test(groups = { "unit" })
    public void testRowPositions() throws SQLException {
        Field[] fields = new Field[] { Field.of("a", JDBCType.VARCHAR), Field.of("b", JDBCType.VARCHAR) };
        List<Row> rows = Arrays.asList(new Row[] {
                Row.of(fields, new StringValue("a1"), new StringValue("b1")),
                Row.of(fields, new StringValue("a2"), new StringValue("b2")),
        });
        ReadOnlyResultSet rs = new ReadOnlyResultSet(null, rows);
        Assert.assertTrue(rs.isBeforeFirst(), "Should be before the first");
        Assert.assertFalse(rs.isFirst(), "Should not be the first");
        Assert.assertFalse(rs.isLast(), "Should not be the last");
        Assert.assertFalse(rs.isAfterLast(), "Should not be after the last");
        Assert.assertThrows(SQLException.class, () -> rs.getString(0));
        Assert.assertThrows(SQLException.class, () -> rs.getString(1));
        Assert.assertThrows(SQLException.class, () -> rs.getString(2));
        Assert.assertThrows(SQLException.class, () -> rs.getString(3));

        Assert.assertTrue(rs.next(), "Should have at least one row");
        Assert.assertFalse(rs.isBeforeFirst(), "Should not be before the first");
        Assert.assertTrue(rs.isFirst(), "Should be the first");
        Assert.assertFalse(rs.isLast(), "Should not be the last");
        Assert.assertFalse(rs.isAfterLast(), "Should not be after the last");
        Assert.assertThrows(SQLException.class, () -> rs.getString(0));
        Assert.assertEquals(rs.getString(1), "a1");
        Assert.assertEquals(rs.getString(2), "b1");
        Assert.assertThrows(SQLException.class, () -> rs.getString(3));

        Assert.assertTrue(rs.next(), "Should have two rows");
        Assert.assertFalse(rs.isBeforeFirst(), "Should not be before the first");
        Assert.assertFalse(rs.isFirst(), "Should not be the first");
        Assert.assertTrue(rs.isLast(), "Should be the last");
        Assert.assertFalse(rs.isAfterLast(), "Should not be after the last");
        Assert.assertThrows(SQLException.class, () -> rs.getString(0));
        Assert.assertEquals(rs.getString(1), "a2");
        Assert.assertEquals(rs.getString(2), "b2");
        Assert.assertThrows(SQLException.class, () -> rs.getString(3));

        Assert.assertFalse(rs.next(), "Should not have more rows");
        Assert.assertFalse(rs.isBeforeFirst(), "Should not be before the first");
        Assert.assertFalse(rs.isFirst(), "Should not be the first");
        Assert.assertFalse(rs.isLast(), "Should not be the last");
        Assert.assertTrue(rs.isAfterLast(), "Should be after the last");
        Assert.assertThrows(SQLException.class, () -> rs.getString(0));
        Assert.assertThrows(SQLException.class, () -> rs.getString(1));
        Assert.assertThrows(SQLException.class, () -> rs.getString(2));
        Assert.assertThrows(SQLException.class, () -> rs.getString(3));
    }
}
