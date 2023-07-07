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
package io.github.jdbcx.impl;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;

import io.github.jdbcx.SqlExceptionUtils;

public class DummyResultSet extends AbstractResultSet {
    public DummyResultSet(Statement stmt) {
        super(stmt);
    }

    @Override
    public boolean next() throws SQLException {
        ensureOpen();

        return false;
    }

    @Override
    public boolean wasNull() throws SQLException { // NOSONAR
        ensureOpen();

        return false;
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("getString not implemented");
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("getBoolean not implemented");
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("getByte not implemented");
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("getShort not implemented");
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("getInt not implemented");
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("getLong not implemented");
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("getFloat not implemented");
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("getDouble not implemented");
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("getBigDecimal not implemented"); // NOSONAR
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("getBytes not implemented");
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("getDate not implemented"); // NOSONAR
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("getTime not implemented"); // NOSONAR
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("getTimestamp not implemented"); // NOSONAR
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("getAsciiStream not implemented");
    }

    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("getUnicodeStream not implemented");
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("getBinaryStream not implemented");
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("getString not implemented");
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("getBoolean not implemented");
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("getByte not implemented");
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("getShort not implemented");
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("getInt not implemented");
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("getLong not implemented");
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("getFloat not implemented");
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("getDouble not implemented");
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("getBigDecimal not implemented");
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("getBytes not implemented");
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("getDate not implemented");
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("getTime not implemented");
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("getTimestamp not implemented");
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("getAsciiStream not implemented");
    }

    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("getUnicodeStream not implemented");
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("getBinaryStream not implemented");
    }

    @Override
    public String getCursorName() throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("getCursorName not implemented");
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("getMetaData not implemented");
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("getObject not implemented"); // NOSONAR
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("getObject not implemented");
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("findColumn not implemented");
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("getCharacterStream not implemented");
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("getCharacterStream not implemented");
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("getBigDecimal not implemented");
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("getBigDecimal not implemented");
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        ensureOpen();

        return true;
    }

    @Override
    public boolean isAfterLast() throws SQLException { // NOSONAR
        ensureOpen();

        return false;
    }

    @Override
    public boolean isFirst() throws SQLException { // NOSONAR
        ensureOpen();

        return false;
    }

    @Override
    public boolean isLast() throws SQLException { // NOSONAR
        ensureOpen();

        return false;
    }

    @Override
    public int getRow() throws SQLException {
        ensureOpen();

        return 0;
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("getBigDecimal not implemented");
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("getRef not implemented");
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("getBlob not implemented");
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("getClob not implemented");
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("getArray not implemented");
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("getObject not implemented");
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("getRef not implemented");
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("getBlob not implemented");
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("getClob not implemented");
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("getArray not implemented");
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("getDate not implemented");
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("getDate not implemented");
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("getTime not implemented");
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("getTime not implemented");
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("getTimestamp not implemented");
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("getTimestamp not implemented");
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("getURL not implemented");
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("getURL not implemented");
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("getRowId not implemented");
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("getRowId not implemented");
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("getNClob not implemented");
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("getNClob not implemented");
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("getSQLXML not implemented");
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("getSQLXML not implemented");
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("getNString not implemented");
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("getNString not implemented");
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("getNCharacterStream not implemented");
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("getNCharacterStream not implemented");
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("getObject not implemented");
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("getObject not implemented");
    }
}
