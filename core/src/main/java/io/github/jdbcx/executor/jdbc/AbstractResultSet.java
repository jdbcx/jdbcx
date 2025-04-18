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
package io.github.jdbcx.executor.jdbc;

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
import java.sql.ResultSet;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public abstract class AbstractResultSet implements ResultSet {
    protected final Statement stmt;

    protected final AtomicReference<SQLWarning> warning;
    protected volatile boolean closed;

    protected AbstractResultSet(Statement stmt) {
        this.stmt = stmt;

        this.warning = new AtomicReference<>();
        this.closed = false;
    }

    protected void ensureOpen() throws SQLException {
        if (isClosed()) {
            throw SqlExceptionUtils.clientError("Cannot operate on a closed ResultSet");
        }
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("absolute not implemented");
    }

    @Override
    public void afterLast() throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("afterLast not implemented");
    }

    @Override
    public void beforeFirst() throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("beforeFirst not implemented");
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("cancelRowUpdates not implemented");
    }

    @Override
    public void clearWarnings() throws SQLException {
        ensureOpen();

        warning.set(null);
    }

    @Override
    public void deleteRow() throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("deleteRow not implemented");
    }

    @Override
    public boolean first() throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("first not implemented");
    }

    @Override
    public int getConcurrency() throws SQLException {
        ensureOpen();

        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public int getFetchDirection() throws SQLException {
        ensureOpen();

        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public int getHoldability() throws SQLException {
        ensureOpen();

        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public int getType() throws SQLException {
        ensureOpen();

        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        ensureOpen();

        return warning.get();
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        return getString(findColumn(columnLabel));
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return getBoolean(findColumn(columnLabel));
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return getByte(findColumn(columnLabel));
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return getShort(findColumn(columnLabel));
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return getInt(findColumn(columnLabel));
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return getLong(findColumn(columnLabel));
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return getFloat(findColumn(columnLabel));
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return getDouble(findColumn(columnLabel));
    }

    @SuppressWarnings("deprecation")
    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return getBigDecimal(findColumn(columnLabel), scale);
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return getBytes(findColumn(columnLabel));
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return getDate(findColumn(columnLabel));
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return getTime(findColumn(columnLabel));
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return getTimestamp(findColumn(columnLabel));
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        return getAsciiStream(findColumn(columnLabel));
    }

    @SuppressWarnings("deprecation")
    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        return getUnicodeStream(findColumn(columnLabel));
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        return getBinaryStream(findColumn(columnLabel));
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return getObject(findColumn(columnLabel));
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        return getCharacterStream(findColumn(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return getBigDecimal(findColumn(columnLabel));
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        return getObject(findColumn(columnLabel), map);
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        return getRef(findColumn(columnLabel));
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        return getBlob(findColumn(columnLabel));
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        return getClob(findColumn(columnLabel));
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        return getArray(findColumn(columnLabel));
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        return getDate(findColumn(columnLabel), cal);
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        return getTime(findColumn(columnLabel), cal);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        return getTimestamp(findColumn(columnLabel), cal);
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        return getURL(findColumn(columnLabel));
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        return getRowId(findColumn(columnLabel));
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        return getNClob(findColumn(columnLabel));
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        return getSQLXML(findColumn(columnLabel));
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        return getNString(findColumn(columnLabel));
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        return getNCharacterStream(findColumn(columnLabel));
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        return getObject(findColumn(columnLabel), type);
    }

    @Override
    public void insertRow() throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("insertRow not implemented");
    }

    @Override
    public boolean last() throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("last not implemented");
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("moveToCurrentRow not implemented");
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("moveToInsertRow not implemented");
    }

    @Override
    public boolean previous() throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("previous not implemented");
    }

    @Override
    public void refreshRow() throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("refreshRow not implemented");
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("relative not implemented");
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        ensureOpen();

        return false;
    }

    @Override
    public boolean rowInserted() throws SQLException { // NOSONAR
        ensureOpen();

        return false;
    }

    @Override
    public boolean rowUpdated() throws SQLException { // NOSONAR
        ensureOpen();

        return false;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        ensureOpen();

        if (direction != ResultSet.FETCH_FORWARD) {
            throw SqlExceptionUtils.unsupportedError("only FETCH_FORWARD is supported in setFetchDirection");
        }
    }

    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateArray not implemented");
    }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {
        updateArray(findColumn(columnLabel), x);
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateAsciiStream not implemented"); // NOSONAR
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        updateAsciiStream(findColumn(columnLabel), x);
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateAsciiStream not implemented");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        updateAsciiStream(findColumn(columnLabel), x, length);
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateAsciiStream not implemented");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        updateAsciiStream(findColumn(columnLabel), x, length);
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateBigDecimal not implemented");
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        updateBigDecimal(findColumn(columnLabel), x);
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateBinaryStream not implemented"); // NOSONAR
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        updateBinaryStream(findColumn(columnLabel), x);
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateBinaryStream not implemented");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        updateBinaryStream(findColumn(columnLabel), x, length);
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateBinaryStream not implemented");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        updateBinaryStream(findColumn(columnLabel), x, length);
    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateBlob not implemented"); // NOSONAR
    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        updateBlob(findColumn(columnLabel), x);
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateBlob not implemented");
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        updateBlob(findColumn(columnLabel), inputStream);
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateBlob not implemented");
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        updateBlob(findColumn(columnLabel), inputStream, length);
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateBoolean not implemented");
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        updateBoolean(findColumn(columnLabel), x);
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateByte not implemented");
    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        updateByte(findColumn(columnLabel), x);
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateBytes not implemented");
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        updateBytes(findColumn(columnLabel), x);
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader reader) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateCharacterStream not implemented"); // NOSONAR
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        updateCharacterStream(findColumn(columnLabel), reader);
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader reader, int length) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateCharacterStream not implemented");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        updateCharacterStream(findColumn(columnLabel), reader, length);
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader reader, long length) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateCharacterStream not implemented");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        updateCharacterStream(findColumn(columnLabel), reader, length);
    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateClob not implemented"); // NOSONAR
    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {
        updateClob(findColumn(columnLabel), x);
    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateClob not implemented");
    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        updateClob(findColumn(columnLabel), reader);
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateClob not implemented");
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        updateClob(findColumn(columnLabel), reader, length);
    }

    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateDate not implemented");
    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {
        updateDate(findColumn(columnLabel), x);
    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateDouble not implemented");
    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        updateDouble(findColumn(columnLabel), x);
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateFloat not implemented");
    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        updateFloat(findColumn(columnLabel), x);
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateInt not implemented");
    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        updateInt(findColumn(columnLabel), x);
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateLong not implemented");
    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        updateLong(findColumn(columnLabel), x);
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader reader) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateNCharacterStream not implemented");
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        updateNCharacterStream(findColumn(columnLabel), reader);
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader reader, long length) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateNCharacterStream not implemented");
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        updateNCharacterStream(findColumn(columnLabel), reader, length);
    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateNClob not implemented"); // NOSONAR
    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        updateNClob(findColumn(columnLabel), nClob);
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateNClob not implemented");
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        updateNClob(findColumn(columnLabel), reader);
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateNClob not implemented");
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        updateNClob(findColumn(columnLabel), reader, length);
    }

    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateNString not implemented");
    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {
        updateNString(findColumn(columnLabel), nString);
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateNull not implemented");
    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {
        updateNull(findColumn(columnLabel));
    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateObject not implemented"); // NOSONAR
    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
        updateObject(findColumn(columnLabel), x);
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateObject not implemented");
    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        updateObject(findColumn(columnLabel), x, scaleOrLength);
    }

    @Override
    public void updateObject(int columnIndex, Object x, SQLType targetSqlType) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateObject not implemented");
    }

    @Override
    public void updateObject(String columnLabel, Object x, SQLType targetSqlType) throws SQLException {
        updateObject(findColumn(columnLabel), x, targetSqlType);
    }

    @Override
    public void updateObject(int columnIndex, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateObject not implemented");
    }

    @Override
    public void updateObject(String columnLabel, Object x, SQLType targetSqlType, int scaleOrLength)
            throws SQLException {
        updateObject(findColumn(columnLabel), x, targetSqlType, scaleOrLength);
    }

    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateRef not implemented");
    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {
        updateRef(findColumn(columnLabel), x);
    }

    @Override
    public void updateRow() throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateRow not implemented");
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateRowId not implemented");
    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        updateRowId(findColumn(columnLabel), x);
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateSQLXML not implemented");
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        updateSQLXML(findColumn(columnLabel), xmlObject);
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateShort not implemented");
    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        updateShort(findColumn(columnLabel), x);
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateString not implemented");
    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        updateString(findColumn(columnLabel), x);
    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateTime not implemented");
    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
        updateTime(findColumn(columnLabel), x);
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateTimestamp not implemented");
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        updateTimestamp(findColumn(columnLabel), x);
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        ensureOpen();

        // do nothing
    }

    @Override
    public int getFetchSize() throws SQLException {
        ensureOpen();

        return stmt != null ? stmt.getFetchSize() : 0;
    }

    @Override
    public Statement getStatement() throws SQLException {
        ensureOpen();

        return stmt;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    @Override
    public void close() throws SQLException {
        closed = true;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw SqlExceptionUtils.unsupportedError("unwrap not implemented");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }
}
