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

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.MalformedURLException;
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
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

import io.github.jdbcx.Constants;
import io.github.jdbcx.Result;
import io.github.jdbcx.Row;
import io.github.jdbcx.Utils;
import io.github.jdbcx.Value;

public final class ReadOnlyResultSet extends AbstractResultSet {
    private static final Result<?> emptyResult = Result.of(Constants.EMPTY_STRING);
    private static final Row LAST_ROW = Row.of("last");

    private final Result<?> result;
    private final Iterator<Row> cursor;

    private ResultSetMetaData metaData;

    private int count;
    private Row row;
    private Value val;

    public ReadOnlyResultSet(Statement stmt, Result<?> result) {
        super(stmt);

        if (result != null) {
            this.result = result;
            this.cursor = result.rows().iterator();
        } else {
            this.result = emptyResult;
            this.cursor = Collections.emptyIterator();
        }

        this.count = 0;
        this.row = null;
        this.val = null;
    }

    protected Row current() throws SQLException {
        ensureOpen();

        if (count == 0 || row == null || row == LAST_ROW) {
            throw SqlExceptionUtils.clientError("No row to read");
        }
        return row;
    }

    protected Value value(int columnIndex) throws SQLException {
        ensureOpen();

        if (count == 0 || row == null || row == LAST_ROW) {
            throw SqlExceptionUtils.clientError("No row to read");
        } else {
            try {
                val = row.value(columnIndex - 1);
            } catch (ArrayIndexOutOfBoundsException e) {
                throw SqlExceptionUtils.clientError(e);
            }
        }
        return val;
    }

    protected ResultSetMetaData createMetaData() throws SQLException {
        if (metaData == null) {
            try {
                metaData = new SimpleResultSetMetaData(Constants.EMPTY_STRING, Constants.EMPTY_STRING,
                        Constants.EMPTY_STRING, result.fields());
            } catch (Exception e) {
                // unwrap the first tier, which is UncheckedIOException in most cases
                Throwable t = e.getCause();
                if (t == null) {
                    t = e;
                }
                throw SqlExceptionUtils.handle(t);
            }
        }
        return metaData;
    }

    @Override
    public boolean next() throws SQLException {
        final boolean b;
        try {
            b = row != LAST_ROW && cursor.hasNext();
            if (b) {
                row = cursor.next();
                val = null;
                count++;
            } else {
                row = LAST_ROW;
                val = null;
            }
        } catch (Exception e) {
            // unwrap the first tier, which is UncheckedIOException in most cases
            Throwable t = e.getCause();
            if (t == null) {
                t = e;
            }
            throw SqlExceptionUtils.handle(t);
        }
        return b;
    }

    @Override
    public void close() throws SQLException {
        try {
            result.close();
        } catch (Exception e) {
            throw SqlExceptionUtils.handle(e);
        } finally {
            super.close();
        }
    }

    @Override
    public boolean wasNull() throws SQLException {
        return val != null && val.isNull();
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        return value(columnIndex).asString();
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        return value(columnIndex).asBoolean();
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        return value(columnIndex).asByte();
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        return value(columnIndex).asShort();
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        return value(columnIndex).asInt();
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        return value(columnIndex).asLong();
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        return value(columnIndex).asFloat();
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        return value(columnIndex).asDouble();
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        return value(columnIndex).asBigDecimal(scale);
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        return value(columnIndex).asBinary();
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        return value(columnIndex).asSqlDate();
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        return value(columnIndex).asSqlTime();
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        return value(columnIndex).asSqlTimestamp();
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getAsciiStream'");
    }

    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getUnicodeStream'");
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getBinaryStream'");
    }

    @Override
    public String getCursorName() throws SQLException {
        ensureOpen();

        throw new UnsupportedOperationException("Unimplemented method 'getCursorName'");
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        ensureOpen();

        return createMetaData();
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        return value(columnIndex).asObject();
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        int index = current().index(columnLabel);
        if (index == -1) {
            throw SqlExceptionUtils.clientError(Utils.format("Column \"%s\" not found", columnLabel));
        }
        return index + 1;
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getCharacterStream'");
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        return value(columnIndex).asBigDecimal();
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        ensureOpen();

        return count == 0 && row == null;
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        ensureOpen();

        return row == LAST_ROW && !cursor.hasNext();
    }

    @Override
    public boolean isFirst() throws SQLException {
        ensureOpen();

        return count == 1;
    }

    @Override
    public boolean isLast() throws SQLException {
        ensureOpen();

        return row != LAST_ROW && !cursor.hasNext();
    }

    @Override
    public int getRow() throws SQLException {
        ensureOpen();

        return count;
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getObject'");
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        ensureOpen();

        throw new UnsupportedOperationException("Unimplemented method 'getRef'");
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getBlob'");
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getClob'");
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getArray'");
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getDate'");
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getTime'");
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getTimestamp'");
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        ensureOpen();

        try {
            return new URL(getString(columnIndex));
        } catch (MalformedURLException e) {
            throw SqlExceptionUtils.clientError(e);
        }
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        ensureOpen();

        throw new UnsupportedOperationException("Unimplemented method 'getRowId'");
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        ensureOpen();

        throw new UnsupportedOperationException("Unimplemented method 'getNClob'");
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        ensureOpen();

        throw new UnsupportedOperationException("Unimplemented method 'getSQLXML'");
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        return getString(columnIndex);
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        return getCharacterStream(columnIndex);
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        return value(columnIndex).asObject(type);
    }
}
