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
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Collection;
import java.util.Map;

/**
 * Wrapper of multiple ResultSets.
 */
public class CombinedResultSet extends AbstractResultSet {
    private final ResultSet[] results;

    private int nextIndex;
    private ResultSet current;
    private int rowNumber;

    protected ResultSet current() throws SQLException {
        if (getNextResult() == null) {
            throw new SQLException("No result to access", SqlExceptionUtils.SQL_STATE_NO_DATA);
        }
        return current;
    }

    protected ResultSet getNextResult() {
        if (this.current == null) {
            for (int i = this.nextIndex, len = results.length; i < len; i++) {
                ResultSet rs = results[i];
                if (rs != null) {
                    this.current = rs;
                    this.nextIndex = i + 1;
                    break;
                }
            }
        }
        return this.current;
    }

    protected boolean hasNext() throws SQLException {
        if (getNextResult() == null) {
            return false;
        } else if (current.next()) {
            return true;
        } else if (nextIndex >= results.length) {
            return false;
        }

        boolean hasNext = false;
        while (nextIndex < results.length) {
            if (current != null) {
                current.close();
            }

            current = results[nextIndex++];
            if (current != null && current.next()) {
                hasNext = true;
                break;
            }
        }

        return hasNext;
    }

    public CombinedResultSet(ResultSet... results) {
        this(results, null);
    }

    public CombinedResultSet(ResultSet[] results, SQLWarning warning) {
        super(null);

        this.nextIndex = 0;
        this.rowNumber = 0;
        this.warning.set(warning);
        if (results == null || results.length == 0) {
            this.results = new ResultSet[0];
            this.current = null;
        } else {
            this.results = results;
            this.current = null;
        }
    }

    public CombinedResultSet(Collection<ResultSet> results) {
        this(results, null);
    }

    public CombinedResultSet(Collection<ResultSet> results, SQLWarning warning) {
        super(null);

        this.nextIndex = 0;
        this.rowNumber = 0;
        this.warning.set(warning);
        if (results == null || results.isEmpty()) {
            this.results = new ResultSet[0];
            this.current = null;
        } else {
            int len = results.size();
            this.results = new ResultSet[len];
            int i = 0;
            for (ResultSet rs : results) {
                this.results[i++] = rs;
                if (this.current == null && rs != null) {
                    this.current = rs;
                    this.nextIndex = i;
                }
            }
            if (this.nextIndex == 0) {
                this.nextIndex = len;
            }
        }
    }

    /**
     * Checks if the combined result set only contains null {@link ResultSet} or
     * empty {@link CombinedResultSet}.
     *
     * @return true if it's empty; false otherwise
     */
    public boolean isEmpty() {
        for (int i = 0, len = results.length; i < len; i++) {
            ResultSet rs = results[i];
            if (rs != null && (!(rs instanceof CombinedResultSet) || !((CombinedResultSet) rs).isEmpty())) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean next() throws SQLException {
        if (hasNext()) {
            this.rowNumber++;
            return true;
        }

        return false;
    }

    @Override
    public void close() throws SQLException {
        SQLException ex = null;
        for (ResultSet rs : results) {
            if (rs != null) {
                try {
                    rs.close();
                } catch (Exception e) {
                    if (ex == null) {
                        ex = SqlExceptionUtils.handle(e);
                    } else {
                        ex.setNextException(SqlExceptionUtils.handle(e));
                    }
                }
            }
        }
        super.close();
        if (ex != null) {
            throw ex;
        }
    }

    @Override
    public boolean wasNull() throws SQLException {
        return current().wasNull();
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        return current().getString(columnIndex);
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        return current().getBoolean(columnIndex);
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        return current().getByte(columnIndex);
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        return current().getShort(columnIndex);
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        return current().getInt(columnIndex);
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        return current().getLong(columnIndex);
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        return current().getFloat(columnIndex);
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        return current().getDouble(columnIndex);
    }

    @SuppressWarnings("deprecation")
    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        return current().getBigDecimal(columnIndex, scale);
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        return current().getBytes(columnIndex);
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        return current().getDate(columnIndex);
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        return current().getTime(columnIndex);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        return current().getTimestamp(columnIndex);
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        return current().getAsciiStream(columnIndex);
    }

    @SuppressWarnings("deprecation")
    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        return current().getUnicodeStream(columnIndex);
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        return current().getBinaryStream(columnIndex);
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        return current().getString(columnLabel);
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return current().getBoolean(columnLabel);
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return current().getByte(columnLabel);
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return current().getShort(columnLabel);
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return current().getInt(columnLabel);
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return current().getLong(columnLabel);
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return current().getFloat(columnLabel);
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return current().getDouble(columnLabel);
    }

    @SuppressWarnings("deprecation")
    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return current().getBigDecimal(columnLabel, scale);
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return current().getBytes(columnLabel);
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return current().getDate(columnLabel);
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return current().getTime(columnLabel);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return current().getTimestamp(columnLabel);
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        return current().getAsciiStream(columnLabel);
    }

    @SuppressWarnings("deprecation")
    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        return current().getUnicodeStream(columnLabel);
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        return current().getBinaryStream(columnLabel);
    }

    @Override
    public String getCursorName() throws SQLException {
        return current().getCursorName();
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return current().getMetaData();
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        return current().getObject(columnIndex);
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return current().getObject(columnLabel);
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        return current().findColumn(columnLabel);
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        return current().getCharacterStream(columnIndex);
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        return current().getCharacterStream(columnLabel);
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        return current().getBigDecimal(columnIndex);
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return current().getBigDecimal(columnLabel);
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        return rowNumber == 0 && current().isBeforeFirst();
    }

    @SuppressWarnings("resource")
    @Override
    public boolean isAfterLast() throws SQLException {
        if (nextIndex >= results.length) {
            return current().isAfterLast();
        } else {
            ResultSet rs = current();
            boolean isAfterLast = false;
            while ((isAfterLast = rs.isAfterLast()) && next()) {
                rs = current();
            }
            return isAfterLast;
        }
    }

    @Override
    public boolean isFirst() throws SQLException {
        return rowNumber == 1 && current().isFirst();
    }

    @SuppressWarnings("resource")
    @Override
    public boolean isLast() throws SQLException {
        if (nextIndex >= results.length) {
            return current().isLast(); // NOSONAR
        } else {
            ResultSet rs = current();
            boolean isLast = false;
            while ((isLast = rs.isLast()) && next()) { // NOSONAR
                rs = current();
            }
            return isLast;
        }
    }

    @Override
    public int getRow() throws SQLException {
        return rowNumber;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        ensureOpen();
    }

    @Override
    public int getFetchSize() throws SQLException {
        return current().getFetchSize();
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        return current().rowUpdated();
    }

    @Override
    public boolean rowInserted() throws SQLException {
        return current().rowInserted();
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        return current().rowDeleted();
    }

    @Override
    public Statement getStatement() throws SQLException {
        return current().getStatement();
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        return current().getObject(columnIndex, map);
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        return current().getRef(columnIndex);
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        return current().getBlob(columnIndex);
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        return current().getClob(columnIndex);
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        return current().getArray(columnIndex);
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        return current().getObject(columnLabel, map);
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        return current().getRef(columnLabel);
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        return current().getBlob(columnLabel);
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        return current().getClob(columnLabel);
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        return current().getArray(columnLabel);
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        return current().getDate(columnIndex, cal);
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        return current().getDate(columnLabel, cal);
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        return current().getTime(columnIndex, cal);
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        return current().getTime(columnLabel, cal);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        return current().getTimestamp(columnIndex, cal);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        return current().getTimestamp(columnLabel, cal);
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        return current().getURL(columnIndex);
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        return current().getURL(columnLabel);
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        return current().getRowId(columnIndex);
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        return current().getRowId(columnLabel);
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        return current().getNClob(columnIndex);
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        return current().getNClob(columnLabel);
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        return current().getSQLXML(columnIndex);
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        return current().getSQLXML(columnLabel);
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        return current().getNString(columnIndex);
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        return current().getNString(columnLabel);
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        return current().getNCharacterStream(columnIndex);
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        return current().getNCharacterStream(columnLabel);
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        return current().getObject(columnIndex, type);
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        return current().getObject(columnLabel, type);
    }
}
