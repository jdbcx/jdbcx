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

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import io.github.jdbcx.Field;
import io.github.jdbcx.Row;

final class SimpleResultSetMetaData implements ResultSetMetaData {
    private final String catalog;
    private final String schema;
    private final String table;

    private final Field[] fields;

    SimpleResultSetMetaData(String catalog, String schema, String table, Row row) {
        this.catalog = catalog;
        this.schema = schema;
        this.table = table;

        int size = row.size();
        this.fields = new Field[size];
        for (int i = 0; i < size; i++) {
            this.fields[i] = row.field(i);
        }
    }

    protected Field field(int columnIndex) throws SQLException {
        if (fields.length < columnIndex) {
            throw SqlExceptionUtils.clientError("Invalid column index %d, should between 1 and %d");
        }
        return fields[columnIndex - 1];
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return fields.getClass() == iface ? iface.cast(iface) : null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return fields.getClass() == iface;
    }

    @Override
    public int getColumnCount() throws SQLException {
        return this.fields.length;
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        return false;
    }

    @Override
    public int isNullable(int column) throws SQLException {
        return ResultSetMetaData.columnNullable;
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        return false;
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        return 20;
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return field(column).name();
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        return field(column).name();
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        return schema;
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        return 0;
    }

    @Override
    public int getScale(int column) throws SQLException {
        return 0;
    }

    @Override
    public String getTableName(int column) throws SQLException {
        return table;
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        return catalog;
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        return field(column).type().getVendorTypeNumber();
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        return field(column).type().name();
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        return String.class.getName();
    }
}
