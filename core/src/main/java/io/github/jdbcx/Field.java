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

import java.io.Serializable;
import java.sql.JDBCType;
import java.util.Locale;

public final class Field implements Serializable {
    private static final long serialVersionUID = 4742910687725377482L;

    public static final Field DEFAULT = of("results", null, JDBCType.VARCHAR, true, 0, 0, false);

    public static final Field BINARY = of(DEFAULT.name(), null, JDBCType.BLOB, DEFAULT.isNullable(), 0, 0,
            false);

    static int[] normalize(String lowerColumnType, JDBCType type, int precision, int scale, boolean signed) {
        switch (type) {
            case BOOLEAN:
                precision = 1;
                scale = 0;
                signed = false;
                break;
            case BIT:
                if (precision < 1) {
                    if (lowerColumnType.startsWith("bool")) {
                        type = JDBCType.BOOLEAN;
                    }
                    precision = 1;
                }
                scale = 0;
                signed = false;
                break;
            case TINYINT:
                precision = precision > 0 && precision <= 3 ? precision : 3;
                scale = 0;
                break;
            case SMALLINT:
                precision = precision > 0 && precision <= 5 ? precision : 5;
                scale = 0;
                break;
            case INTEGER:
                precision = precision > 0 && precision <= 10 ? precision : 10;
                scale = 0;
                break;
            case BIGINT:
                precision = precision > 0 && precision <= 19 ? precision : 19;
                scale = 0;
                break;
            case FLOAT:
            case REAL:
                precision = precision > 0 && precision <= 12 ? precision : 12;
                scale = scale > 0 ? scale : 0;
                break;
            case DOUBLE:
                precision = precision > 0 && precision <= 22 ? precision : 22;
                scale = scale > 0 ? scale : 0;
                break;
            case NUMERIC:
            case DECIMAL:
                precision = precision > 0 ? precision : 22;
                scale = scale > 0 ? scale : 0;
                break;
            case DATE:
                precision = 10;
                scale = 0;
                signed = false;
                break;
            case TIME:
            case TIME_WITH_TIMEZONE:
                if (precision < 9) {
                    precision = 18;
                    scale = scale > 0 && scale < 9 ? scale : 9;
                } else {
                    scale = precision - 9;
                    if (scale > 9) {
                        scale = 9;
                    }
                }
                signed = false;
                break;
            case TIMESTAMP:
            case TIMESTAMP_WITH_TIMEZONE:
                if (precision < 20) {
                    precision = 29;
                    scale = scale > 0 && scale < 9 ? scale : 9;
                } else {
                    scale = precision - 20;
                    if (scale > 9) {
                        scale = 9;
                    }
                }
                signed = false;
                break;
            case BINARY:
            case VARBINARY:
            case LONGVARBINARY:
            case BLOB:
            case CLOB:
            case CHAR:
            case NCHAR:
            case VARCHAR:
            case NVARCHAR:
            case LONGVARCHAR:
            case LONGNVARCHAR:
                precision = precision > 0 ? precision : 0;
                scale = 0;
                signed = false;
                break;
            default:
                precision = precision > 0 ? precision : 0;
                scale = scale > 0 ? scale : 0;
                signed = false;
                break;
        }
        return new int[] { type.getVendorTypeNumber(), precision, scale, signed ? 1 : 0 };
    }

    public static final Field of(String name) {
        return of(Checker.nonNull(name, "Name"), null, JDBCType.VARCHAR, true, 0, 0, true);
    }

    public static final Field of(String name, int type) {
        return of(name, null, JDBCType.valueOf(type), true, 0, 0, true);
    }

    public static final Field of(String name, JDBCType type) {
        return of(name, null, type, true, 0, 0, true);
    }

    public static final Field of(String name, JDBCType type, boolean nullable) {
        return of(name, null, type, nullable, 0, 0, true);
    }

    public static final Field of(String name, String columnType, JDBCType type, boolean nullable, int precision,
            int scale, boolean signed) {
        if (name == null || type == null) {
            throw new IllegalArgumentException("Non-null name and type are required");
        }
        return new Field(name, columnType, type, nullable, precision, scale, signed);
    }

    private final String name;
    private final JDBCType type;
    private final String columnType;
    private final boolean nullable;
    private final int precision;
    private final int scale;
    private final boolean signed;
    // default value, codec, remark

    public String name() {
        return name;
    }

    public JDBCType type() {
        return type;
    }

    public String columnType() {
        return columnType;
    }

    public int precision() {
        return precision;
    }

    public int scale() {
        return scale;
    }

    public boolean isNullable() {
        return nullable;
    }

    public boolean isSigned() {
        return signed;
    }

    private Field(String name, String columnType, JDBCType type, boolean nullable, int precision, int scale,
            boolean signed) {
        this.name = name;
        this.columnType = columnType != null ? columnType : Constants.EMPTY_STRING;
        this.nullable = nullable;

        int[] info = normalize(this.columnType.toLowerCase(Locale.ROOT), type, precision, scale, signed);
        this.type = JDBCType.valueOf(info[0]);
        this.precision = info[1];
        this.scale = info[2];
        this.signed = info[3] == 1;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = prime + name.hashCode();
        result = prime * result + columnType.hashCode();
        result = prime * result + type.hashCode();
        result = prime * result + (nullable ? 1231 : 1237);
        result = prime * result + precision;
        result = prime * result + scale;
        result = prime * result + (signed ? 1231 : 1237);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        Field other = (Field) obj;
        return name.equals(other.name) && columnType.equals(other.columnType) && type.equals(other.type)
                && nullable == other.nullable && precision == other.precision && scale == other.scale
                && signed == other.signed;
    }

    @Override
    public String toString() {
        return new StringBuilder(super.toString()).append('[').append(name).append(' ').append(type.name())
                .append(nullable ? " NULL" : " NOT NULL").append(']').toString();
    }
}
