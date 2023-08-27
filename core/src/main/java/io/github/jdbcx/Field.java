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
package io.github.jdbcx;

import java.io.Serializable;
import java.sql.JDBCType;

public final class Field implements Serializable {
    private static final long serialVersionUID = 4742910687725377482L;

    public static final Field DEFAULT = of("results", JDBCType.VARCHAR, true, 0, 0, false);

    public static final Field of(String name) {
        return of(Checker.nonNull(name, "Name"), JDBCType.VARCHAR, true, 0, 0, true);
    }

    public static final Field of(String name, int type) {
        return of(name, JDBCType.valueOf(type), true, 0, 0, true);
    }

    public static final Field of(String name, JDBCType type) {
        return of(name, type, true, 0, 0, true);
    }

    public static final Field of(String name, JDBCType type, boolean nullable) {
        return of(name, type, nullable, 0, 0, true);
    }

    public static final Field of(String name, JDBCType type, boolean nullable, int precision, int scale,
            boolean signed) {
        if (name == null || type == null) {
            throw new IllegalArgumentException("Non-null name and type are required");
        }
        return new Field(name, type, nullable, precision, scale, signed);
    }

    private final String name;
    private final JDBCType type;
    private final boolean nullable;
    private final int precision;
    private final int scale;
    private final boolean signed;

    public String name() {
        return name;
    }

    public JDBCType type() {
        return type;
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

    private Field(String name, JDBCType type, boolean nullable, int precision, int scale, boolean signed) {
        this.name = name;
        this.type = type;
        this.nullable = nullable;

        switch (type) {
            case TINYINT:
                this.precision = precision > 0 && precision <= 3 ? precision : 3;
                this.scale = 0;
                this.signed = signed;
                break;
            case SMALLINT:
                this.precision = precision > 0 && precision <= 5 ? precision : 5;
                this.scale = 0;
                this.signed = signed;
                break;
            case INTEGER:
                this.precision = precision > 0 && precision <= 10 ? precision : 10;
                this.scale = 0;
                this.signed = signed;
                break;
            case BIGINT:
                this.precision = precision > 0 && precision <= 19 ? precision : 19;
                this.scale = 0;
                this.signed = signed;
                break;
            case FLOAT:
            case REAL:
                this.precision = precision > 0 && precision <= 12 ? precision : 12;
                this.scale = 0;
                this.signed = signed;
                break;
            case DOUBLE:
                this.precision = precision > 0 && precision <= 22 ? precision : 22;
                this.scale = 0;
                this.signed = signed;
                break;
            case NUMERIC:
            case DECIMAL:
                this.precision = precision > 0 ? precision : 22;
                this.scale = scale > 0 ? scale : 0;
                this.signed = signed;
                break;
            case DATE:
            case TIME:
            case TIME_WITH_TIMEZONE:
                this.precision = precision > 0 ? precision : 10;
                this.scale = 0;
                this.signed = false;
                break;
            case TIMESTAMP:
            case TIMESTAMP_WITH_TIMEZONE:
                this.precision = precision > 0 ? precision : 29;
                this.scale = scale > 0 ? scale : 0;
                this.signed = false;
                break;
            default:
                this.precision = 0;
                this.scale = 0;
                this.signed = false;
                break;
        }
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = prime + name.hashCode();
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
        return name.equals(other.name) && type.equals(other.type) && nullable == other.nullable
                && precision == other.precision && scale == other.scale && signed == other.signed;
    }

    @Override
    public String toString() {
        return new StringBuilder(super.toString()).append('[').append(name).append(' ').append(type.name())
                .append(nullable ? " NULL" : " NOT NULL").append(']').toString();
    }
}
