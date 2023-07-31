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

    public static final Field of(String name) {
        return new Field(Checker.nonNull(name, "Name"), JDBCType.VARCHAR);
    }

    public static final Field of(String name, int type) {
        return of(name, JDBCType.valueOf(type));
    }

    public static final Field of(String name, JDBCType type) {
        if (name == null || type == null) {
            throw new IllegalArgumentException("Non-null name and type are required");
        }
        return new Field(name, type);
    }

    private final String name;
    private final JDBCType type;

    public String name() {
        return name;
    }

    public JDBCType type() {
        return type;
    }

    private Field(String name, JDBCType type) {
        this.name = name;
        this.type = type;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = prime + name.hashCode();
        result = prime * result + type.hashCode();
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
        return name.equals(other.name) && type.equals(other.type);
    }

    @Override
    public String toString() {
        return new StringBuilder(super.toString()).append('[').append(name).append(' ').append(type.name())
                .append(']').toString();
    }
}
