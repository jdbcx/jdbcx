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

import java.lang.reflect.Modifier;
import java.sql.JDBCType;

import io.github.jdbcx.value.StringValue;

public class TypeMapping {
    public static final String ANY_TYPE = "*";

    static JDBCType getJdbcType(String fromJdbcType) {
        JDBCType jdbcType = null;

        if (fromJdbcType != null) {
            try {
                fromJdbcType = fromJdbcType.trim().toUpperCase();
                jdbcType = JDBCType.valueOf(fromJdbcType);
            } catch (RuntimeException e) {
                // ignore
            }
        }
        return jdbcType;
    }

    static JDBCType getJdbcType(int fromJdbcType) {
        JDBCType jdbcType = null;

        try {
            jdbcType = JDBCType.valueOf(fromJdbcType);
        } catch (RuntimeException e) {
            // ignore
        }
        return jdbcType;
    }

    @SuppressWarnings("unchecked")
    static Class<? extends Value> getValueType(String toValueType) {
        Class<? extends Value> valueType = StringValue.class;

        if (toValueType != null) {
            final String suffix = Value.class.getSimpleName();
            final StringBuilder builder = new StringBuilder(toValueType.trim());
            // String or StringValue instead of io.github.jdbcx.data.StringValue
            if (toValueType.indexOf('.') == -1) {
                // String instead of StringValue
                if (!toValueType.endsWith(suffix)) {
                    builder.append(suffix);
                }
                String packageName = valueType.getName();
                int index = packageName.lastIndexOf('.');
                builder.insert(0, '.').insert(0,
                        index != -1 ? packageName.substring(0, packageName.lastIndexOf('.')) : packageName);
            }
            try {
                ClassLoader loader = Thread.currentThread().getContextClassLoader();
                if (loader == null) {
                    loader = TypeMapping.class.getClassLoader();
                }
                Class<?> clazz = loader.loadClass(builder.toString());
                if (Value.class.isAssignableFrom(clazz) && !Modifier.isAbstract(clazz.getModifiers())
                        && !clazz.isInterface()) {
                    valueType = (Class<? extends Value>) clazz;
                }
            } catch (ClassNotFoundException | NoClassDefFoundError | RuntimeException e) {
                // ignore
            }
        }
        return valueType;
    }

    private final JDBCType fromJdbcType;
    private final String fromDatabaseType;
    private final Class<? extends Value> toValueType;

    public TypeMapping(String fromJdbcType, String fromDatabaseType, String toValueType) {
        this(getJdbcType(fromJdbcType), fromDatabaseType, getValueType(toValueType));
    }

    public TypeMapping(int fromJdbcType, String fromDatabaseType, Class<? extends Value> toValueType) {
        this(getJdbcType(fromJdbcType), fromDatabaseType, toValueType);
    }

    public TypeMapping(JDBCType fromJdbcType, String fromDatabaseType, Class<? extends Value> toValueType) {
        if (fromJdbcType == null && Checker.isNullOrEmpty(fromDatabaseType)) {
            throw new IllegalArgumentException("At least one valid source type is required");
        } else if (toValueType == null) {
            throw new IllegalArgumentException("Non-null value type is required");
        } else if (Modifier.isAbstract(toValueType.getModifiers()) || toValueType.isInterface()) {
            throw new IllegalArgumentException("Value type cannot be an abstract class or interface");
        }
        this.fromJdbcType = fromJdbcType;
        this.fromDatabaseType = ANY_TYPE.equals(fromDatabaseType) ? ANY_TYPE : fromDatabaseType;
        this.toValueType = toValueType;
    }

    /**
     * Gets source JDBC type.
     *
     * @return source JDBC type, could be {@code null}
     */
    public JDBCType getSourceJdbcType() {
        return fromJdbcType;
    }

    /**
     * Gets source database type.
     *
     * @return source database type, could be {@code null}
     */
    public String getSourceDatabaseType() {
        return fromDatabaseType;
    }

    /**
     * Gets mapped value type.
     *
     * @return non-null value type
     */
    public Class<? extends Value> getMappedType() {
        return toValueType;
    }

    /**
     * Checks if the given types matches the mapping rule or not. It first checks
     * database type, if defined, and then JDBC type.
     *
     * @param jdbcType     optional JDBC type
     * @param databaseType optional database type
     * @return true if matching the mapping rule; false otherwise
     */
    public boolean accept(JDBCType jdbcType, String databaseType) {
        return Checker.isNullOrEmpty(fromDatabaseType)
                ? fromJdbcType == jdbcType
                : (ANY_TYPE.equals(fromDatabaseType) || ANY_TYPE.equals(databaseType)
                        || fromDatabaseType.equals(databaseType));
    }
}
