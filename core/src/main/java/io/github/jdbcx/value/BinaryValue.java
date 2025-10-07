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
package io.github.jdbcx.value;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Arrays;

import io.github.jdbcx.Constants;
import io.github.jdbcx.Stream;
import io.github.jdbcx.Utils;
import io.github.jdbcx.ValueFactory;

public final class BinaryValue extends AbstractValue {
    public static BinaryValue of(ValueFactory factory, boolean nullable) {
        return new BinaryValue(factory, nullable, null);
    }

    public static BinaryValue of(ValueFactory factory, boolean nullable, String value) {
        return new BinaryValue(factory, nullable,
                value != null
                        ? new ByteArrayInputStream(value.getBytes(
                                factory != null ? factory.getCharset() : ValueFactory.getInstance().getCharset()))
                        : null);
    }

    public static BinaryValue of(ValueFactory factory, boolean nullable, byte[] value) {
        return new BinaryValue(factory, nullable, value != null ? new ByteArrayInputStream(value) : null);
    }

    public static BinaryValue of(ValueFactory factory, boolean nullable, InputStream value) {
        return new BinaryValue(factory, nullable, value);
    }

    public static BinaryValue of(String value) {
        return of(ValueFactory.getInstance(), true, value);
    }

    public static BinaryValue of(Object value) {
        return of(value == null ? null : value.toString());
    }

    public static BinaryValue ofJson(Object value) {
        return of(value == null ? null : ValueFactory.toJson(value));
    }

    private transient InputStream value;

    protected BinaryValue(ValueFactory factory, boolean nullable, InputStream value) {
        super(factory, nullable);
        this.value = value;
    }

    @Override
    public boolean isNull() {
        return value == null;
    }

    @Override
    public byte[] asBinary() {
        if (isNull()) {
            return Constants.EMPTY_BYTE_ARRAY;
        }

        try {
            return Stream.readAllBytes(value);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public byte[] asBinary(Charset charset) {
        return asBinary();
    }

    @Override
    public byte asByte() {
        final String s = asString();
        if (s.isEmpty()) {
            return factory.getDefaultByte();
        }
        return Byte.parseByte(s);
    }

    @Override
    public short asShort() {
        final String s = asString();
        if (s.isEmpty()) {
            return factory.getDefaultShort();
        }
        return Short.parseShort(s);
    }

    @Override
    public int asInt() {
        final String s = asString();
        if (s.isEmpty()) {
            return factory.getDefaultInt();
        }
        return Integer.parseInt(s);
    }

    @Override
    public long asLong() {
        final String s = asString();
        if (s.isEmpty()) {
            return factory.getDefaultLong();
        }
        return Long.parseLong(s);
    }

    @Override
    public float asFloat() {
        final String s = asString();
        if (s.isEmpty()) {
            return factory.getDefaultFloat();
        }
        return Float.parseFloat(s);
    }

    @Override
    public double asDouble() {
        final String s = asString();
        if (s.isEmpty()) {
            return factory.getDefaultDouble();
        }
        return Double.parseDouble(s);
    }

    @Override
    public BigInteger asBigInteger() {
        if (isNull()) {
            return nullable ? null : BigInteger.ZERO;
        }
        final String s = asString();
        return s.isEmpty() ? BigInteger.ZERO : new BigInteger(s);
    }

    @Override
    public BigDecimal asBigDecimal() {
        if (isNull()) {
            return nullable ? null : BigDecimal.ZERO;
        }
        final String s = asString();
        return s.isEmpty() ? BigDecimal.ZERO : new BigDecimal(s);
    }

    @Override
    public InputStream asObject() {
        return isNull() ? (nullable ? value : new ByteArrayInputStream(Constants.EMPTY_BYTE_ARRAY)) : value;
    }

    @Override
    public String asString(Charset charset) {
        if (value == null) {
            return factory.getDefaultString();
        }

        try {
            return Stream.readAllAsString(value, factory.getCharset());
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public String toJsonExpression() {
        if (isNull()) {
            return Constants.NULL_STR;
        }
        try {
            return Arrays.toString(Stream.readAllBytes(value));
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public String toSqlExpression() {
        if (isNull()) {
            return Constants.NULL_EXPR;
        }
        try {
            return Utils.toHex(Stream.readAllBytes(value), true);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public BinaryValue resetToDefault() {
        throw new UnsupportedOperationException(ObjectValue.UNSUPPORTED_RESET_TO_DEFAULT);
    }

    @Override
    public BinaryValue resetToNull() {
        this.value = null;
        return this;
    }

    @Override
    public BinaryValue updateFrom(ResultSet rs, int index) throws SQLException {
        try {
            this.value = rs.getBinaryStream(index);
        } catch (SQLFeatureNotSupportedException | UnsupportedOperationException | NoSuchMethodError e) {
            this.value = new ByteArrayInputStream(rs.getBytes(index));
        }
        return this;
    }
}
