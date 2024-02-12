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
package io.github.jdbcx.data;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

import io.github.jdbcx.Constants;
import io.github.jdbcx.Converter;
import io.github.jdbcx.ValueFactory;

public class DateValue extends ObjectValue<LocalDate> {
    public static DateValue of(ValueFactory factory, boolean nullable, LocalDate value) {
        return new DateValue(factory, nullable, value);
    }

    @Override
    protected DateValue set(LocalDate value) {
        if (nullable || value != null) {
            super.set(value);
        } else {
            super.set(factory.getDefaultDate());
        }
        return this;
    }

    protected DateValue(ValueFactory factory, boolean nullable, LocalDate value) {
        super(factory, nullable, value);
        if (!nullable && value == null) {
            set(this.factory.getDefaultDate());
        }
    }

    @Override
    public boolean asBoolean() {
        return isNull() ? factory.getDefaultBoolean() : asLong() != 0L;
    }

    @Override
    public byte asByte() {
        return isNull() ? factory.getDefaultByte() : (byte) asLong();
    }

    @Override
    public char asChar() {
        return isNull() ? factory.getDefaultChar() : (char) asLong();
    }

    @Override
    public short asShort() {
        return isNull() ? factory.getDefaultShort() : (short) asLong();
    }

    @Override
    public int asInt() {
        return isNull() ? factory.getDefaultInt() : (int) asLong();
    }

    @Override
    public long asLong() {
        return isNull() ? factory.getDefaultLong() : asObject().toEpochDay();
    }

    @Override
    public float asFloat() {
        return isNull() ? factory.getDefaultFloat() : (float) asLong();
    }

    @Override
    public double asDouble() {
        return isNull() ? factory.getDefaultDouble() : (double) asLong();
    }

    @Override
    public BigInteger asBigInteger() {
        return isNull() ? null : BigInteger.valueOf(asObject().toEpochDay());
    }

    @Override
    public BigDecimal asBigDecimal() {
        return isNull() ? null : new BigDecimal(asBigInteger(), factory.getDecimalScale());
    }

    @Override
    public LocalDate asDate() {
        return asObject();
    }

    @Override
    public LocalTime asTime(int scale) {
        if (isNull()) {
            return null;
        }
        return factory.getDefaultTime();
    }

    @Override
    public LocalDateTime asDateTime(int scale) {
        if (isNull()) {
            return null;
        }
        return LocalDateTime.of(asObject(), factory.getDefaultTime());
    }

    @Override
    public String asString(Charset charset) {
        if (isNull()) {
            return factory.getDefaultString();
        }
        return factory.getDateFormatter().format(asObject());
    }

    @Override
    public BigDecimal asBigDecimal(int scale) {
        return isNull() ? null : new BigDecimal(asBigInteger(), scale);
    }

    @Override
    public String toJsonExpression() {
        if (isNull()) {
            return Constants.NULL_STR;
        }
        return Converter.toJsonExpression(factory.getDateFormatter().format(asObject()));
    }

    @Override
    public String toSqlExpression() {
        if (isNull()) {
            return Constants.NULL_EXPR;
        }
        return Converter.toSqlExpression(factory.getDateFormatter().format(asObject()));
    }

    @Override
    public DateValue updateFrom(ResultSet rs, int index) throws SQLException {
        final Date date = rs.getDate(index);
        set(date != null ? date.toLocalDate() : null);
        return this;
    }
}
