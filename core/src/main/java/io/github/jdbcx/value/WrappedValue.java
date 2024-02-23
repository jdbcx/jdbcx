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
package io.github.jdbcx.value;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;

import io.github.jdbcx.Value;
import io.github.jdbcx.ValueFactory;

public class WrappedValue implements Value {
    protected final Value value;

    private boolean updated;

    protected boolean isUpdated() {
        return updated;
    }

    protected void setUpdated(boolean updated) {
        this.updated = updated;
    }

    protected void ensureUpdated() {
        setUpdated(true);
    }

    protected WrappedValue(Value value) {
        this.value = value;
        this.updated = false;
    }

    @Override
    public ValueFactory getFactory() {
        return value.getFactory();
    }

    @Override
    public boolean isNull() {
        ensureUpdated();
        return value.isNull();
    }

    @Override
    public boolean isNaN() {
        ensureUpdated();
        return value.isNaN();
    }

    @Override
    public byte[] asBinary() {
        ensureUpdated();
        return value.asBinary();
    }

    @Override
    public byte[] asBinary(Charset charset) {
        ensureUpdated();
        return value.asBinary(charset);
    }

    @Override
    public boolean asBoolean() {
        ensureUpdated();
        return value.asBoolean();
    }

    @Override
    public char asChar() {
        ensureUpdated();
        return value.asChar();
    }

    @Override
    public byte asByte() {
        ensureUpdated();
        return value.asByte();
    }

    @Override
    public short asShort() {
        ensureUpdated();
        return value.asShort();
    }

    @Override
    public int asInt() {
        ensureUpdated();
        return value.asInt();
    }

    @Override
    public long asLong() {
        ensureUpdated();
        return value.asLong();
    }

    @Override
    public float asFloat() {
        ensureUpdated();
        return value.asFloat();
    }

    @Override
    public double asDouble() {
        ensureUpdated();
        return value.asDouble();
    }

    @Override
    public BigInteger asBigInteger() {
        ensureUpdated();
        return value.asBigInteger();
    }

    @Override
    public BigDecimal asBigDecimal() {
        ensureUpdated();
        return value.asBigDecimal();
    }

    @Override
    public BigDecimal asBigDecimal(int scale) {
        ensureUpdated();
        return value.asBigDecimal(scale);
    }

    @Override
    public <T extends Enum<T>> T asEnum(Class<T> enumType) {
        ensureUpdated();
        return value.asEnum(enumType);
    }

    @Override
    public Object asObject() {
        ensureUpdated();
        return value.asObject();
    }

    @Override
    public <T> T asObject(Class<T> clazz) {
        ensureUpdated();
        return value.asObject(clazz);
    }

    @Override
    public Date asSqlDate() {
        ensureUpdated();
        return value.asSqlDate();
    }

    @Override
    public Time asSqlTime() {
        ensureUpdated();
        return value.asSqlTime();
    }

    @Override
    public Timestamp asSqlTimestamp() {
        ensureUpdated();
        return value.asSqlTimestamp();
    }

    @Override
    public LocalDate asDate() {
        ensureUpdated();
        return value.asDate();
    }

    @Override
    public LocalTime asTime() {
        ensureUpdated();
        return value.asTime();
    }

    @Override
    public LocalTime asTime(int scale) {
        ensureUpdated();
        return value.asTime(scale);
    }

    @Override
    public LocalDateTime asDateTime() {
        ensureUpdated();
        return value.asDateTime();
    }

    @Override
    public LocalDateTime asDateTime(int scale) {
        ensureUpdated();
        return value.asDateTime(scale);
    }

    @Override
    public Instant asInstant() {
        ensureUpdated();
        return value.asInstant();
    }

    @Override
    public Instant asInstant(int scale) {
        ensureUpdated();
        return value.asInstant(scale);
    }

    @Override
    public OffsetDateTime asOffsetDateTime() {
        ensureUpdated();
        return value.asOffsetDateTime();
    }

    @Override
    public OffsetDateTime asOffsetDateTime(int scale) {
        ensureUpdated();
        return value.asOffsetDateTime(scale);
    }

    @Override
    public ZonedDateTime asZonedDateTime() {
        ensureUpdated();
        return value.asZonedDateTime();
    }

    @Override
    public ZonedDateTime asZonedDateTime(int scale) {
        ensureUpdated();
        return value.asZonedDateTime(scale);
    }

    @Override
    public String asString() {
        ensureUpdated();
        return value.asString();
    }

    @Override
    public String asString(Charset charset) {
        ensureUpdated();
        return value.asString(charset);
    }

    @Override
    public String asAsciiString() {
        ensureUpdated();
        return value.asAsciiString();
    }

    @Override
    public String asUnicodeString() {
        ensureUpdated();
        return value.asUnicodeString();
    }

    @Override
    public String toJsonExpression() {
        ensureUpdated();
        return value.toJsonExpression();
    }

    @Override
    public String toSqlExpression() {
        ensureUpdated();
        return value.toSqlExpression();
    }

    @Override
    public Value resetToDefault() {
        value.resetToDefault();
        this.updated = false;
        return this;
    }

    @Override
    public Value resetToNull() {
        value.resetToNull();
        this.updated = false;
        return this;
    }

    @Override
    public Value updateFrom(ResultSet rs, int index) throws SQLException {
        value.updateFrom(rs, index);
        this.updated = true;
        return this;
    }
}
