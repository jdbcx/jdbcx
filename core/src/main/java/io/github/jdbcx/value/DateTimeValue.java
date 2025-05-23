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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;

import io.github.jdbcx.Constants;
import io.github.jdbcx.Converter;
import io.github.jdbcx.Utils;
import io.github.jdbcx.ValueFactory;

public class DateTimeValue extends ObjectValue<LocalDateTime> {
    public static DateTimeValue of(ValueFactory factory, boolean nullable, LocalDateTime value) {
        return new DateTimeValue(factory, nullable, value);
    }

    public static DateTimeValue of(ValueFactory factory, boolean nullable, int scale, LocalDateTime value) {
        return new DateTimeValue(factory, nullable, scale, value);
    }

    private final int scale;

    @Override
    protected DateTimeValue set(LocalDateTime value) {
        if (value == null) {
            super.set(nullable ? null : factory.getDefaultTimestamp());
        } else {
            if (value.getNano() == 0) {
                super.set(value);
            } else {
                final LocalDateTime t;
                if (scale == Constants.MIN_TIME_SCALE) {
                    t = value.truncatedTo(ChronoUnit.SECONDS);
                } else {
                    int divisor = getTimeDivisor(scale);
                    t = value.withNano((value.getNano() / divisor) * divisor);
                }
                super.set(t);
            }
        }
        return this;
    }

    protected DateTimeValue(ValueFactory factory, boolean nullable, LocalDateTime value) {
        super(factory, nullable, value);
        this.scale = normalizeTimeScale(this.factory.getTimestampScale());
        set(value);
    }

    protected DateTimeValue(ValueFactory factory, boolean nullable, int scale, LocalDateTime value) {
        super(factory, nullable, value);
        this.scale = normalizeTimeScale(scale);
        set(value);
    }

    @Override
    public boolean asBoolean() {
        return isNull() ? factory.getDefaultBoolean() : asInt() != 0;
    }

    @Override
    public byte asByte() {
        return isNull() ? factory.getDefaultByte() : (byte) asInt();
    }

    @Override
    public char asChar() {
        return isNull() ? factory.getDefaultChar() : (char) asInt();
    }

    @Override
    public short asShort() {
        return isNull() ? factory.getDefaultShort() : (short) asInt();
    }

    @Override
    public int asInt() {
        return isNull() ? factory.getDefaultInt() : (int) asObject().toEpochSecond(factory.getZoneOffset());
    }

    @Override
    public long asLong() {
        return isNull() ? factory.getDefaultLong()
                : Utils.toEpochNanoSeconds(asObject(), factory.getZoneOffset()) / getTimeDivisor(scale);
    }

    @Override
    public float asFloat() {
        if (isNull()) {
            return factory.getDefaultFloat();
        }
        return (float) asDouble();
    }

    @Override
    public double asDouble() {
        if (isNull()) {
            return factory.getDefaultDouble();
        }

        return (double) Utils.toEpochNanoSeconds(asObject(), factory.getZoneOffset()) / getTimeDivisor(scale);
    }

    @Override
    public BigInteger asBigInteger() {
        return isNull() ? null : BigInteger.valueOf(asLong());
    }

    @Override
    public BigDecimal asBigDecimal() {
        return isNull() ? null : new BigDecimal(asBigInteger(), scale);
    }

    @Override
    public BigDecimal asBigDecimal(int scale) {
        return isNull() ? null : new BigDecimal(asBigInteger(), scale);
    }

    @Override
    public LocalDate asDate() {
        if (isNull()) {
            return null;
        }
        return asDateTime().toLocalDate();
    }

    @Override
    public LocalTime asTime() {
        return asTime(scale);
    }

    @Override
    public LocalTime asTime(int scale) {
        if (isNull()) {
            return null;
        }
        final int normalizedScale = normalizeTimeScale(scale);
        LocalTime value = asObject().toLocalTime();
        if (normalizedScale < scale && value.getNano() != 0) {
            if (scale == Constants.MIN_TIME_SCALE) {
                value = value.truncatedTo(ChronoUnit.SECONDS);
            } else {
                int divisor = getTimeDivisor(scale);
                value = value.withNano((value.getNano() / divisor) * divisor);
            }
        }
        return value;
    }

    @Override
    public LocalDateTime asDateTime() {
        return asObject();
    }

    @Override
    public LocalDateTime asDateTime(int scale) {
        if (isNull()) {
            return null;
        }
        final int normalizedScale = normalizeTimeScale(scale);
        LocalDateTime value = asObject();
        if (normalizedScale < this.scale && value.getNano() != 0) {
            if (normalizedScale == Constants.MIN_TIME_SCALE) {
                value = value.truncatedTo(ChronoUnit.SECONDS);
            } else {
                int divisor = getTimeDivisor(normalizedScale);
                value = value.withNano((value.getNano() / divisor) * divisor);
            }
        }
        return value;
    }

    @Override
    public String asString(Charset charset) {
        if (isNull()) {
            return factory.getDefaultString();
        }
        return factory.getTimestampFormatter().format(asObject());
    }

    @Override
    public String toJsonExpression() {
        if (isNull()) {
            return Constants.NULL_STR;
        }
        return Converter.toJsonExpression(factory.getTimestampFormatter().format(asObject()));
    }

    @Override
    public String toSqlExpression() {
        if (isNull()) {
            return Constants.NULL_EXPR;
        }
        return Converter.toSqlExpression(factory.getTimestampFormatter().format(asObject()));
    }

    @Override
    public DateTimeValue updateFrom(ResultSet rs, int index) throws SQLException {
        final Timestamp ts = rs.getTimestamp(index);
        set(ts != null ? ts.toLocalDateTime() : null);
        return this;
    }
}
