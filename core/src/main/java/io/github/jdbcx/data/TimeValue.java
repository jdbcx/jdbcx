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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;

import io.github.jdbcx.Constants;
import io.github.jdbcx.Converter;
import io.github.jdbcx.ValueFactory;

public class TimeValue extends ObjectValue<LocalTime> {
    public static TimeValue of(ValueFactory factory, boolean nullable, LocalTime value) {
        return new TimeValue(factory, nullable, value);
    }

    public static TimeValue of(ValueFactory factory, boolean nullable, int scale, LocalTime value) {
        return new TimeValue(factory, nullable, scale, value);
    }

    private final int scale;

    @Override
    protected TimeValue set(LocalTime value) {
        if (value == null) {
            super.set(nullable ? null : factory.getDefaultTime());
        } else {
            if (value.getNano() == 0) {
                super.set(value);
            } else {
                final LocalTime t;
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

    protected TimeValue(ValueFactory factory, boolean nullable, LocalTime value) {
        super(factory, nullable, value);
        this.scale = normalizeTimeScale(this.factory.getTimeScale());
        set(value);
    }

    protected TimeValue(ValueFactory factory, boolean nullable, int scale, LocalTime value) {
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
        return isNull() ? factory.getDefaultInt() : asObject().toSecondOfDay();
    }

    @Override
    public long asLong() {
        return isNull() ? factory.getDefaultLong() : asObject().toSecondOfDay();
    }

    @Override
    public float asFloat() {
        return isNull() ? factory.getDefaultFloat()
                : (float) ((double) asObject().toNanoOfDay() / getTimeDivisor(Constants.MIN_TIME_SCALE));
    }

    @Override
    public double asDouble() {
        return isNull() ? factory.getDefaultDouble()
                : (double) asObject().toNanoOfDay() / getTimeDivisor(Constants.MIN_TIME_SCALE);
    }

    @Override
    public BigInteger asBigInteger() {
        return isNull() ? null : BigInteger.valueOf(asLong());
    }

    @Override
    public BigDecimal asBigDecimal() {
        return isNull() ? null
                : BigDecimal.valueOf((double) asObject().toNanoOfDay() / getTimeDivisor(Constants.MIN_TIME_SCALE))
                        .setScale(scale, factory.getRoundingMode());
    }

    @Override
    public BigDecimal asBigDecimal(int scale) {
        return isNull() ? null
                : BigDecimal.valueOf((double) asObject().toNanoOfDay() / getTimeDivisor(Constants.MIN_TIME_SCALE))
                        .setScale(scale, factory.getRoundingMode());
    }

    @Override
    public LocalDate asDate() {
        if (isNull()) {
            return null;
        }
        return factory.getDefaultDate();
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
        LocalTime value = asObject();
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
    public LocalDateTime asDateTime() {
        if (isNull()) {
            return null;
        }
        return LocalDateTime.of(factory.getDefaultDate(), asTime());
    }

    @Override
    public LocalDateTime asDateTime(int scale) {
        if (isNull()) {
            return null;
        }
        return LocalDateTime.of(factory.getDefaultDate(), asTime(scale));
    }

    @Override
    public String asString(Charset charset) {
        if (isNull()) {
            return factory.getDefaultString();
        }
        return factory.getTimeFormatter().format(asObject());
    }

    @Override
    public String toJsonExpression() {
        if (isNull()) {
            return Constants.NULL_STR;
        }
        return Converter.toJsonExpression(factory.getTimeFormatter().format(asObject()));
    }

    @Override
    public String toSqlExpression() {
        if (isNull()) {
            return Constants.NULL_EXPR;
        }
        return Converter.toSqlExpression(factory.getTimeFormatter().format(asObject()));
    }

    @Override
    public TimeValue updateFrom(ResultSet rs, int index) throws SQLException {
        final Time time = rs.getTime(index);
        set(time != null ? time.toLocalTime() : null);
        return this;
    }
}
