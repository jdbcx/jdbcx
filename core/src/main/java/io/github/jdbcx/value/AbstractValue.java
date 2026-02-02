/*
 * Copyright 2022-2026, Zhichun Wu
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

import java.nio.charset.Charset;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;

import io.github.jdbcx.Constants;
import io.github.jdbcx.Converter;
import io.github.jdbcx.Value;
import io.github.jdbcx.ValueFactory;

abstract class AbstractValue implements Value {
    private static final int[] timeScales = new int[Constants.MAX_TIME_SCALE + 1];

    static {
        for (int i = 0; i < timeScales.length; i++) {
            timeScales[i] = (int) Math.pow(10, (double) Constants.MAX_TIME_SCALE - i);
        }
    }

    protected final ValueFactory factory;
    protected final boolean nullable;

    protected AbstractValue(ValueFactory factory, boolean nullable) {
        this.factory = factory != null ? factory : ValueFactory.getInstance();
        this.nullable = nullable;
        // NEVER call set or reset* method here,
        // leave the fancy part to concrete implementation
    }

    protected final int normalizeTimeScale(int scale) {
        if (scale < Constants.MIN_TIME_SCALE) {
            scale = Constants.MIN_TIME_SCALE;
        } else if (scale > Constants.MAX_TIME_SCALE) {
            scale = Constants.MAX_TIME_SCALE;
        }
        return scale;
    }

    protected final int getTimeDivisor(int normalizedScale) {
        return timeScales[normalizedScale];
    }

    @Override
    public ValueFactory getFactory() {
        return factory;
    }

    @Override
    public byte[] asBinary() {
        return asBinary(factory.getCharset());
    }

    @Override
    public byte[] asBinary(Charset charset) {
        return asString().getBytes(charset != null ? charset : Constants.DEFAULT_CHARSET);
    }

    @Override
    public boolean asBoolean() {
        return isNull() ? factory.getDefaultBoolean() : Converter.toBoolean(asString());
    }

    @Override
    public char asChar() {
        return isNull() ? factory.getDefaultChar() : (char) (0xFFFF & asShort());
    }

    @Override
    public byte asByte() {
        return isNull() ? factory.getDefaultByte() : Byte.parseByte(asString());
    }

    @Override
    public short asShort() {
        return isNull() ? factory.getDefaultShort() : Short.parseShort(asString());
    }

    @Override
    public int asInt() {
        return isNull() ? factory.getDefaultInt() : Integer.parseInt(asString());
    }

    @Override
    public long asLong() {
        return isNull() ? factory.getDefaultLong() : Long.parseLong(asString());
    }

    @Override
    public float asFloat() {
        return isNull() ? factory.getDefaultFloat() : Float.parseFloat(asString());
    }

    @Override
    public double asDouble() {
        return isNull() ? factory.getDefaultDouble() : Double.parseDouble(asString());
    }

    @Override
    public LocalDate asDate() {
        return isNull() ? null : LocalDate.parse(asString(), factory.getDateFormatter());
    }

    @Override
    public LocalTime asTime() {
        return asTime(factory.getTimeScale());
    }

    @Override
    public LocalTime asTime(int scale) {
        if (isNull()) {
            return null;
        }
        LocalTime t = LocalTime.parse(asString(), factory.getTimeFormatter());
        if (t.getNano() != 0) {
            scale = normalizeTimeScale(scale);
            if (scale == Constants.MIN_TIME_SCALE) {
                t = t.truncatedTo(ChronoUnit.SECONDS);
            } else {
                int divisor = getTimeDivisor(scale);
                t = t.withNano((t.getNano() / divisor) * divisor);
            }
        }
        return t;
    }

    @Override
    public LocalDateTime asDateTime() {
        return asDateTime(factory.getTimestampScale());
    }

    @Override
    public LocalDateTime asDateTime(int scale) {
        if (isNull()) {
            return null;
        }
        LocalDateTime dt = LocalDateTime.parse(asString(), factory.getTimestampFormatter());
        if (dt.getNano() != 0) {
            scale = normalizeTimeScale(scale);
            if (scale == Constants.MIN_TIME_SCALE) {
                dt = dt.truncatedTo(ChronoUnit.SECONDS);
            } else {
                int divisor = getTimeDivisor(scale);
                dt = dt.withNano((dt.getNano() / divisor) * divisor);
            }
        }
        return dt;
    }

    @Override
    public Instant asInstant() {
        return asInstant(factory.getTimestampScale());
    }

    @Override
    public Instant asInstant(int scale) {
        return isNull() ? null : Converter.toInstant(asBigDecimal(normalizeTimeScale(scale)));
    }

    @Override
    public OffsetDateTime asOffsetDateTime() {
        return asOffsetDateTime(factory.getTimestampScale());
    }

    @Override
    public OffsetDateTime asOffsetDateTime(int scale) {
        return isNull() ? null : asDateTime(scale).atOffset(factory.getZoneOffset());
    }

    @Override
    public ZonedDateTime asZonedDateTime() {
        return asZonedDateTime(factory.getTimestampScale());
    }

    @Override
    public ZonedDateTime asZonedDateTime(int scale) {
        return isNull() ? null : asDateTime(scale).atZone(factory.getZoneId());
    }

    @Override
    public String asString() {
        return asString(factory.getCharset());
    }
}
