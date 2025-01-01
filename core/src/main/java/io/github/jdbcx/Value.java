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
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
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

/**
 * Container object for encapsulating both object and primitive values.
 */
public interface Value extends Serializable {
    /**
     * Gets the factory which created this instance.
     *
     * @return non-null factory
     */
    ValueFactory getFactory();

    /**
     * Checks whether the encapsulated value is null or not.
     *
     * @return true if the encapsulated value is null; false otherwise
     */
    boolean isNull();

    /**
     * Checks if the value is either positive or negative infinity as defined in
     * {@link Double}.
     *
     * @return true if it's infinity; false otherwise
     */
    default boolean isInfinity() {
        double value = asDouble();
        return value == Double.NEGATIVE_INFINITY || value == Double.POSITIVE_INFINITY;
    }

    /**
     * Checks if the value is Not-a-Number (NaN).
     *
     * @return true if the value is NaN; false otherwise
     */
    default boolean isNaN() {
        return Double.isNaN(asDouble());
    }

    /**
     * Gets value as byte array.
     *
     * @return non-null byte array
     */
    byte[] asBinary();

    /**
     * Gets value as byte array.
     *
     * @param charset optional charset
     * @return non-null byte array
     */
    byte[] asBinary(Charset charset);

    /**
     * Gets value as boolean.
     *
     * @return boolean value, {@code null} is treated as
     *         {@link ValueFactory#getDefaultBoolean()}
     */
    boolean asBoolean();

    /**
     * Gets value as char.
     *
     * @return char value, {@code null} is treated as
     *         {@link ValueFactory#getDefaultChar()}
     */
    char asChar();

    /**
     * Gets value as byte.
     *
     * @return byte value, {@code null} is treated as
     *         {@link ValueFactory#getDefaultByte()}
     */
    byte asByte();

    /**
     * Gets value as short.
     *
     * @return short value, {@code null} is treated as
     *         {@link ValueFactory#getDefaultShort()}
     */
    short asShort();

    /**
     * Gets value as integer.
     *
     * @return int value, {@code null} is treated as
     *         {@link ValueFactory#getDefaultInt()}
     */
    int asInt();

    /**
     * Gets value as long.
     *
     * @return long value, {@code null} is treated as
     *         {@link ValueFactory#getDefaultLong()}
     */
    long asLong();

    /**
     * Gets value as float.
     *
     * @return float value, {@code null} is treated as
     *         {@link ValueFactory#getDefaultFloat()}
     */
    float asFloat();

    /**
     * Gets value as double.
     *
     * @return double value, {@code null} is treated as
     *         {@link ValueFactory#getDefaultDouble()}
     */
    double asDouble();

    /**
     * Gets value as big integer.
     *
     * @return big integer, could be {@code null}
     */
    default BigInteger asBigInteger() {
        return isNull() ? null : new BigInteger(asString());
    }

    /**
     * Gets value as big decimal.
     *
     * @return big decimal, could be {@code null}
     */
    default BigDecimal asBigDecimal() {
        return isNull() ? null : new BigDecimal(asString());
    }

    /**
     * Gets value as big decimal.
     *
     * @param scale scale of the {@code BigDecimal}
     * @return big decimal, could be {@code null}
     */
    default BigDecimal asBigDecimal(int scale) {
        return isNull() ? null : new BigDecimal(asBigInteger(), scale);
    }

    /**
     * Gets value as enum.
     *
     * @param <T>      type of the enum
     * @param enumType enum class
     * @return enum, could be {@code null}
     */
    default <T extends Enum<T>> T asEnum(Class<T> enumType) {
        if (isNull() || enumType == null) {
            return null;
        }

        int value = asInt();
        for (T t : enumType.getEnumConstants()) {
            if (t.ordinal() == value) {
                return t;
            }
        }

        throw new IllegalArgumentException(
                Utils.format("Ordinal [%d] not found in enum %s", value, enumType.getName()));
    }

    /**
     * Gets raw value.
     *
     * @return raw value, could be {@code null}
     */
    Object asObject();

    /**
     * Gets value as object.
     *
     * @param <T>   type of the class
     * @param clazz class of the value
     * @return object value, could be {@code null}
     */
    default <T> T asObject(Class<T> clazz) {
        return clazz.cast(asObject());
    }

    /**
     * Gets value as {@link java.sql.Date}.
     *
     * @return date, could be {@code null}
     */
    default Date asSqlDate() {
        return isNull() ? null : Date.valueOf(asDate());
    }

    /**
     * Gets value as {@link java.sql.Time}.
     *
     * @return time, could be {@code null}
     */
    default Time asSqlTime() {
        return isNull() ? null : Time.valueOf(asTime());
    }

    /**
     * Gets value as {@link java.sql.Timestamp}.
     *
     * @return timestamp, could be {@code null}
     */
    default Timestamp asSqlTimestamp() {
        return isNull() ? null : Timestamp.valueOf(asDateTime());
    }

    /**
     * Gets value as {@link java.time.LocalDate}.
     *
     * @return local date, could be {@code null}
     */
    LocalDate asDate();

    /**
     * Gets value as {@link java.time.LocalTime}. Same as {@code asTime(0)}.
     *
     * @return local time, could be {@code null}
     */
    default LocalTime asTime() {
        return asTime(0);
    }

    /**
     * Gets value as {@link java.time.LocalTime}.
     *
     * @param scale scale between 0 (second) and 9 (nano second)
     * @return local time, could be {@code null}
     */
    LocalTime asTime(int scale);

    /**
     * Gets value as {@link java.time.LocalDateTime}. Same as
     * {@code asDateTime(0)}.
     *
     * @return local date time, could be {@code null}
     */
    LocalDateTime asDateTime();

    /**
     * Gets value as {@link java.time.LocalDateTime}.
     *
     * @param scale scale between 0 (second) and 9 (nano second)
     * @return date time, could be {@code null}
     */
    LocalDateTime asDateTime(int scale);

    /**
     * Gets value as {@link java.time.Instant}. Same as
     * {@code asInstant(0)}.
     *
     * @return instant, could be {@code null}
     */
    Instant asInstant();

    /**
     * Gets value as {@link java.time.Instant}.
     *
     * @param scale scale between 0 (second) and 9 (nano second)
     * @return instant, could be {@code null}
     */
    Instant asInstant(int scale);

    /**
     * Gets value as {@link java.time.OffsetDateTime}. Same as
     * {@code asOffsetDateTime(0)}.
     *
     * @return offset date time, could be {@code null}
     */
    OffsetDateTime asOffsetDateTime();

    /**
     * Gets value as {@link java.time.OffsetDateTime}.
     *
     * @param scale scale between 0 (second) and 9 (nano second)
     * @return offset date time, could be {@code null}
     */
    OffsetDateTime asOffsetDateTime(int scale);

    /**
     * Gets value as {@link java.time.ZonedDateTime}. Same as
     * {@code asZonedDateTime(0)}.
     *
     * @return zoned date time, could be {@code null}
     */
    ZonedDateTime asZonedDateTime();

    /**
     * Gets value as {@link java.time.ZonedDateTime}.
     *
     * @param scale scale between 0 (second) and 9 (nano second)
     * @return date time, could be {@code null}
     */
    ZonedDateTime asZonedDateTime(int scale);

    /**
     * Gets value as string. It simply passes {@link ValueFactory#getCharset()} to
     * {@link #asString(Charset)}.
     *
     * @return non-null string value
     */
    String asString();

    /**
     * Gets value as string.
     *
     * @param charset optional charset
     * @return non-null string value
     */
    String asString(Charset charset);

    /**
     * Gets value as string. Same as {@code asString(StandardCharsets.US_ASCII)}.
     *
     * @return non-null string value
     */
    default String asAsciiString() {
        return asString(StandardCharsets.US_ASCII);
    }

    /**
     * Gets value as string. Same as {@code asString(StandardCharsets.UTF_8)}.
     *
     * @return non-null string value
     */
    default String asUnicodeString() {
        return asString(StandardCharsets.UTF_8);
    }

    /**
     * Converts value to JSON expression.
     *
     * @return non-null JSON expression
     */
    default String toJsonExpression() {
        return isNull() ? Constants.NULL_STR : Converter.toJsonExpression(asString());
    }

    /**
     * Converts value to SQL expression.
     *
     * @return non-null SQL expression
     */
    default String toSqlExpression() {
        return isNull() ? Constants.NULL_STR : Converter.toSqlExpression(asString());
    }

    /**
     * Resets value to default.
     *
     * @return this value object
     */
    Value resetToDefault();

    /**
     * Resets value to null.
     *
     * @return this value object
     */
    Value resetToNull();

    /**
     * Updates value from the given resultset.
     *
     * @param rs    non-null resultset containing the new value
     * @param index 1-based index pointing the value
     * @return this value object
     * @throws SQLException when failed to get new value from resultset
     */
    Value updateFrom(ResultSet rs, int index) throws SQLException;
}
