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

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.charset.Charset;
import java.sql.JDBCType;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;

public class ValueFactory implements Serializable {
    private static final Logger log = LoggerFactory.getLogger(ValueFactory.class);

    static final class InstanceHolder {
        static final ValueFactory instance = ValueFactory.newInstance(null);

        private InstanceHolder() {
        }
    }

    public static final Option OPTION_CHARSET = Option.of("charset", "Preferred charset",
            Constants.DEFAULT_CHARSET.name());
    public static final Option OPTION_ROUNDING_MODE = Option.ofEnum("rounding.mode", "Preferred rounding mode", "DOWN",
            RoundingMode.class);
    public static final Option OPTION_TIMEZONE = Option.of("timezone", "Preferred timezone",
            Constants.UTC_TIMEZONE.getID());
    public static final Option OPTION_OFFSET = Option.of("offset", "Preferred zone offset id", "Z");
    public static final Option OPTION_DECIMAL_SCALE = Option.of("decimal.scale", "Preferred scale of decimal", "0");
    public static final Option OPTION_TIME_SCALE = Option.of(
            new String[] { "time.scale", "Preferred scale of time", "0", "1", "2", "3", "4", "5", "6", "7", "8", "9" });
    public static final Option OPTION_TIMESTAMP_SCALE = Option
            .of(new String[] { "timestamp.scale", "Preferred scale of timestamp",
                    "0", "1", "2", "3", "4", "5", "6", "7", "8", "9" });
    public static final Option OPTION_DATE_FORMAT = Option.of("date.format", "Preferred date format", "yyyy-MM-dd");
    public static final Option OPTION_TIME_FORMAT = Option.of("time.format", "Preferred time format", "HH:mm:ss");
    public static final Option OPTION_TIMESTAMP_FORMAT = Option.of("timestamp.format", "Preferred timestamp format",
            "yyyy-MM-dd HH:mm:ss");

    public static final Option OPTION_BOOLEAN = Option.of("bool", "Default value of boolean",
            Constants.FALSE_EXPR);
    public static final Option OPTION_CHAR = Option.of("char", "Default value of char", "\0");
    public static final Option OPTION_BYTE = Option.of("byte", "Default value of byte",
            Constants.ZERO_EXPR);
    public static final Option OPTION_SHORT = Option.of("short", "Default value of short",
            Constants.ZERO_EXPR);
    public static final Option OPTION_INT = Option.of("int", "Default value of int",
            Constants.ZERO_EXPR);
    public static final Option OPTION_LONG = Option.of("long", "Default value of long",
            Constants.ZERO_EXPR);
    public static final Option OPTION_FLOAT = Option.of("float", "Default value of float",
            Constants.ZERO_EXPR);
    public static final Option OPTION_DOUBLE = Option.of("double", "Default value of double",
            Constants.ZERO_EXPR);
    public static final Option OPTION_STRING = Option.of("string", "Default value of string",
            Constants.EMPTY_STRING);
    public static final Option OPTION_BIGINT = Option.of("bigint", "Default value of bigint",
            Constants.ZERO_EXPR);
    public static final Option OPTION_DECIMAL = Option.of("decimal", "Default value of decimal",
            Constants.ZERO_EXPR);
    public static final Option OPTION_DATE = Option.of("date", "Default numeric value of date",
            Constants.ZERO_EXPR);
    public static final Option OPTION_TIME = Option.of("time", "Default numeric value of time",
            Constants.ZERO_EXPR);
    public static final Option OPTION_TIMESTAMP = Option.of("timestamp", "Default numeric value of timestamp",
            Constants.ZERO_EXPR);

    public static ValueFactory newInstance(Properties config, TypeMapping... mappings) {
        return new ValueFactory(config, mappings);
    }

    public static ValueFactory getInstance() {
        return InstanceHolder.instance;
    }

    static DateTimeFormatter newFormatter(String format) {
        return format.lastIndexOf('s') == format.length() - 1
                ? new DateTimeFormatterBuilder().appendPattern(format).optionalStart()
                        .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
                        .optionalEnd().toFormatter()
                : DateTimeFormatter.ofPattern(format);
    }

    // configuration
    protected final transient Charset charset;
    protected final RoundingMode roundingMode;
    protected final TimeZone timeZone;
    protected final ZoneId zoneId;
    protected final ZoneOffset zoneOffset;
    protected final int decimalScale;
    protected final int timeScale;
    protected final int timestampScale;
    protected final transient DateTimeFormatter dateFormatter;
    protected final transient DateTimeFormatter timeFormatter;
    protected final transient DateTimeFormatter timestampFormatter;

    // default values
    protected final boolean defaultBool;
    protected final byte defaultByte;
    protected final short defaultShort;
    protected final int defaultInt;
    protected final long defaultLong;
    protected final float defaultFloat;
    protected final double defaultDouble;
    protected final char defaultChar;
    protected final String defaultString;
    protected final BigInteger defaultBigint;
    protected final BigDecimal defaultDecimal;
    protected final LocalDate defaultDate;
    protected final LocalTime defaultTime;
    protected final LocalDateTime defaultTimestamp;

    // mappings from JDBC and/or database type to Value
    private final List<TypeMapping> mappings;

    protected ValueFactory(Properties config, List<TypeMapping> mappings) {
        this(config,
                mappings == null || mappings.isEmpty() ? new TypeMapping[0] : mappings.toArray(new TypeMapping[0]));
    }

    protected ValueFactory(Properties config, TypeMapping... mappings) {
        this.charset = Charset.forName(OPTION_CHARSET.getValue(config));
        this.roundingMode = RoundingMode.valueOf(OPTION_ROUNDING_MODE.getValue(config));
        this.timeZone = TimeZone.getTimeZone(OPTION_TIMEZONE.getValue(config));
        this.zoneId = this.timeZone.toZoneId();
        this.zoneOffset = ZoneOffset.of(OPTION_OFFSET.getValue(config));
        this.decimalScale = Integer.parseInt(OPTION_DECIMAL_SCALE.getValue(config));
        this.timeScale = Integer.parseInt(OPTION_TIME_SCALE.getValue(config));
        this.timestampScale = Integer.parseInt(OPTION_TIMESTAMP_SCALE.getValue(config));
        this.dateFormatter = newFormatter(OPTION_DATE_FORMAT.getValue(config));
        this.timeFormatter = newFormatter(OPTION_TIME_FORMAT.getValue(config));
        this.timestampFormatter = newFormatter(OPTION_TIMESTAMP_FORMAT.getValue(config));

        this.defaultBool = Converter.toBoolean(OPTION_BOOLEAN.getValue(config));
        this.defaultByte = Byte.parseByte(OPTION_BYTE.getValue(config));
        this.defaultChar = OPTION_CHAR.getValue(config).charAt(0);
        this.defaultShort = Short.parseShort(OPTION_SHORT.getValue(config));
        this.defaultInt = Integer.parseInt(OPTION_INT.getValue(config));
        this.defaultLong = Long.parseLong(OPTION_LONG.getValue(config));
        this.defaultFloat = Float.parseFloat(OPTION_FLOAT.getValue(config));
        this.defaultDouble = Double.parseDouble(OPTION_DOUBLE.getValue(config));
        this.defaultBigint = new BigInteger(OPTION_BIGINT.getValue(config));
        this.defaultDecimal = new BigDecimal(OPTION_DECIMAL.getValue(config));
        this.defaultDate = LocalDate.ofEpochDay(Integer.parseInt(OPTION_DATE.getValue(config)));
        this.defaultTime = LocalTime.ofSecondOfDay(Integer.parseInt(OPTION_TIME.getValue(config)));
        this.defaultTimestamp = LocalDateTime.ofEpochSecond(Long.parseLong(OPTION_TIMESTAMP.getValue(config)), 0,
                ZoneOffset.UTC);
        this.defaultString = OPTION_STRING.getValue(config);

        this.mappings = Utils.toImmutableList(TypeMapping.class, mappings);
    }

    public Value newValue(Field field, String databaseType) {
        final JDBCType jdbcType = field.type();
        final boolean nullable = field.isNullable();
        final boolean signed = field.isSigned();
        final int precision = field.precision();
        final int scale = field.scale();
        Class<? extends Value> valueType = null;
        for (TypeMapping mapping : mappings) {
            if (mapping.accept(jdbcType, databaseType)) {
                valueType = mapping.getMappedType();
                break;
            }
        }
        if (valueType != null) {
            return Values.of(this, valueType);
        }

        final Value value;
        switch (jdbcType) {
            case BIT:
                if (precision > 64) {
                    value = Values.ofBigInteger(this, nullable, null);
                } else if (precision > 32) {
                    value = Values.ofLong(this, nullable, true, defaultLong);
                } else if (precision > 16) {
                    value = Values.ofInt(this, nullable, true, defaultInt);
                } else if (precision > 8) {
                    value = Values.ofShort(this, nullable, true, defaultShort);
                } else if (precision > 1) {
                    value = Values.ofByte(this, nullable, true, defaultByte);
                } else {
                    value = Values.ofBoolean(this, nullable, defaultBool);
                }
                break;
            case BOOLEAN:
                value = Values.ofBoolean(this, nullable, defaultBool);
                break;
            case CHAR:
            case NCHAR:
                value = Values.ofString(this, nullable, precision, defaultString);
                break;
            case TINYINT:
                value = Values.ofByte(this, nullable, signed, defaultByte);
                break;
            case SMALLINT:
                value = Values.ofShort(this, nullable, signed, defaultShort);
                break;
            case INTEGER:
                value = Values.ofInt(this, nullable, signed, defaultInt);
                break;
            case BIGINT:
                // subclassing to take care of signed/unsigned
                value = Values.ofBigInteger(this, nullable, null);
                break;
            case REAL:
            case FLOAT:
                value = Values.ofFloat(this, nullable, defaultFloat);
                break;
            case DOUBLE:
                value = Values.ofDouble(this, nullable, defaultDouble);
                break;
            case NUMERIC:
            case DECIMAL:
                // subclassing to take care of precision
                value = Values.ofBigDecimal(this, nullable, scale, null);
                break;
            case DATE:
                value = Values.ofDate(this, nullable, null);
                break;
            case TIME:
                value = Values.ofTime(this, nullable, scale, null);
                break;
            case TIMESTAMP:
            case TIME_WITH_TIMEZONE:
            case TIMESTAMP_WITH_TIMEZONE:
                value = Values.ofTimestamp(this, nullable, scale, null);
                break;
            case ARRAY:
            case OTHER:
            case VARCHAR:
            case NVARCHAR:
            case LONGVARCHAR:
            case LONGNVARCHAR:
                value = Values.ofString(this, nullable, 0, null);
                break;
            case NULL:
                value = Values.ofNull(this);
                break;
            default:
                log.warn("Unsupported type [jdbc=%s, db=%s], which will be treated as [%s]", jdbcType, databaseType,
                        Values.DEFAULT_VALUE_TYPE);
                value = Values.ofString(this, nullable, 0, null);
                break;
        }
        return value;
    }

    /**
     * Gets charset.
     *
     * @return non-null charset
     */
    public Charset getCharset() {
        return charset != null ? charset : Constants.DEFAULT_CHARSET;
    }

    /**
     * Gets rounding mode.
     *
     * @return non-null rounding mode
     */
    public RoundingMode getRoundingMode() {
        return roundingMode;
    }

    /**
     * Gets timezone.
     *
     * @return non-null timezone
     */
    public TimeZone getTimeZone() {
        return timeZone;
    }

    /**
     * Gets zone id. Same as {@code getTimeZone().toZoneId()}.
     *
     * @return non-null zone id
     */
    public ZoneId getZoneId() {
        return zoneId;
    }

    /**
     * Gets zone offset.
     *
     * @return non-null zone offset
     */
    public ZoneOffset getZoneOffset() {
        return zoneOffset;
    }

    /**
     * Gets scale of decimal value.
     *
     * @return scale of decimal value
     */
    public int getDecimalScale() {
        return decimalScale;
    }

    /**
     * Gets scale of time value.
     *
     * @return scale of time value, from 0 (second) to 9 (nanosecond)
     */
    public int getTimeScale() {
        return timeScale;
    }

    /**
     * Gets scale of timestamp value.
     *
     * @return scale of timestamp value, from 0 (second) to 9 (nanosecond)
     */
    public int getTimestampScale() {
        return timestampScale;
    }

    /**
     * Gets date formatter.
     *
     * @return non-null date formatter
     */
    public DateTimeFormatter getDateFormatter() {
        return dateFormatter != null ? dateFormatter : Constants.DEFAULT_DATE_FORMATTER;
    }

    /**
     * Gets time formatter.
     *
     * @return non-null time formatter
     */
    public DateTimeFormatter getTimeFormatter() {
        return timeFormatter != null ? timeFormatter : Constants.DEFAULT_TIME_FORMATTER;
    }

    /**
     * Same as {@link #getTimestampFormatter()}.
     *
     * @return non-null date time formatter
     */
    public DateTimeFormatter getDateTimeFormatter() {
        return timestampFormatter != null ? timestampFormatter : Constants.DEFAULT_TIMESTAMP_FORMATTER;
    }

    /**
     * Gets timestamp formatter.
     *
     * @return non-null timestamp formatter
     */
    public DateTimeFormatter getTimestampFormatter() {
        return timestampFormatter;
    }

    public boolean getDefaultBoolean() {
        return defaultBool;
    }

    public char getDefaultChar() {
        return defaultChar;
    }

    public byte getDefaultByte() {
        return defaultByte;
    }

    public short getDefaultShort() {
        return defaultShort;
    }

    public int getDefaultInt() {
        return defaultInt;
    }

    public long getDefaultLong() {
        return defaultLong;
    }

    public float getDefaultFloat() {
        return defaultFloat;
    }

    public double getDefaultDouble() {
        return defaultDouble;
    }

    public BigInteger getDefaultBigInteger() {
        return defaultBigint;
    }

    public BigDecimal getDefaultBigDecimal() {
        return defaultDecimal;
    }

    public LocalDate getDefaultDate() {
        return defaultDate;
    }

    public LocalTime getDefaultTime() {
        return defaultTime;
    }

    public LocalDateTime getDefaultTimestamp() {
        return defaultTimestamp;
    }

    public String getDefaultString() {
        return defaultString;
    }

    public String getDefaultString(int length) {
        if (length < 1) {
            return defaultString;
        }
        StringBuilder builder = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            builder.append(defaultChar);
        }
        return builder.toString();
    }

    public List<TypeMapping> getMappings() {
        return mappings;
    }
}
