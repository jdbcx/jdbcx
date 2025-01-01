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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.TimeZone;
import java.util.UUID;

/**
 * Helper class for converting one value to another.
 */
public final class Converter {
    static final BigInteger BIGINT_HL_BOUNDARY = BigInteger.ONE.shiftLeft(64); // 2^64
    static final BigInteger BIGINT_SL_BOUNDARY = BigInteger.valueOf(Long.MAX_VALUE);

    static final BigDecimal NANOS = new BigDecimal(BigInteger.TEN.pow(9));

    private static final String[] REPLACEMENT_CHARS;
    private static final String[] HTML_SAFE_REPLACEMENT_CHARS;

    static {
        REPLACEMENT_CHARS = new String[128];
        for (int i = 0; i <= 0x1f; i++) {
            REPLACEMENT_CHARS[i] = String.format("\\u%04x", i);
        }
        REPLACEMENT_CHARS['"'] = "\\\"";
        REPLACEMENT_CHARS['\\'] = "\\\\";
        REPLACEMENT_CHARS['\t'] = "\\t";
        REPLACEMENT_CHARS['\b'] = "\\b";
        REPLACEMENT_CHARS['\n'] = "\\n";
        REPLACEMENT_CHARS['\r'] = "\\r";
        REPLACEMENT_CHARS['\f'] = "\\f";
        HTML_SAFE_REPLACEMENT_CHARS = REPLACEMENT_CHARS.clone();
        HTML_SAFE_REPLACEMENT_CHARS['<'] = "\\u003c";
        HTML_SAFE_REPLACEMENT_CHARS['>'] = "\\u003e";
        HTML_SAFE_REPLACEMENT_CHARS['&'] = "\\u0026";
        HTML_SAFE_REPLACEMENT_CHARS['='] = "\\u003d";
        HTML_SAFE_REPLACEMENT_CHARS['\''] = "\\u0027";
    }

    /**
     * Converts UUID to big integer.
     *
     * @param value UUID
     * @return big integer, could be {@code null}
     */

    public static BigInteger toBigInteger(UUID value) {
        if (value == null) {
            return null;
        }

        BigInteger high = BigInteger.valueOf(value.getMostSignificantBits());
        BigInteger low = BigInteger.valueOf(value.getLeastSignificantBits());

        if (high.signum() < 0) {
            high = high.add(BIGINT_HL_BOUNDARY);
        }
        if (low.signum() < 0) {
            low = low.add(BIGINT_HL_BOUNDARY);
        }

        return low.add(high.multiply(BIGINT_HL_BOUNDARY));
    }

    /**
     * Converts given character to boolean value.
     *
     * @param value character represents a boolean value
     * @return boolean value
     */
    public static boolean toBoolean(char value) {
        // not going to support V/X, ✓/✗ and 是/否 as they're less common
        if (value == '1' || value == 'T' || value == 't' || value == 'Y' || value == 'y') {
            return true;
        } else if (value == '0' || value == 'F' || value == 'f' || value == 'N' || value == 'n') {
            return false;
        } else {
            throw new IllegalArgumentException("Invalid boolean value, please use 1/0, T/F, Y/N");
        }
    }

    /**
     * Converts given string to boolean value.
     *
     * @param value string represents a boolean value
     * @return boolean value
     */
    public static boolean toBoolean(String value) {
        if (value == null || value.isEmpty() || Constants.ZERO_EXPR.equals(value)
                || Constants.FALSE_EXPR.equalsIgnoreCase(value) || Constants.NO_EXPR.equalsIgnoreCase(value)) {
            return false;
        } else if (Constants.ONE_EXPR.equals(value) || Constants.TRUE_EXPR.equalsIgnoreCase(value)
                || Constants.YES_EXPR.equalsIgnoreCase(value)) {
            return true;
        } else {
            throw new IllegalArgumentException("Invalid boolean value, please use 1/0, true/false, yes/no");
        }
    }

    /**
     * Converts big decimal to instant.
     *
     * @param value big decimal
     * @return instant, could be {@code null}
     */
    public static Instant toInstant(BigDecimal value) {
        if (value == null) {
            return null;
        } else if (value.scale() == 0) {
            return Instant.ofEpochSecond(value.longValue());
        } else if (value.signum() >= 0) {
            return Instant.ofEpochSecond(value.longValue(),
                    value.remainder(BigDecimal.ONE).multiply(NANOS).intValue());
        }

        long v = NANOS.add(value.remainder(BigDecimal.ONE).multiply(NANOS)).longValue();
        int nanoSeconds = v < 1000000000L ? (int) v : 0;

        return Instant.ofEpochSecond(value.longValue() - (nanoSeconds > 0 ? 1 : 0), nanoSeconds);
    }

    /**
     * Converts big decimal to local date time.
     *
     * @param value big decimal
     * @return local date time, could be {@code null}
     */
    public static LocalDateTime toDateTime(BigDecimal value) {
        return value != null ? LocalDateTime.ofInstant(toInstant(value), ZoneOffset.UTC) : null;
    }

    /**
     * Converts big decimal to zoned date time.
     *
     * @param value big decimal
     * @param tz    time zone, null is treated as UTC
     * @return zoned date time, could be {@code null}
     */
    public static ZonedDateTime toDateTime(BigDecimal value, TimeZone tz) {
        if (value == null) {
            return null;
        }
        return toDateTime(value, tz != null ? tz.toZoneId() : Constants.UTC_ZONE);
    }

    /**
     * Converts big decimal to zoned date time.
     *
     * @param value big decimal
     * @param zone  zone id, null is treated as UTC
     * @return zoned date time, could be {@code null}
     */
    public static ZonedDateTime toDateTime(BigDecimal value, ZoneId zone) {
        if (value == null) {
            return null;
        }
        return toInstant(value).atZone(zone != null ? zone : Constants.UTC_ZONE);
    }

    /**
     * Converts big decimal to offset date time.
     *
     * @param value  big decimal
     * @param offset zone offset, null is treated as {@code ZoneOffset.UTC}
     * @return offset date time, could be {@code null}
     */
    public static OffsetDateTime toDateTime(BigDecimal value, ZoneOffset offset) {
        if (value == null) {
            return null;
        }
        return toInstant(value).atOffset(offset != null ? offset : ZoneOffset.UTC);
    }

    public static String toJsonExpression(String value) {
        if (value == null) {
            return Constants.NULL_STR;
        }

        String[] replacements = REPLACEMENT_CHARS;
        int last = 0;
        int len = value.length();
        StringBuilder builder = new StringBuilder(len + 2).append('"');
        for (int i = 0; i < len; i++) { // NOSONAR
            char ch = value.charAt(i);
            String replacement;
            if (ch < 128) {
                replacement = replacements[ch];
                if (replacement == null) {
                    continue;
                }
            } else if (ch == '\u2028') {
                replacement = "\\u2028";
            } else if (ch == '\u2029') {
                replacement = "\\u2029";
            } else {
                continue;
            }
            if (last < i) {
                builder.append(value, last, i);
            }
            builder.append(replacement);
            last = i + 1;
        }
        if (last < len) {
            builder.append(value, last, len);
        }
        return builder.append('"').toString();
    }

    public static String toSqlExpression(String value) {
        if (value == null) {
            return Constants.NULL_EXPR;
        }

        int len = value.length();
        StringBuilder builder = new StringBuilder(len + 2).append('\'');
        for (int i = 0; i < len; i++) {
            char ch = value.charAt(i);
            if (ch == '\'') {
                builder.append('\\').append(ch);
            } else if (ch == '\\') {
                builder.append('\\').append(ch);
            } else {
                builder.append(ch);
            }
        }
        return builder.append('\'').toString();
    }

    private Converter() {
    }
}
