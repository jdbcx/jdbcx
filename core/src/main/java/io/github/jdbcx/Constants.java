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
package io.github.jdbcx;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Locale;
import java.util.TimeZone;

/**
 * This class contains shared constants that are accessible by all classes.
 */
public final class Constants {
    public static final String PRODUCT_NAME = "JDBCX";

    public static final Object[] EMPTY_OBJECT_ARRAY = new Object[0];
    public static final Object[][] EMPTY_OBJECT_ARRAY2 = new Object[0][];
    public static final boolean[] EMPTY_BOOL_ARRAY = new boolean[0];
    public static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
    public static final short[] EMPTY_SHORT_ARRAY = new short[0];
    public static final int[] EMPTY_INT_ARRAY = new int[0];
    public static final long[] EMPTY_LONG_ARRAY = new long[0];
    public static final float[] EMPTY_FLOAT_ARRAY = new float[0];
    public static final double[] EMPTY_DOUBLE_ARRAY = new double[0];
    public static final String[] EMPTY_STRING_ARRAY = new String[0];
    public static final String[][] EMPTY_STRING_ARRAY2 = new String[0][];

    // public static final Value[] EMPTY_VALUES = new Value[0];

    public static final String EMPTY_STRING = "";

    public static final String LOCAL_HOST_IPV4 = "127.0.0.1";
    public static final String LOCAL_HOST_IPV6 = "::1";
    public static final String LOCAL_HOST_NAME = "localhost";

    public static final String JSON_PROP_ID = "\"id\":";
    public static final String JSON_PROP_ALIASES = "\"aliases\":";
    public static final String JSON_PROP_NAME = "\"name\":";
    public static final String JSON_PROP_DESC = "\"description\":";
    public static final String JSON_PROP_TABLE = "\"table\":";
    public static final String JSON_PROP_TYPE = "\"type\":";

    public static final String PROP_COMPRESSION = "compression";
    public static final String PROP_FORMAT = "format";

    public static final TimeZone UTC_TIMEZONE = TimeZone.getTimeZone("UTC");
    public static final TimeZone SYS_TIMEZONE = TimeZone.getDefault();
    public static final ZoneId UTC_ZONE = UTC_TIMEZONE.toZoneId();
    public static final ZoneId SYS_ZONE = SYS_TIMEZONE.toZoneId();

    public static final LocalDate DATE_ZERO = LocalDate.ofEpochDay(0L);
    public static final LocalDateTime DATETIME_ZERO = LocalDateTime.ofEpochSecond(0L, 0, ZoneOffset.UTC);
    public static final LocalTime TIME_ZERO = LocalTime.ofSecondOfDay(0L);

    public static final String FALSE_EXPR = Boolean.FALSE.toString();
    public static final String NO_EXPR = "no";
    public static final String ZERO_EXPR = "0";
    public static final String TRUE_EXPR = Boolean.TRUE.toString();
    public static final String YES_EXPR = "yes";
    public static final String ONE_EXPR = "1";

    public static final String EMPTY_EXPR = "''";
    public static final String NULL_EXPR = "NULL";
    public static final String NAN_EXPR = "NaN";
    public static final String INF_EXPR = "Inf";
    public static final String NINF_EXPR = "-Inf";

    public static final String NULL_STR = "null";

    public static final String PROTOCOL_DELIMITER = "://";

    public static final int MIN_TIME_SCALE = 0;
    public static final int MAX_TIME_SCALE = 9;

    public static final String RE_META_CHARS = "<([{\\^-=$!|]})?*+.>";

    public static final String SCOPE_GLOBAL = "global";
    public static final String SCOPE_THREAD = "thread";
    public static final String SCOPE_QUERY = "query";

    public static final int DEFAULT_BUFFER_SIZE = 2048;
    public static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;

    public static final DateTimeFormatter DEFAULT_DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    public static final DateTimeFormatter DEFAULT_TIME_FORMATTER = new DateTimeFormatterBuilder()
            .appendPattern("HH:mm:ss").optionalStart()
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
            .optionalEnd().toFormatter();
    public static final DateTimeFormatter DEFAULT_TIMESTAMP_FORMATTER = new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd HH:mm:ss").optionalStart()
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
            .optionalEnd().toFormatter();

    public static final int MIN_CORE_THREADS = 4;

    public static final int JAVA_MAJOR_VERSION;

    public static final boolean IS_UNIX;
    public static final boolean IS_WINDOWS;

    public static final String CONF_DIR;
    public static final String CURRENT_DIR;
    public static final String HOME_DIR;
    public static final String TMP_DIR;

    public static final String FILE_ENCODING;

    private static final InputStream EMPTY_INPUT = new ByteArrayInputStream(EMPTY_BYTE_ARRAY);

    public static final InputStream nullInputStream() {
        return EMPTY_INPUT;
    }

    public static final Reader nullReader() {
        return new StringReader(Constants.EMPTY_STRING);
    }

    static {
        final String javaVersion = System.getProperty("java.version");
        if (javaVersion != null && !javaVersion.isEmpty()) {
            int version = -1;
            try {
                if (javaVersion.startsWith("1.")) { // Java 8 and earlier
                    int index = javaVersion.indexOf('.', 2);
                    if (index != -1) {
                        version = Integer.parseInt(javaVersion.substring(2, index));
                    }
                } else { // Java 9 and later
                    int index = javaVersion.indexOf('.');
                    if (index > 0) {
                        version = Integer.parseInt(javaVersion.substring(0, index));
                    }
                }
            } catch (NumberFormatException e) {
                // Could not parse Java version string
            }
            JAVA_MAJOR_VERSION = version;
        } else {
            JAVA_MAJOR_VERSION = -1;
        }

        final String osName = System.getProperty("os.name", "");

        // https://github.com/apache/commons-lang/blob/5a3904c8678574a4ddb8502ebbc606be1091fb3f/src/main/java/org/apache/commons/lang3/SystemUtils.java#L1370
        IS_UNIX = osName.startsWith("AIX") || osName.startsWith("HP-UX") || osName.startsWith("OS/400")
                || osName.startsWith("Irix") || osName.startsWith("Linux") || osName.startsWith("LINUX")
                || osName.startsWith("Mac OS X") || osName.startsWith("Solaris") || osName.startsWith("SunOS")
                || osName.startsWith("FreeBSD") || osName.startsWith("OpenBSD") || osName.startsWith("NetBSD");
        IS_WINDOWS = osName.toLowerCase(Locale.ROOT).contains("windows");

        CURRENT_DIR = System.getProperty("user.dir");
        HOME_DIR = System.getProperty("user.home");
        TMP_DIR = System.getProperty("java.io.tmpdir");
        CONF_DIR = new StringBuilder(Constants.HOME_DIR).append(File.separatorChar).append(".jdbcx").toString();

        FILE_ENCODING = System.getProperty("file.encoding", DEFAULT_CHARSET.name());
    }

    private Constants() {
    }
}
