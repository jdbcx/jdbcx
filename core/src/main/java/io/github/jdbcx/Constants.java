/*
 * Copyright 2022-2023, Zhichun Wu
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

import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Locale;

/**
 * This class contains shared constants that are accessible by all classes.
 */
public final class Constants {
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

    public static final String NULL_EXPR = "NULL";
    public static final String NAN_EXPR = "NaN";
    public static final String INF_EXPR = "Inf";
    public static final String NINF_EXPR = "-Inf";

    public static final boolean IS_UNIX;
    public static final boolean IS_WINDOWS;

    public static final String HOME_DIR;
    public static final String CONF_DIR;
    public static final String FILE_ENCODING;
    public static final String FILE_SEPARATOR;

    static {
        final String osName = System.getProperty("os.name", "");

        // https://github.com/apache/commons-lang/blob/5a3904c8678574a4ddb8502ebbc606be1091fb3f/src/main/java/org/apache/commons/lang3/SystemUtils.java#L1370
        IS_UNIX = osName.startsWith("AIX") || osName.startsWith("HP-UX") || osName.startsWith("OS/400")
                || osName.startsWith("Irix") || osName.startsWith("Linux") || osName.startsWith("LINUX")
                || osName.startsWith("Mac OS X") || osName.startsWith("Solaris") || osName.startsWith("SunOS")
                || osName.startsWith("FreeBSD") || osName.startsWith("OpenBSD") || osName.startsWith("NetBSD");
        IS_WINDOWS = osName.toLowerCase(Locale.ROOT).contains("windows");

        HOME_DIR = System.getProperty("user.home");
        CONF_DIR = IS_WINDOWS
                ? Paths.get(System.getenv("APPDATA"), "jdbcx").toFile().getAbsolutePath()
                : Paths.get(HOME_DIR, ".jdbcx").toFile().getAbsolutePath();

        FILE_ENCODING = System.getProperty("file.encoding", StandardCharsets.UTF_8.name());
        FILE_SEPARATOR = System.getProperty("file.separator");
    }

    private Constants() {
    }
}
