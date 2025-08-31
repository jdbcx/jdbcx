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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Constructor;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This class is a utility class that provides a collection of useful static
 * methods.
 */
public final class Utils {
    private static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();

    static final String PATTERN_GLOB = "glob:";
    static final String PATTERN_REGEX = "regex:";

    private static <T> T findFirstService(Class<? extends T> serviceInterface, boolean preferCustomImpl) {
        Checker.nonNull(serviceInterface, Class.class);

        String className = Utils.class.getName();
        String packageName = className.substring(0, className.lastIndexOf('.') + 1);

        T defaultImpl = null;
        T service = null;

        for (T s : ServiceLoader.load(serviceInterface, Utils.class.getClassLoader())) {
            if (preferCustomImpl) {
                if (s.getClass().getName().startsWith(packageName)) {
                    defaultImpl = s;
                } else if (service == null) {
                    service = s;
                }
            } else {
                service = s;
                break;
            }
        }

        return service != null ? service : defaultImpl;
    }

    public static Path getPath(String path, boolean normalize) {
        if (path.startsWith("~/")) {
            return Paths.get(Constants.HOME_DIR, path.substring(2)).normalize();
        }

        return normalize ? Paths.get(path).toAbsolutePath().normalize() : Paths.get(path);
    }

    public static <T> ServiceLoader<T> load(Class<T> serviceClass, ClassLoader classLoader) {
        if (serviceClass == null || classLoader == null) {
            throw new IllegalArgumentException("Non-null service class and class loader are required");
        }

        // Why? Because the given class loader may have a different version of the
        // serviceClass, which will later cause ServiceConfigurationError
        try {
            final ServiceLoader<T> loader = ServiceLoader.load(serviceClass, classLoader);
            Iterator<T> it = loader.iterator();
            if (it.hasNext() && it.next() != null) { // potential ServiceConfigurationError
                loader.reload(); // slow but safe
                return loader;
            }
        } catch (Throwable e) { // NOSONAR
            // ignore
        }

        return ServiceLoader.load(serviceClass, serviceClass.getClassLoader());
    }

    public static String normalizePath(String path) {
        if (path == null || path.isEmpty()) {
            return Constants.EMPTY_STRING;
        } else if (path.startsWith("~/")) {
            String p = path.substring(2);
            if (p.isEmpty()) {
                return Constants.HOME_DIR;
            } else {
                return new StringBuilder(Constants.HOME_DIR).append(File.separatorChar).append(p).toString();
            }
        }
        return path;
    }

    public static List<Path> findFiles(String pathOrPattern, String fileExt) throws IOException {
        if (Checker.isNullOrEmpty(pathOrPattern)) {
            return Collections.emptyList();
        }

        final String normalizedFileExt = fileExt != null ? fileExt.trim() : Constants.EMPTY_STRING;

        final int starPos = pathOrPattern.indexOf('*');
        final int qmarkPos = pathOrPattern.indexOf('?');
        List<Path> list = new LinkedList<>();
        if (starPos != -1 || qmarkPos != -1) { // glob
            int len = pathOrPattern.length();
            int endIndex = Math.min(starPos != -1 ? starPos : len, qmarkPos != -1 ? qmarkPos : len);
            String str = pathOrPattern.substring(0, endIndex);
            int index = str.lastIndexOf(File.separatorChar);
            if (File.separatorChar != '/') {
                index = Math.max(index, str.lastIndexOf('/'));
            }

            final Path dir;
            String pattern;
            if (index != -1) {
                dir = getPath(pathOrPattern.substring(0, index), true);
                pattern = new StringBuilder(dir.toString()).append(File.separatorChar)
                        .append(pathOrPattern.substring(index + 1)).toString();
            } else {
                dir = Paths.get(Constants.CURRENT_DIR);
                pattern = new StringBuilder(dir.toString()).append(File.separatorChar).append(pathOrPattern).toString();
            }
            if (!pattern.startsWith(PATTERN_GLOB) && !pattern.startsWith(PATTERN_REGEX)) {
                pattern = PATTERN_GLOB + pattern.replace("\\", "\\\\");
            }

            final PathMatcher matcher = FileSystems.getDefault().getPathMatcher(pattern);
            try (Stream<Path> s = Files.list(dir)) {
                list.addAll(s.filter(p -> matcher.matches(p) && Files.isRegularFile(p)).sorted()
                        .collect(Collectors.toList()));
            }
        } else {
            Path path = getPath(pathOrPattern, true);
            if (Files.exists(path)) {
                if (Files.isDirectory(path)) {
                    try (Stream<Path> s = Files.list(path)) {
                        list.addAll(s.filter(p -> Files.isRegularFile(p)
                                && (normalizedFileExt.isEmpty() || p.toString().endsWith(normalizedFileExt))).sorted()
                                .collect(Collectors.toList()));
                    }
                } else {
                    list.add(path);
                }
            }
        }

        return list.isEmpty() ? Collections.emptyList() : Collections.unmodifiableList(new ArrayList<>(list));
    }

    public static String applyVariables(String template, VariableTag tag, UnaryOperator<String> applyFunc) {
        return applyVariables(template, tag, applyFunc, false);
    }

    static String applyVariables(String template, VariableTag tag, UnaryOperator<String> applyFunc,
            boolean applyDefaultValue) {
        if (template == null || template.isEmpty()) {
            return Constants.EMPTY_STRING;
        } else if (tag == null) {
            tag = VariableTag.BRACE;
        }
        if (applyFunc == null || template.indexOf(tag.variableLeft()) == -1) {
            return template;
        }

        final char leftChar = tag.leftChar();
        final char rightChar = tag.rightChar();
        final char varChar = tag.variableChar();
        final char escapeChar = tag.escapeChar();

        final int len = template.length();
        StringBuilder builder = new StringBuilder(len);
        StringBuilder sb = new StringBuilder();
        boolean escaped = false;
        int colonIndex = -1;
        int startIndex = -1;
        for (int i = 0; i < len; i++) {
            char ch = template.charAt(i);
            if (startIndex == -1) {
                if (escaped) {
                    builder.append(ch);
                    escaped = false;
                } else if (ch == escapeChar) {
                    builder.append(ch);
                    escaped = true;
                } else if (ch == varChar && i + 2 < len && template.charAt(i + 1) == leftChar) {
                    startIndex = i++;
                } else {
                    builder.append(ch);
                }
            } else {
                if (escaped) {
                    sb.append(ch);
                    escaped = false;
                } else if (ch == escapeChar) {
                    escaped = true;
                } else if (applyDefaultValue && ch == ':') { // key:defaultValue
                    colonIndex = sb.length();
                } else if (ch == rightChar) {
                    final String key;
                    final String defaultValue;
                    if (colonIndex >= 0) {
                        key = sb.substring(0, colonIndex);
                        defaultValue = sb.substring(colonIndex);
                        colonIndex = -1;
                    } else {
                        key = sb.toString();
                        defaultValue = template.substring(startIndex, i + 1);
                    }
                    sb.setLength(0);

                    String value = applyFunc.apply(key);
                    if (value == null) {
                        builder.append(defaultValue);
                    } else {
                        builder.append(value); // recursive? not going to make escaping tedious
                    }
                    startIndex = -1;
                } else {
                    sb.append(ch);
                }
            }
        }

        if (startIndex != -1) {
            builder.append(template.substring(startIndex));
        }
        return builder.toString();
    }

    public static String applyVariables(CharSequence template, VariableTag tag, Map<String, String> variables) {
        if (template == null || template.length() == 0) {
            return Constants.EMPTY_STRING;
        }
        return applyVariables(template.toString(), tag,
                variables == null || variables.isEmpty() ? null : variables::get);
    }

    public static String applyVariables(String template, VariableTag tag, Map<String, String> variables) {
        return applyVariables(template, tag, variables == null || variables.isEmpty() ? null : variables::get);
    }

    public static String applyVariables(CharSequence template, VariableTag tag, Properties variables) {
        if (template == null || template.length() == 0) {
            return Constants.EMPTY_STRING;
        }
        return applyVariables(template.toString(), tag, variables == null ? null : variables::getProperty);
    }

    public static String applyVariables(String template, VariableTag tag, Properties variables) {
        return applyVariables(template, tag, variables == null ? null : variables::getProperty, false);
    }

    public static String applyVariablesWithDefault(String template, VariableTag tag, Properties variables) {
        return applyVariables(template, tag, variables == null ? null : variables::getProperty, true);
    }

    public static int getMapInitialCapacity(int capacity) {
        int guess = 1;
        for (int i = 0; i < 11; i++) { // up to 2048
            guess <<= 1;
            if (guess >= capacity) {
                break;
            }
        }
        return guess;
    }

    /**
     * Converts given string to key value pairs. Same as
     * {@code toKeyValuePairs(str, ',', true)}.
     * 
     * @param str string
     * @return non-null key value pairs
     */
    public static Map<String, String> toKeyValuePairs(String str) {
        return toKeyValuePairs(str, ',', false);
    }

    /**
     * Converts given string to key value paris.
     *
     * @param str          string
     * @param delimiter    delimiter between key value pairs
     * @param isUrlEncoded whether the key and value are URL encoded or not
     * @return non-null key value pairs
     */
    public static Map<String, String> toKeyValuePairs(String str, char delimiter, boolean isUrlEncoded) {
        if (str == null || str.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, String> map = new LinkedHashMap<>();
        String key = null;
        StringBuilder builder = new StringBuilder();
        for (int i = 0, len = str.length(); i < len; i++) {
            char ch = str.charAt(i);
            if (!isUrlEncoded && ch == '\\' && i + 1 < len) {
                ch = str.charAt(++i);
                builder.append(ch);
                continue;
            }

            if (Character.isWhitespace(ch)) {
                if (builder.length() > 0) {
                    builder.append(ch);
                }
            } else if (ch == '=' && key == null) {
                key = builder.toString().trim();
                if (isUrlEncoded) {
                    key = decode(key);
                }
                builder.setLength(0);
            } else if (ch == delimiter && key != null) {
                String value = builder.toString().trim();
                if (isUrlEncoded) {
                    value = decode(value);
                }
                builder.setLength(0);
                if (!key.isEmpty() && !value.isEmpty()) {
                    map.put(key, value);
                }
                key = null;
            } else {
                builder.append(ch);
            }
        }

        if (key != null && builder.length() > 0) {
            String value = builder.toString().trim();
            if (!key.isEmpty() && !value.isEmpty()) {
                if (isUrlEncoded) {
                    key = decode(key);
                    value = decode(value);
                }
                map.put(key, value);
            }
        }

        return Collections.unmodifiableMap(map);
    }

    /**
     * Converts given map to key value pairs in string format.
     *
     * @param kvps map
     * @return non-null key value pairs in string format
     */
    public static String toKeyValuePairs(Map<String, String> kvps) {
        return toKeyValuePairs(kvps, ',', false);
    }

    /**
     * Converts given map to key value pairs in string format.
     *
     * @param kvps             map
     * @param delimiter        delimiter between key value pairs
     * @param requireUrlEncode whether the key and value are URL encoded or not
     * @return non-null key value pairs in string format
     */
    public static String toKeyValuePairs(Map<String, String> kvps, char delimiter, boolean requireUrlEncode) {
        if (kvps == null || kvps.isEmpty()) {
            return Constants.EMPTY_STRING;
        }

        StringBuilder builder = new StringBuilder(kvps.size() * 20);
        if (requireUrlEncode) {
            for (Entry<String, String> entry : kvps.entrySet()) {
                builder.append(encode(entry.getKey())).append('=').append(encode(entry.getValue())).append(delimiter);
            }
        } else {
            for (Entry<String, String> entry : kvps.entrySet()) {
                builder.append(entry.getKey()).append('=').append(entry.getValue()).append(delimiter);
            }
        }
        if (builder.length() > 0) {
            builder.deleteCharAt(builder.length() - 1);
        }
        return builder.toString();
    }

    /**
     * Creates instance according to given options.
     *
     * @param <T>            type of the class
     * @param preferredClass preferred class for instantiation
     * @param defaultClass   default class for instantiation
     * @param autoDetect     whether to detect preferred class
     * @return non-null instance
     */
    public static <T> T createInstance(Class<? extends T> preferredClass, Class<? extends T> defaultClass,
            boolean autoDetect) {
        T instance = null;
        if (autoDetect) {
            try {
                instance = preferredClass.getDeclaredConstructor().newInstance();
            } catch (Throwable t) { // NOSONAR
                // ignore
            }
        }

        if (instance == null) {
            try {
                instance = defaultClass.getDeclaredConstructor().newInstance();
            } catch (Throwable e) { // NOSONAR
                throw new UnsupportedOperationException("Failed to create default instance of " + defaultClass, e);
            }
        }
        return instance;
    }

    public static <T, I extends T> T createInstance(Class<T> interfaceClass, String className, I defaultInstance) {
        if (interfaceClass == null || defaultInstance == null) {
            throw new IllegalArgumentException("Non-null interface class and default instance are required");
        } else if (!Checker.isNullOrEmpty(className)) {
            final String fullQualifiedClassName = className.indexOf('.') != -1 ? className
                    : new StringBuilder(defaultInstance.getClass().getPackage().getName())
                            .append('.').append(className).toString();

            try {
                ClassLoader loader = Thread.currentThread().getContextClassLoader();
                if (loader == null) {
                    loader = defaultInstance.getClass().getClassLoader();
                }
                Class<?> clazz = loader.loadClass(fullQualifiedClassName);
                return interfaceClass.cast(clazz.getConstructor().newInstance());
            } catch (Exception e) {
                // ignore
            }
        }

        return defaultInstance;
    }

    public static Class<?> toPrimitiveType(Class<?> clazz) {
        if (Checker.nonNull(clazz, Class.class).isPrimitive()) {
            return clazz;
        }

        final Class<?> primitiveClass;
        if (Boolean.class.equals(clazz)) {
            primitiveClass = boolean.class;
        } else if (Character.class.equals(clazz)) {
            primitiveClass = char.class;
        } else if (Byte.class.equals(clazz)) {
            primitiveClass = byte.class;
        } else if (Short.class.equals(clazz)) {
            primitiveClass = short.class;
        } else if (Integer.class.equals(clazz)) {
            primitiveClass = int.class;
        } else if (Long.class.equals(clazz)) {
            primitiveClass = long.class;
        } else if (Float.class.equals(clazz)) {
            primitiveClass = float.class;
        } else if (Double.class.equals(clazz)) {
            primitiveClass = double.class;
        } else {
            primitiveClass = clazz;
        }
        return primitiveClass;
    }

    public static <T> T newInstance(Class<T> clazz, String className, Object... args) {
        final int len;
        final Class<?>[] argClasses;
        final Class<?>[] altArgClasses;
        if (clazz == null || Checker.isNullOrEmpty(className) || args == null) {
            throw new IllegalArgumentException("Non-null arguments are required");
        } else {
            len = args.length;
            argClasses = new Class[len];
            Class<?>[] alternatives = new Class[len];
            boolean hasPrimitiveType = false;
            for (int i = 0; i < len; i++) {
                Object arg = args[i];
                if (arg == null) {
                    throw new IllegalArgumentException("Non-null arguments are required");
                }
                Class<?> c = arg.getClass();
                argClasses[i] = c;
                alternatives[i] = toPrimitiveType(c);
                if (c != alternatives[i]) {
                    hasPrimitiveType = true;
                }
            }
            altArgClasses = hasPrimitiveType ? alternatives : argClasses;
        }

        try {
            Class<?> c = Class.forName(className);
            Constructor<?> constructor = null;
            try { // NOSONAR
                constructor = c.getConstructor(argClasses);
            } catch (NoSuchMethodException e) {
                if (altArgClasses == argClasses) {
                    throw e;
                }
                constructor = c.getConstructor(altArgClasses);
            }
            return clazz.cast(constructor.newInstance(args));
        } catch (Throwable t) { // NOSONAR
            throw new IllegalArgumentException("Failed to create instance for " + className, t);
        }
    }

    public static String capitalize(String str) {
        if (str == null) {
            return Constants.EMPTY_STRING;
        } else if (str.isEmpty()) {
            return str;
        }
        return Character.toUpperCase(str.charAt(0)) + str.substring(1).toLowerCase(Locale.ROOT);
    }

    /**
     * Creates a temporary file. Same as {@code createTempFile(null, null, true)}.
     *
     * @return non-null temporary file
     * @throws IOException when failed to create the temporary file
     */
    public static File createTempFile() throws IOException {
        return createTempFile(null, null, null, true);
    }

    /**
     * Creates a temporary file with given prefix and suffix. Same as
     * {@code createTempFile(prefix, suffix, true)}.
     *
     * @param prefix prefix, could be {@code null}
     * @param suffix suffix, could be {@code null}
     * @return non-null temporary file
     * @throws IOException when failed to create the temporary file
     */
    public static File createTempFile(String prefix, String suffix) throws IOException {
        return createTempFile(null, prefix, suffix, true);
    }

    /**
     * Creates a temporary file with the given prefix and suffix. The file has only
     * read and write access granted to the owner.
     *
     * @param dir          directory to create the temp file
     * @param prefix       prefix, null or empty string is taken as {@code "tmp"}
     * @param suffix       suffix, null or empty string is taken as {@code ".data"}
     * @param deleteOnExit whether the file be deleted on exit
     * @return non-null temporary file
     * @throws IOException when failed to create the temporary file
     */
    public static File createTempFile(Path dir, String prefix, String suffix, boolean deleteOnExit) throws IOException {
        if (dir == null) {
            dir = Paths.get(Constants.TMP_DIR);
        }
        if (prefix == null || prefix.isEmpty()) {
            prefix = "tmp";
        }
        if (suffix == null || suffix.isEmpty()) {
            suffix = ".data";
        }

        final File f;
        if (Constants.IS_UNIX) {
            FileAttribute<Set<PosixFilePermission>> attr = PosixFilePermissions
                    .asFileAttribute(PosixFilePermissions.fromString("rw-------"));
            f = Files.createTempFile(dir, prefix, suffix, attr).toFile();
        } else {
            f = Files.createTempFile(dir, prefix, suffix).toFile(); // NOSONAR
            f.setReadable(true, true); // NOSONAR
            f.setWritable(true, true); // NOSONAR
            f.setExecutable(false, false); // NOSONAR
        }

        if (deleteOnExit) {
            f.deleteOnExit();
        }
        return f;
    }

    /**
     * Decode given string using {@link URLDecoder} and
     * {@link Constants#DEFAULT_CHARSET}.
     *
     * @param encodedString encoded string
     * @return non-null decoded string
     */
    public static String decode(String encodedString) {
        if (Checker.isNullOrEmpty(encodedString)) {
            return Constants.EMPTY_STRING;
        }

        try {
            return URLDecoder.decode(encodedString, Constants.DEFAULT_CHARSET.name());
        } catch (UnsupportedEncodingException e) {
            return encodedString;
        }
    }

    /**
     * Encode given string using {@link URLEncoder} and
     * {@link Constants#DEFAULT_CHARSET}.
     *
     * @param str string to encode
     * @return non-null encoded string
     */
    public static String encode(String str) {
        if (Checker.isNullOrEmpty(str)) {
            return "";
        }

        try {
            return URLEncoder.encode(str, Constants.DEFAULT_CHARSET.name());
        } catch (UnsupportedEncodingException e) {
            return str;
        }
    }

    /**
     * Gets absolute and normalized path to the given file.
     *
     * @param file non-empty file
     * @return non-null absolute and normalized path to the file
     */
    public static Path getFile(String file) {
        if (file == null || file.isEmpty()) {
            throw new IllegalArgumentException("Non-empty file is required");
        }

        return getPath(file, true);
    }

    public static String getHost(String defaultHost) {
        boolean isLocal = false;
        if (Checker.isNullOrEmpty(defaultHost)) {
            defaultHost = Constants.LOCAL_HOST_IPV4;
            isLocal = true;
        } else {
            try {
                InetAddress address = InetAddress.getByName(defaultHost);
                isLocal = address.isLoopbackAddress();
            } catch (UnknownHostException e) {
                defaultHost = Constants.LOCAL_HOST_IPV4;
                isLocal = true;
            }
        }

        String host = defaultHost;
        try {
            InetAddress localHost = InetAddress.getLocalHost();
            host = localHost.getHostAddress();
            for (InetAddress addr : InetAddress.getAllByName(localHost.getHostName())) {
                if (addr.isSiteLocalAddress()) { // RFC 1918
                    host = addr.getHostAddress();
                    break;
                }
            }
        } catch (UnknownHostException e) { // should never happen
            throw new ExceptionInInitializerError(e);
        }

        if (!isLocal && (Constants.LOCAL_HOST_IPV4.equals(host) || Constants.LOCAL_HOST_IPV6.equals(host))) {
            host = defaultHost;
        }
        return host;
    }

    public static boolean containsJdbcWildcard(CharSequence chars) {
        if (chars != null) {
            for (int i = 0, len = chars.length(); i < len; i++) {
                char ch = chars.charAt(i);
                if (ch == '\\') { // escaped
                    i++;
                } else if (ch == '%' || ch == '_') {
                    return true;
                }
            }
        }

        return false;
    }

    public static String jdbcNamePatternToRe(CharSequence chars) {
        if (chars == null) {
            return Constants.EMPTY_STRING;
        }

        int len = chars.length();
        StringBuilder builder = new StringBuilder(len);
        for (int i = 0; i < len; i++) {
            char ch = chars.charAt(i);
            if (ch == '\\') {
                if (++i < len) {
                    ch = chars.charAt(i);
                }
            } else if (ch == '%') {
                builder.append(".*");
                continue;
            } else if (ch == '_') {
                builder.append('.');
                continue;
            }

            if (Constants.RE_META_CHARS.indexOf(ch) != -1) {
                builder.append('\\');
            }
            builder.append(ch);
        }
        return builder.toString();
    }

    public static boolean isCloseBracket(char ch) {
        return ch == ')' || ch == ']' || ch == '}';
    }

    public static boolean isOpenBracket(char ch) {
        return ch == '(' || ch == '[' || ch == '{';
    }

    public static boolean isQuote(char ch) {
        return ch == '\'' || ch == '`' || ch == '"';
    }

    public static boolean isSeparator(char ch) {
        return ch == ',' || ch == ';';
    }

    public static String escape(String str, char target) {
        return escape(str, target, '\\');
    }

    /**
     * Escape quotes in given string.
     * 
     * @param str    string
     * @param target quote to escape
     * @param escape escaping character
     * @return escaped string
     */
    public static String escape(String str, char target, char escape) {
        if (str == null) {
            return str;
        }

        int len = str.length();
        StringBuilder sb = new StringBuilder(len + 10);

        for (int i = 0; i < len; i++) {
            char ch = str.charAt(i);
            if (ch == target || ch == escape) {
                sb.append(escape);
            }
            sb.append(ch);
        }

        return sb.toString();
    }

    /**
     * Unescape quoted string.
     * 
     * @param str quoted string
     * @return unescaped string
     */
    public static String unescape(String str) {
        if (Checker.isNullOrEmpty(str)) {
            return str;
        }

        int len = str.length();
        char quote = str.charAt(0);
        if (!isQuote(quote) || quote != str.charAt(len - 1)) { // not a quoted string
            return str;
        }

        StringBuilder sb = new StringBuilder(len = len - 1);
        for (int i = 1; i < len; i++) {
            char ch = str.charAt(i);

            if (++i >= len) {
                sb.append(ch);
            } else {
                char nextChar = str.charAt(i);
                if (ch == '\\' || (ch == quote && nextChar == quote)) {
                    sb.append(nextChar);
                } else {
                    sb.append(ch);
                    i--;
                }
            }
        }

        return sb.toString();
    }

    /**
     * Wrapper of {@code String.format(Locale.ROOT, ...)}.
     *
     * @param template string to format
     * @param args     arguments used in substitution
     * @return formatted string
     */
    public static String format(String template, Object... args) {
        return String.format(Locale.ROOT, template, args);
    }

    /**
     * Normalizes given directory by appending back slash if it does exist.
     *
     * @param dir original directory
     * @return normalized directory
     */
    public static String normalizeDirectory(String dir) {
        if (dir == null || dir.isEmpty()) {
            return "./";
        }

        dir = getPath(dir, false).toFile().getAbsolutePath();
        return dir.charAt(dir.length() - 1) == '/' ? dir : dir.concat("/");
    }

    public static char getCloseBracket(char openBracket) {
        char closeBracket;
        if (openBracket == '(') {
            closeBracket = ')';
        } else if (openBracket == '[') {
            closeBracket = ']';
        } else if (openBracket == '{') {
            closeBracket = '}';
        } else {
            throw new IllegalArgumentException("Unsupported bracket: " + openBracket);
        }

        return closeBracket;
    }

    public static <T> T getService(Class<? extends T> serviceInterface) {
        return getService(serviceInterface, null);
    }

    /**
     * Load service according to given interface using
     * {@link java.util.ServiceLoader}, fallback to given default service or
     * supplier function if not found.
     *
     * @param <T>              type of service
     * @param serviceInterface non-null service interface
     * @param defaultService   optionally default service
     * @return non-null service
     */
    public static <T> T getService(Class<? extends T> serviceInterface, T defaultService) {
        T service = defaultService;
        Exception error = null;

        // load custom implementation if any
        try {
            T s = findFirstService(serviceInterface, defaultService == null);
            if (s != null) {
                service = s;
            }
        } catch (Exception t) {
            error = t;
        }

        if (service == null) {
            throw new IllegalStateException(String.format("Failed to get service %s", serviceInterface.getName()),
                    error);
        }

        return service;
    }

    public static <T> T getService(Class<? extends T> serviceInterface, Supplier<T> supplier) {
        T service = null;
        Exception error = null;

        // load custom implementation if any
        try {
            service = findFirstService(serviceInterface, supplier == null);
        } catch (Exception t) {
            error = t;
        }

        // and then try supplier function if no luck
        if (service == null && supplier != null) {
            try {
                service = supplier.get();
            } catch (Exception t) {
                // override the error
                error = t;
            }
        }

        if (service == null) {
            throw new IllegalStateException(String.format("Failed to get service %s", serviceInterface.getName()),
                    error);
        }

        return service;
    }

    /**
     * Search file in current directory, {@link Constants#CONF_DIR}, and then
     * classpath, and then get input stream to read the given file.
     *
     * @param file path to the file
     * @return input stream
     * @throws FileNotFoundException when the file does not exists
     */
    public static InputStream getFileInputStream(String file) throws FileNotFoundException {
        Path path = Paths.get(Checker.nonBlank(file, "file"));

        StringBuilder builder = new StringBuilder();
        InputStream in = null;
        if (Files.exists(path)) {
            builder.append(',').append(file);
            in = new FileInputStream(path.toFile());
        } else if (!path.isAbsolute()) {
            path = Paths.get(Constants.CONF_DIR, file);

            if (Files.exists(path)) {
                builder.append(',').append(path.toString());
                in = new FileInputStream(path.toFile());
            }
        }

        if (in == null) {
            builder.append(',').append("classpath:").append(file);
            in = Thread.currentThread().getContextClassLoader().getResourceAsStream(file);
        }

        if (in == null) {
            throw new FileNotFoundException(format("Could not open file from: %s", builder.deleteCharAt(0).toString()));
        }

        return in;
    }

    /**
     * Get output stream for writing a file. Directories and file will be created if
     * they do not exist.
     *
     * @param file path to the file
     * @return output stream
     * @throws IOException when failed to create directories and/or file
     */
    public static OutputStream getFileOutputStream(String file) throws IOException {
        Path path = Paths.get(Checker.nonBlank(file, "file"));

        if (Files.notExists(path)) {
            Files.createDirectories(path.getParent());
            Files.createFile(path);
        }

        return new FileOutputStream(file, false);
    }

    public static String getProperty(String key, Properties... props) {
        return getProperty(key, null, props);
    }

    public static String getProperty(String key, String defaultValue, Properties... props) {
        String value = null;

        if (props != null) {
            for (Properties p : props) {
                value = p.getProperty(key);
                if (value != null) {
                    break;
                }
            }
        }

        if (value == null) {
            value = System.getProperty(key);
        }

        return value == null ? defaultValue : value;
    }

    /**
     * Finds the index of the specified characters within the provided
     * {@link CharSequence}.
     *
     * @param args       CharSequence to scan
     * @param startIndex start index
     * @param len        length of the CharSequence
     * @param chars      characters to find
     * @return index of the specified characters; -1 indicates not found
     */
    public static int indexOf(CharSequence args, int startIndex, int len, char... chars) {
        if (chars != null) {
            int m = chars.length;
            for (int i = startIndex; i < len; i++) {
                boolean matched = true;
                for (int j = 0; j < m; j++) {
                    if (args.charAt(i + j) != chars[j]) {
                        matched = false;
                        break;
                    }
                }
                if (matched) {
                    return i;
                }
            }
        }
        return -1;
    }

    /**
     * Skips over single line SQL comment.
     *
     * @param args       non-null CharSequence to scan
     * @param startIndex start index, optionally start of the single line comment
     * @param len        end index, usually length of the given CharSequence
     * @return index of start of next line, right after {@code \n}
     */
    public static int skipSingleLineComment(CharSequence args, int startIndex, int len) {
        for (int i = startIndex; i < len; i++) {
            if (args.charAt(i) == '\n') {
                return i + 1;
            }
        }
        return len;
    }

    /**
     * Skips over a multi-line SQL comment that may or may not contain nested
     * comments.
     *
     * @param args       non-null CharSequence to scan
     * @param startIndex start index of the multi-line comment
     * @param len        end index, usually length of the given CharSequence
     * @return index next to end of the outter most multi-line comment
     * @throws IllegalArgumentException when multi-line comment is unclosed
     */
    public static int skipMultiLineComment(CharSequence args, int startIndex, int len) {
        int commentLevel = 0;

        for (int i = startIndex; i < len; i++) {
            char ch = args.charAt(i);
            boolean hasNext = i < len - 1;
            if (ch == '/' && hasNext && args.charAt(i + 1) == '*') {
                i++;
                commentLevel++;
            } else if (ch == '*' && hasNext && args.charAt(i + 1) == '/') {
                i++;
                if (--commentLevel == 0) {
                    return i + 1;
                }
            }
            if (commentLevel <= 0) {
                break;
            }
        }

        return -1;
    }

    public static List<String> split(String str, char delimiter) {
        return split(str, delimiter, false, false, false);
    }

    public static List<String> split(String str, char delimiter, boolean trim, boolean ignoreEmpty, boolean dedup) {
        if (str == null) {
            return Collections.emptyList();
        } else if (trim) {
            str = str.trim();
        }

        List<String> list = new LinkedList<>();
        StringBuilder builder = new StringBuilder();
        for (int i = 0, len = str.length(); i < len; i++) {
            char ch = str.charAt(i);
            if (ch == delimiter) {
                String v = trim ? builder.toString().trim() : builder.toString();
                builder.setLength(0);
                if ((ignoreEmpty && v.isEmpty()) || (dedup && list.contains(v))) {
                    continue;
                }
                list.add(v);
            } else {
                builder.append(ch);
            }
        }

        String v = trim ? builder.toString().trim() : builder.toString();
        if (list.isEmpty()) {
            return ignoreEmpty && v.isEmpty() ? Collections.emptyList() : Collections.singletonList(v);
        } else if (!(ignoreEmpty && v.isEmpty()) && !(dedup && list.contains(v))) {
            list.add(v);
        }
        return Collections.unmodifiableList(list);
    }

    public static List<String> split(String str, String delimiter) {
        if (str == null) {
            return Collections.emptyList();
        } else if (Checker.isNullOrEmpty(delimiter)) {
            return Collections.singletonList(str);
        }

        final int dlen = delimiter.length();
        final int len = str.length();
        if (dlen > len) {
            return Collections.singletonList(str);
        }

        List<String> list = new LinkedList<>();
        int cursor = 0;
        do {
            int index = str.indexOf(delimiter, cursor);
            if (index == -1) {
                list.add(str.substring(cursor));
                break;
            } else {
                list.add(str.substring(cursor, index));
                cursor = index + dlen;
            }
        } while (cursor < len);
        return Collections.unmodifiableList(list);
    }

    public static String[] splitUrl(String url) {
        final int len;
        if (url == null || (len = url.length()) == 0) {
            return new String[] { Constants.EMPTY_STRING, Constants.EMPTY_STRING };
        }

        int refIndex = url.indexOf(Constants.PROTOCOL_DELIMITER);
        int index = refIndex == -1 ? url.indexOf('/')
                : url.indexOf('/', refIndex + Constants.PROTOCOL_DELIMITER.length());
        return index > 0 && index < len ? new String[] { url.substring(0, index), url.substring(index) }
                : new String[] { url, Constants.EMPTY_STRING };
    }

    public static boolean startsWith(String str, String test, boolean ignoreCase) {
        if (str == null || test == null) {
            return false;
        } else if (!ignoreCase) {
            return str.startsWith(test);
        } else if (test.isEmpty()) {
            return true;
        }

        final int length = test.length();
        return str.length() >= length && str.substring(0, length).equalsIgnoreCase(test);
    }

    /**
     * Encapsulates the given array in an immutable list. Same as
     * {@code toImmutableList(type, values, true, true)}.
     *
     * @param <T>    type of the element
     * @param type   class
     * @param values array
     * @return non-null list
     */
    public static <T> List<T> toImmutableList(Class<T> type, T[] values) {
        return toImmutableList(type, values, true, true);
    }

    /**
     * Encapsulates the given array in an immutable list.
     *
     * @param <T>               type of the element
     * @param type              class
     * @param values            array
     * @param discardNulls      whether to discard null values in array
     * @param discardDuplicates whether to discard duplicated values in array
     * @return non-null list
     */
    public static <T> List<T> toImmutableList(Class<T> type, T[] values, boolean discardNulls,
            boolean discardDuplicates) {
        final int len;
        if (type == null || values == null || (len = values.length) == 0) {
            return Collections.emptyList();
        }

        boolean resize = false;
        List<T> list = new ArrayList<>(len);
        for (int i = 0; i < len; i++) {
            T v = values[i];
            if ((discardNulls && v == null) || (discardDuplicates && list.contains(v))) {
                resize = true;
                continue;
            }
            list.add(v);
        }
        return Collections.unmodifiableList(resize ? new ArrayList<>(list) : list);
    }

    public static long toEpochNanoSeconds(LocalDateTime value, ZoneOffset zoneOffset) {
        final long base = value.toEpochSecond(zoneOffset) * 1_000_000_000L;
        final int nanos = value.getNano();
        if (nanos == 0) {
            return base;
        } else if (base > 0) {
            return base + nanos;
        } else {
            return base - nanos;
        }
    }

    /**
     * Converts a String representation of a URL to a URL object.
     * 
     * @param url the String representation of the URL to be converted
     * @return A URL object representing the given string.
     * @throws MalformedURLException If the string cannot be parsed as a URI or URL
     */
    public static URL toURL(String url) throws MalformedURLException {
        if (Checker.isNullOrBlank(url)) {
            throw new MalformedURLException();
        }

        URI uri = null;
        if (url.indexOf(' ') == -1 && url.indexOf('\\') == -1) {
            try {
                uri = new URI(url);
            } catch (URISyntaxException e) {
                throw new MalformedURLException(e.getMessage());
            }

            if (uri.getScheme() == null || (Constants.IS_WINDOWS && uri.getScheme().length() == 1)) {
                uri = null;
            }
        }

        if (uri == null) {
            try {
                uri = Paths.get(url).normalize().toUri();
            } catch (InvalidPathException e) {
                throw new MalformedURLException(e.getMessage());
            }
        }
        return uri.toURL();
    }

    /**
     * Waits until the flag turns to {@code true} or timed out.
     *
     * @param flag    non-null boolean flag to check
     * @param timeout timeout, negative or zero means forever
     * @param unit    non-null time unit
     * @return true if the flag turns to true within given timeout; false otherwise
     * @throws InterruptedException when thread was interrupted
     */
    public static boolean waitFor(AtomicBoolean flag, long timeout, TimeUnit unit) throws InterruptedException {
        if (flag == null || unit == null) {
            throw new IllegalArgumentException("Non-null flag and time unit required");
        }

        final long timeoutMs = timeout > 0L ? unit.toMillis(timeout) : 0L;
        final long startTime = timeoutMs < 1L ? 0L : System.currentTimeMillis();
        while (!flag.get()) {
            if (Thread.interrupted()) {
                throw new InterruptedException();
            } else if (startTime > 0L && System.currentTimeMillis() - startTime >= timeoutMs) {
                return false;
            }
        }
        return true;
    }

    /**
     * Closes resources without throwing any exception.
     *
     * @param resource resource to close
     * @param more     optionally more resources to close
     */
    public static void closeQuietly(AutoCloseable resource, AutoCloseable... more) {
        final int len = more != null ? more.length : 0;

        int i = 0;
        do {
            if (resource != null) {
                try {
                    resource.close();
                } catch (Exception e) {
                    // ignore
                }
            }
            if (i < len) {
                resource = more[i];
            }
        } while (i++ < len);
    }

    public static void closeQuietly(Collection<? extends AutoCloseable> resources, boolean removeAll) {
        if (resources == null || resources.isEmpty()) {
            return;
        }

        final List<AutoCloseable> list = new ArrayList<>(resources);
        if (removeAll) {
            resources.clear();
        }
        for (AutoCloseable resource : list) {
            if (resource != null) {
                try {
                    resource.close();
                } catch (Exception e) {
                    // ignore
                }
            }
        }
    }

    /**
     * Gets affected rows from JDBC statement. It tries
     * {@link Statement#getLargeUpdateCount()} first, and then fall back to
     * {@link Statement#getUpdateCount()} if any error.
     *
     * @param stmt non-null JDBC statement
     * @return affected rows
     * @throws SQLException when failed to get affected rows
     */
    public static long getAffectedRows(Statement stmt) throws SQLException {
        long rows = 0L;
        try {
            rows = stmt.getLargeUpdateCount();
        } catch (Exception e) {
            rows = stmt.getUpdateCount();
        }
        return rows;
    }

    /**
     * Gets column label. It tries {@link ResultSetMetaData#getColumnLabel(int)}
     * first, and then fall back to {@link ResultSetMetaData#getColumnName(int)} if
     * any error.
     *
     * @param md          non-null ResultSet metadata
     * @param columnIndex column index greater than zero
     * @return non-null column label
     * @throws SQLException when failed to get column label
     */
    public static String getColumnLabel(ResultSetMetaData md, int columnIndex) throws SQLException {
        String label = md.getColumnLabel(columnIndex);
        if (label == null || label.isEmpty()) {
            label = md.getColumnName(columnIndex);
        }
        return label;
    }

    public static String removeTrailingChar(String str, char ch) {
        final int len;
        if (str == null || (len = str.length()) == 0) {
            return Constants.EMPTY_STRING;
        }
        return str.charAt(len - 1) == ch ? str.substring(0, len - 1) : str;
    }

    public static byte[] fromBase64(byte[] bytes) {
        return fromBase64(bytes, false);
    }

    public static byte[] fromBase64(byte[] bytes, boolean stopAtWhitespace) {
        if (bytes == null || bytes.length == 0) {
            return Constants.EMPTY_BYTE_ARRAY;
        }

        if (stopAtWhitespace) {
            for (int i = 0, l = bytes.length; i < l; i++) {
                if (Character.isWhitespace(bytes[i])) {
                    bytes = Arrays.copyOf(bytes, i);
                    break;
                }
            }
        }
        return Base64.getDecoder().decode(bytes);
    }

    public static byte[] fromBase64(String str) {
        return fromBase64(str, null, false);
    }

    public static byte[] fromBase64(String str, boolean stopAtWhitespace) {
        return fromBase64(str, null, stopAtWhitespace);
    }

    public static byte[] fromBase64(String str, Charset charset, boolean stopAtWhitespace) {
        if (str == null) {
            return Constants.EMPTY_BYTE_ARRAY;
        } else if (charset == null) {
            charset = Constants.DEFAULT_CHARSET;
        }
        byte[] bytes = str.getBytes(charset);
        if (stopAtWhitespace) {
            for (int i = 0, l = bytes.length; i < l; i++) {
                if (Character.isWhitespace(bytes[i])) {
                    bytes = Arrays.copyOf(bytes, i);
                    break;
                }
            }
        }
        return Base64.getDecoder().decode(bytes);
    }

    public static String toBase64(String str) {
        return toBase64(str, null);
    }

    public static String toBase64(byte[] bytes) {
        return toBase64(bytes, null);
    }

    public static String toBase64(String str, Charset charset) {
        if (str == null) {
            return Constants.EMPTY_STRING;
        } else if (charset == null) {
            charset = Constants.DEFAULT_CHARSET;
        }
        return toBase64(str.getBytes(charset), charset);
    }

    public static String toBase64(byte[] bytes, Charset charset) {
        if (bytes == null) {
            return Constants.EMPTY_STRING;
        } else if (charset == null) {
            charset = Constants.DEFAULT_CHARSET;
        }
        return new String(Base64.getEncoder().encode(bytes), charset);
    }

    public static String toHex(byte[] bytes) {
        final int len;
        if (bytes == null || (len = bytes.length) == 0) {
            return Constants.EMPTY_STRING;
        }

        char[] hexChars = new char[len * 2];
        for (int i = 0; i < len; i++) {
            int v = bytes[i] & 0xFF;
            int j = i * 2;
            hexChars[j++] = HEX_ARRAY[v >>> 4];
            hexChars[j] = HEX_ARRAY[v & 0x0F];
        }

        return new String(hexChars);
    }

    private Utils() {
    }
}
