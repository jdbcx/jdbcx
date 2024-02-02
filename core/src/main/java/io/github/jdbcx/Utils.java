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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.Arrays;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * This class is a utility class that provides a collection of useful static
 * methods.
 */
public final class Utils {
    private static <T> T findFirstService(Class<? extends T> serviceInterface, boolean preferCustomImpl) {
        Checker.nonNull(serviceInterface, "serviceInterface");

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

    public static String getVersion() {
        final String version;
        String str = Utils.class.getPackage().getImplementationVersion();
        if (Checker.isNullOrEmpty(str)) {
            version = Constants.EMPTY_STRING;
        } else {
            char[] chars = str.toCharArray();
            for (int i = 0, len = chars.length; i < len; i++) {
                if (Character.isDigit(chars[i])) {
                    str = str.substring(i);
                    break;
                }
            }
            version = str;
        }
        return version;
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
        } catch (Throwable e) {
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

    public static String applyVariables(String template, UnaryOperator<String> applyFunc) {
        if (template == null || template.isEmpty()) {
            return Constants.EMPTY_STRING;
        } else if (applyFunc == null || template.indexOf("${") == -1) {
            return template;
        }

        final int len = template.length();
        StringBuilder builder = new StringBuilder(len);
        StringBuilder sb = new StringBuilder();
        boolean escaped = false;
        int startIndex = -1;
        for (int i = 0; i < len; i++) {
            char ch = template.charAt(i);
            if (startIndex == -1) {
                if (escaped) {
                    builder.append(ch);
                    escaped = false;
                } else if (ch == '\\') {
                    builder.append(ch);
                    escaped = true;
                } else if (ch == '$' && i + 2 < len && template.charAt(i + 1) == '{') {
                    startIndex = i++;
                } else {
                    builder.append(ch);
                }
            } else {
                if (escaped) {
                    sb.append(ch);
                    escaped = false;
                } else if (ch == '\\') {
                    escaped = true;
                } else if (ch == '}') {
                    String key = sb.toString();
                    sb.setLength(0);

                    String value = applyFunc.apply(key);
                    if (value == null) {
                        builder.append(template.substring(startIndex, i + 1));
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

    public static String applyVariables(CharSequence template, Map<String, String> variables) {
        if (template == null || template.length() == 0) {
            return Constants.EMPTY_STRING;
        }
        return applyVariables(template.toString(), variables == null || variables.isEmpty() ? null : variables::get);
    }

    public static String applyVariables(String template, Map<String, String> variables) {
        return applyVariables(template, variables == null || variables.isEmpty() ? null : variables::get);
    }

    public static String applyVariables(CharSequence template, Properties variables) {
        if (template == null || template.length() == 0) {
            return Constants.EMPTY_STRING;
        }
        return applyVariables(template.toString(), variables == null ? null : variables::getProperty);
    }

    public static String applyVariables(String template, Properties variables) {
        return applyVariables(template, variables == null ? null : variables::getProperty);
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
        return toKeyValuePairs(str, ',', true);
    }

    /**
     * Converts given string to key value paris.
     *
     * @param str           string
     * @param delimiter     delimiter maong key value pairs
     * @param notUrlEncoded whether the key and value are URL encoded or not
     * @return non-null key value pairs
     */
    public static Map<String, String> toKeyValuePairs(String str, char delimiter, boolean notUrlEncoded) {
        if (str == null || str.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, String> map = new LinkedHashMap<>();
        String key = null;
        StringBuilder builder = new StringBuilder();
        for (int i = 0, len = str.length(); i < len; i++) {
            char ch = str.charAt(i);
            if (notUrlEncoded && ch == '\\' && i + 1 < len) {
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
                if (!notUrlEncoded) {
                    key = decode(key);
                }
                builder.setLength(0);
            } else if (ch == delimiter && key != null) {
                String value = builder.toString().trim();
                if (!notUrlEncoded) {
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
                map.put(key, value);
            }
        }

        return Collections.unmodifiableMap(map);
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
     * @param prefix prefix, could be null
     * @param suffix suffix, could be null
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

    /**
     * Finds files according to the given pattern and path.
     *
     * @param pattern non-empty pattern may or may have syntax prefix as in
     *                {@link java.nio.file.FileSystem#getPathMatcher(String)},
     *                defaults to {@code glob} syntax
     * @param paths   path to search, defaults to current work directory
     * @return non-null list of normalized absolute paths matching the pattern
     * @throws IOException when failed to find files
     */
    public static List<Path> findFiles(String pattern, String... paths) throws IOException {
        if (pattern == null || pattern.isEmpty()) {
            throw new IllegalArgumentException("Non-empty pattern is required");
        } else if (pattern.startsWith("~/")) {
            return Collections.singletonList(Paths.get(Constants.HOME_DIR, pattern.substring(2)).normalize());
        }

        if (!pattern.startsWith("glob:") && !pattern.startsWith("regex:")) {
            Path path = Paths.get(pattern);
            if (path.isAbsolute()) {
                return Collections.singletonList(path);
            } else {
                pattern = "glob:" + pattern;
            }
        }

        final Path searchPath;
        if (paths == null || paths.length == 0) {
            searchPath = Paths.get("");
        } else {
            String root = paths[0];
            Path rootPath = getPath(root, true);
            searchPath = paths.length < 2 ? rootPath
                    : Paths.get(rootPath.toFile().getAbsolutePath(), Arrays.copyOfRange(paths, 1, paths.length))
                            .normalize();
        }

        final List<Path> files = new ArrayList<>();
        final PathMatcher matcher = FileSystems.getDefault().getPathMatcher(pattern);
        Files.walkFileTree(searchPath, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) throws IOException {
                if (matcher.matches(path)) {
                    files.add(path.normalize());
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc)
                    throws IOException {
                return FileVisitResult.CONTINUE;
            }
        });
        return files;
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
        if (str == null) {
            return Collections.emptyList();
        }

        List<String> list = new LinkedList<>();
        StringBuilder builder = new StringBuilder();
        for (int i = 0, len = str.length(); i < len; i++) {
            char ch = str.charAt(i);
            if (ch == delimiter) {
                list.add(builder.toString());
                builder.setLength(0);
            } else {
                builder.append(ch);
            }
        }

        if (list.isEmpty()) {
            return Collections.singletonList(str);
        } else if (builder.length() > 0) {
            list.add(builder.toString());
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

    private Utils() {
    }
}
