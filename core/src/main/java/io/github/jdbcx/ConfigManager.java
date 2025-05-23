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
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * Manages application configurations using a two-tier structure.
 * 
 * Configurations are organized into categories and identified by unique IDs.
 * The category and ID structure is implementation-dependent:
 * 
 * <ul>
 * <li>For file system storage, the category is the folder name.
 * The ID is the file name for an individual configuration file.</li>
 *
 * <li>For database storage, the category could map to a table or column name.
 * The ID would be the primary key of a configuration row in the table.</li>
 * </ul>
 * 
 * This class provides methods to load and retrieve configurations using the
 * category/ID values. The underlying storage mechanism is abstracted.
 */
public abstract class ConfigManager {
    private static final Logger log = LoggerFactory.getLogger(ConfigManager.class);

    private static final Option OPTION_CACHE_SIZE = Option.of("globpattern.cache.size", "Glob pattern cache size",
            "100");
    private static final Option OPTION_CACHE_EXPTIME = Option.of("globpattern.cache.exptime",
            "Glob pattern cache expiration time in second", "0");
    private static final Cache<String, Pattern> cache = Cache.create(
            Integer.parseInt(OPTION_CACHE_SIZE.getEffectiveDefaultValue(Option.PROPERTY_PREFIX)),
            Integer.parseInt(OPTION_CACHE_EXPTIME.getEffectiveDefaultValue(Option.PROPERTY_PREFIX)),
            ConfigManager::parseGlobPattern);

    public static final String PROPERTY_FILE_PROVIDER = ConfigManager.class.getPackage().getName()
            + ".config.PropertyFile" + ConfigManager.class.getSimpleName();

    public static final Option OPTION_CACHE = Option
            .of(new String[] { "cache",
                    "Whether to load configuration into cache and use background thread to monitor changes" });
    public static final Option OPTION_ALIAS = Option.of(new String[] { "alias", "Comma separated aliases" });
    public static final Option OPTION_MANAGED = Option
            .of(new String[] { "manage", "Whether all the configuration are managed", Constants.FALSE_EXPR,
                    Constants.TRUE_EXPR });

    /**
     * Loads configuration from the given file. It's a no-op when {@code fileName}
     * is null or empty string.
     *
     * @param fileName file name
     * @param baseDir  optional base directory to search the file, null or empty
     *                 string is same as {@link Constants#CURRENT_DIR}
     * @param base     optional base configuration, which is parent of the returned
     *                 properties
     * @return non-null configuration
     */
    public static final Properties loadConfig(String fileName, String baseDir, Properties base) {
        fileName = Utils.normalizePath(fileName);

        Properties config = new Properties(base);
        if (Checker.isNullOrEmpty(fileName)) {
            log.debug("No config file specified");
        } else {
            Path path = Paths.get(fileName);
            if (!path.isAbsolute()) {
                path = Paths.get(Checker.isNullOrEmpty(baseDir) ? Constants.CURRENT_DIR : baseDir, fileName)
                        .normalize();
            }
            File file = path.toFile();
            if (file.exists() && file.canRead()) {
                try (Reader reader = new InputStreamReader(new FileInputStream(file), Constants.DEFAULT_CHARSET)) {
                    config.load(reader);
                    log.debug("Loaded config from file \"%s\".", fileName);
                } catch (IOException e) {
                    log.warn("Failed to load config from file \"%s\"", fileName, e);
                }
            } else {
                log.debug("Skip loading config as file \"%s\" is not accessible.", fileName);
            }
        }
        return config;
    }

    public static final ConfigManager newInstance(String provider, Properties props) {
        if (props == null) {
            props = new Properties();
        }
        return Utils.newInstance(ConfigManager.class, provider, props);
    }

    public static final Pattern parseGlobPattern(String str) {
        if (Checker.isNullOrEmpty(str)) {
            return null;
        }

        final int len = str.length();
        StringBuilder builder = new StringBuilder(len + 4);
        boolean inCharClass = false;

        for (int i = 0; i < len; i++) {
            char ch = str.charAt(i);
            switch (ch) {
                case '*':
                    if (inCharClass) {
                        builder.append(ch);
                    } else {
                        builder.append(".*");
                    }
                    break;
                case '?':
                    if (inCharClass) {
                        builder.append(ch);
                    } else {
                        builder.append('.');
                    }
                    break;
                case '[':
                    inCharClass = true;
                    builder.append(ch);
                    // Handle negated character class
                    if (i + 1 < len && str.charAt(i + 1) == '!') {
                        builder.append('^');
                        i++;
                    }
                    break;
                case ']':
                    inCharClass = false;
                    builder.append(ch);
                    break;
                case '\\':
                    if (i + 1 < len) {
                        builder.append('\\').append(str.charAt(++i));
                    } else {
                        builder.append('\\');
                    }
                    break;
                case '-':
                    if (!inCharClass) {
                        builder.append('\\').append(ch);
                    } else {
                        builder.append(ch);
                    }
                    break;
                // Escape regex special characters
                case '.':
                case '^':
                case '$':
                case '+':
                case '{':
                case '}':
                case '(':
                case ')':
                case '|': // NOSONAR
                    builder.append('\\').append(ch);
                    break;
                default:
                    builder.append(ch);
            }
        }
        return Pattern.compile(builder.toString());
    }

    protected final String getUniqueId(String category, String id) {
        if (category == null) {
            category = Constants.EMPTY_STRING;
        }
        if (id == null) {
            id = Constants.EMPTY_STRING;
        }
        return new StringBuilder(category.length() + id.length() + 1).append(category).append('/').append(id)
                .toString();
    }

    protected ConfigManager(Properties props) {
    }

    public List<String> getAllIDs(String category) { // NOSONAR
        return Collections.emptyList();
    }

    public List<String> getMatchedIDs(String category, String globPattern) {
        final Pattern pattern = cache.get(globPattern);
        if (pattern == null) {
            return Collections.emptyList();
        }

        List<String> ids = new LinkedList<>();
        for (String id : getAllIDs(category)) {
            if (pattern.matcher(id).matches()) {
                ids.add(id);
            }
        }
        return ids.isEmpty() ? Collections.emptyList() : Collections.unmodifiableList(new ArrayList<>(ids));
    }

    public boolean hasConfig(String category, String id) { // NOSONAR
        return false;
    }

    public Properties getConfig(String category, String id) {
        throw new UnsupportedOperationException();
    }

    public void reload(Properties props) {
    }
}
