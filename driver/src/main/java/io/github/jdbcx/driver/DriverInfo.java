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
package io.github.jdbcx.driver;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import io.github.jdbcx.Checker;
import io.github.jdbcx.Constants;
import io.github.jdbcx.DriverExtension;
import io.github.jdbcx.ExpandedUrlClassLoader;
import io.github.jdbcx.Logger;
import io.github.jdbcx.LoggerFactory;
import io.github.jdbcx.Option;
import io.github.jdbcx.Utils;

final class DriverInfo {
    private static final Logger log = LoggerFactory.getLogger(DriverInfo.class);

    final Driver driver;

    final String normalizedUrl;
    final Properties mergedInfo; // consolidated properties for both JDBCX and the driver
    final Properties normalizedInfo; // properties without "jdbcx." prefix

    final ClassLoader customClassLoader;
    final String actualUrl;
    final DriverExtension extension;
    final Properties extensionProps;

    private final String originalUrl;
    private final Properties originalProperties;
    private final AtomicReference<Map<String, DriverExtension>> extensions = new AtomicReference<>();

    /**
     * Creates a {@link DriverPropertyInfo}.
     *
     * @param prefix non-null prefix of the option's name
     * @param option non-null option
     * @param props  non-null properties
     * @return a new instance
     */
    static DriverPropertyInfo create(String prefix, Option option, Properties props) {
        DriverPropertyInfo propInfo = new DriverPropertyInfo(prefix.concat(option.getName()),
                props.getProperty(option.getName(), option.getEffectiveDefaultValue(prefix)));
        propInfo.required = false;
        propInfo.description = option.getDescription();
        propInfo.choices = option.getChoices();
        return propInfo;
    }

    /**
     * Finds the suitable JDBC driver.
     *
     * @param actualUrl   connection url
     * @param props       connection properties
     * @param classLoader class loader can be used to find JDBC driver
     * @return non-null JDBC driver
     * @throws SQLException when failed to get the actual driver
     */
    static Driver findSuitableDriver(String url, Properties props, ClassLoader classLoader) {
        Driver d = null;
        boolean found = false;

        for (Driver newDriver : Utils.load(Driver.class, classLoader)) {
            try {
                if (newDriver.acceptsURL(url)) {
                    d = newDriver;
                    found = true;
                    break;
                }
            } catch (Throwable t) { // NOSONAR
                log.debug("Failed to test url [%s] using driver [%s]", url, newDriver, t);
            }
        }

        if (!found) {
            Driver newDriver = new InvalidDriver(props);
            try {
                newDriver = DriverManager.getDriver(url);
            } catch (SQLException e) {
                // ignore
            }
            d = newDriver;
        }
        return d;
    }

    static ClassLoader getCustomClassLoader(String customClassPath) {
        return Checker.isNullOrEmpty(customClassPath) ? DriverInfo.class.getClassLoader()
                : ExpandedUrlClassLoader.of(DriverInfo.class, customClassPath);
    }

    static DriverExtension getDriverExtension(String url, Map<String, DriverExtension> extensions) {
        if (extensions == null) {
            extensions = Collections.emptyMap();
        }

        DriverExtension ext = null;
        if (url != null) {
            int prefixLength = ConnectionManager.JDBCX_PREFIX.length();
            int index = url.indexOf(':', prefixLength);
            if (index > 0) {
                String extName = url.substring(prefixLength, index);
                ext = extensions.get(extName);
            }
        }
        return ext != null ? ext : DefaultDriverExtension.getInstance();
    }

    /**
     * Loads default configuration from the given file. It's a no-op when
     * {@code fileName} is null or empty string.
     *
     * @param fileName file name
     * @return non-null default configuration
     */
    static Properties loadDefaultConfig(String fileName) {
        fileName = Utils.normalizePath(fileName);

        Properties defaultConfig = new Properties();
        if (Checker.isNullOrEmpty(fileName)) {
            log.debug("No default config file specified");
        } else {
            Path path = Paths.get(fileName);
            if (!path.isAbsolute()) {
                path = Paths.get(Constants.CURRENT_DIR, fileName).normalize();
            }
            File file = path.toFile();
            if (file.exists() && file.canRead()) {
                try (Reader reader = new InputStreamReader(new FileInputStream(file), Constants.DEFAULT_CHARSET)) {
                    defaultConfig.load(reader);
                    log.debug("Loaded default config from file \"%s\".", fileName);
                } catch (IOException e) {
                    log.warn("Failed to load default config from file \"%s\"", fileName, e);
                }
            } else {
                log.debug("Skip loading default config as file \"%s\" is not accessible.", fileName);
            }
        }
        return defaultConfig;
    }

    /**
     * Normalizes the connection URL by removing extension prefix as well as
     * replacing the leading {@code jdbcx:} to {@code jdbc:}.
     * 
     * @param extension non-null driver extension
     * @param url       connection URL
     * @return normalized URL
     */
    static String normalizeUrl(DriverExtension extension, String url) {
        if (!Utils.startsWith(url, ConnectionManager.JDBCX_PREFIX, true)) { // invalid
            return url;
        }

        int index = ConnectionManager.JDBCX_PREFIX.length();
        if (extension != DefaultDriverExtension.getInstance()) {
            index += extension.getName().length() + 1;
        }

        return ConnectionManager.JDBC_PREFIX.concat(url.substring(index));
    }

    Map<String, DriverExtension> getExtensions() {
        Map<String, DriverExtension> map = extensions.get();
        if (map == null) {
            log.debug("Loading driver extensions...");
            final String suffix = DriverExtension.class.getSimpleName();
            map = new LinkedHashMap<>();
            log.debug("Adding default extension: %s", DefaultDriverExtension.getInstance());
            map.put(Constants.EMPTY_STRING, DefaultDriverExtension.getInstance());
            final ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
            final boolean useCustomClassLoader = customClassLoader instanceof ExpandedUrlClassLoader
                    && customClassLoader != originalClassLoader;
            int counter = 0;
            try {
                if (useCustomClassLoader) {
                    // FIXME this is tricky, perhaps it's better to explicitly initialize extension
                    Thread.currentThread().setContextClassLoader(customClassLoader);
                    log.debug("Changed context class loader from [%s] to [%s]...", originalClassLoader,
                            customClassLoader);
                }

                for (DriverExtension ext : Utils.load(DriverExtension.class, customClassLoader)) {
                    final String className = ext.getClass().getSimpleName();
                    // enforce class name for consistency
                    if (className.endsWith(suffix)) {
                        final String name = className.substring(0, className.length() - suffix.length())
                                .toLowerCase(Locale.ROOT);
                        log.debug("Adding extension [%s]: %s", name, ext);
                        map.put(name, ext);
                        counter++;
                        // name matters but aliases are not
                        for (String alias : ext.getAliases()) {
                            String nAlias = alias.toLowerCase(Locale.ROOT);
                            DriverExtension e = map.get(nAlias);
                            if (e != null) {
                                log.warn(" - skip alias [%s] as it's been taken by [%s]", nAlias, e);
                            } else {
                                log.debug(" - adding alias [%s]", nAlias);
                                e = map.put(nAlias, ext);
                                if (e != null) {
                                    log.warn(" - alias [%s] has been reassigned from [%s] to [%s]", nAlias, e, ext);
                                }
                            }
                        }
                    } else {
                        log.warn("Skip extension [%s] as its class name does not end with \"%s\"", ext, suffix);
                    }
                }
            } finally {
                if (useCustomClassLoader) {
                    Thread.currentThread().setContextClassLoader(originalClassLoader);
                    log.debug("Changed context class loader back to [%s]", originalClassLoader);
                }
            }
            map = Collections.unmodifiableMap(map);
            if (!extensions.compareAndSet(null, map)) {
                map = extensions.get();
            }
            log.debug("Loaded %d driver extension(s)", counter);
        }
        return map;
    }

    /**
     * Gets extension-specific driver properties.
     *
     * @param info non-null properties
     * @return non-null driver propperties
     */
    DriverPropertyInfo[] getExtensionInfo() {
        List<DriverPropertyInfo> list = new ArrayList<>(15);

        Set<DriverExtension> sets = new HashSet<>(extensions.get().values());
        for (DriverExtension ext : sets) {
            final String prefix = ext == DefaultDriverExtension.getInstance() ? Option.PROPERTY_PREFIX
                    : new StringBuilder(Option.PROPERTY_PREFIX).append(ext.getName())
                            .append('.').toString();
            for (Option option : ext.getOptions(extensionProps)) {
                list.add(create(prefix, option, extensionProps));
            }
        }
        return list.toArray(new DriverPropertyInfo[0]);
    }

    DriverInfo() {
        this(null, null);
    }

    DriverInfo(String url, Properties info) {
        this.originalUrl = url;
        this.originalProperties = info;

        this.normalizedUrl = url != null ? url.trim() : Constants.EMPTY_STRING;

        if (info == null) {
            info = new Properties();
        }
        final String configPath = info.getProperty(Option.PROPERTY_PREFIX.concat(Option.CONFIG_PATH.getName()),
                Option.CONFIG_PATH.getDefaultValue());
        final Properties defaultConfig = loadDefaultConfig(configPath);
        if (!defaultConfig.isEmpty()) {
            for (Entry<Object, Object> entry : info.entrySet()) {
                String name = (String) entry.getKey();
                String value = (String) entry.getValue();
                // FIXME what if we want to use empty value to override the default?
                if (name.startsWith(Option.PROPERTY_PREFIX) && value.isEmpty()) {
                    continue;
                }
                defaultConfig.setProperty(name, value);
            }
            info = defaultConfig;
        }
        this.mergedInfo = info;
        this.normalizedInfo = new Properties();
        for (Entry<Object, Object> entry : info.entrySet()) {
            String name = (String) entry.getKey();
            String value = (String) entry.getValue();
            if (!name.startsWith(Option.PROPERTY_PREFIX)) {
                normalizedInfo.setProperty(name, value);
            }
        }

        final String customClassPath = Utils.normalizePath(info.getProperty(Option.CUSTOM_CLASSPATH.getJdbcxName(),
                Option.CUSTOM_CLASSPATH.getEffectiveDefaultValue(Option.PROPERTY_PREFIX)));
        this.customClassLoader = getCustomClassLoader(customClassPath);
        this.extension = getDriverExtension(this.normalizedUrl, getExtensions());
        this.actualUrl = normalizeUrl(this.extension, this.normalizedUrl);
        this.extensionProps = DriverExtension.extractProperties(this.extension, info);

        this.driver = findSuitableDriver(this.actualUrl, this.extensionProps, this.customClassLoader);
    }

    boolean isFor(String url, Properties info) {
        return Objects.equals(originalUrl, url) && Objects.equals(originalProperties, info);
    }

    void close() {
        if (customClassLoader instanceof URLClassLoader) {
            URLClassLoader ucl = (URLClassLoader) customClassLoader;
            ucl.clearAssertionStatus();
            try {
                ucl.close();
            } catch (IOException e) {
                log.debug("Failed to close class loader [%s]", ucl, e);
            }
        }
    }
}
