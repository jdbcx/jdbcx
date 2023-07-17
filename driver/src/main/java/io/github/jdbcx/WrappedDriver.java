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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverAction;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicReference;

import io.github.jdbcx.impl.DefaultDriverExtension;

/**
 * This class is essentially a wrapper of the {@link Driver} interface. It
 * accepts a connection string that starts with {@code jdbcx:}, followed by an
 * optional name of a {@code DriverExtension}, which defines the default
 * behaviors of the driver. For example, in the JDBC driver, we used to use
 * {@code jdbc:mysql://localhost/test} to connect to MySQL. However, now it has
 * changed to {@code jdbcx:mysql://localhost/test} or
 * {@code jdbcx:prql:mysql://localhost/test}, if you prefer to use PRQL.
 */
public class WrappedDriver implements Driver, DriverAction {
    private static final Logger log = LoggerFactory.getLogger(WrappedDriver.class);

    static final Option OPTION_JDBCX_PREFIX = Option.of(new String[] { "jdbcx.prefix", "JDBCX prefix", "jdbcx" });

    static final String PROPERTY_JDBCX = OPTION_JDBCX_PREFIX.getEffectiveDefaultValue(null).toLowerCase(Locale.ROOT);
    static final String PROPERTY_PREFIX = PROPERTY_JDBCX + ".";

    static final String JDBC_PREFIX = "jdbc:";
    static final String JDBCX_PREFIX = PROPERTY_JDBCX + ":";

    static {
        if (PROPERTY_JDBCX.isEmpty() || "jdbc".equals(PROPERTY_JDBCX)) {
            throw new IllegalStateException(
                    Utils.format("The JDBCX prefix cannot be empty or \"jdbc\". "
                            + "Please modify it to a different value using either "
                            + "the system property \"%s\" or the environment variable \"%s\".",
                            OPTION_JDBCX_PREFIX.getSystemProperty(null),
                            OPTION_JDBCX_PREFIX.getEnvironmentVariable(null)));
        }

        log.debug("Registering %s", WrappedDriver.class.getName());
        try {
            DriverManager.registerDriver(new WrappedDriver());
        } catch (SQLException e) {
            throw new IllegalStateException(e); // should never happen
        }
    }

    static final class DriverInfo {
        final WrappedDriver driver;
        final String normalizedUrl;
        final Properties normalizedInfo;

        final URLClassLoader customClassLoader;
        final String actualUrl;
        final DriverExtension extension;
        final Properties extensionProps;

        static void closeUrlClassLoader(URLClassLoader loader) {
            if (loader != null) {
                try {
                    loader.close();
                } catch (IOException e) {
                    log.warn("Failed to close class loader [%s]", loader, e);
                }
            }
        }

        /**
         * Extracts extended properties from the given properties.
         *
         * @param extension  extension
         * @param properties properties
         * @return non-null properties
         */
        static Properties extractExtendedProperties(DriverExtension extension, Properties properties) {
            Properties props = DefaultDriverExtension.getInstance().getDefaultConfig();
            for (String key : props.stringPropertyNames()) {
                String name = PROPERTY_PREFIX.concat(key);
                String value = properties.getProperty(name);
                if (value != null) {
                    props.setProperty(key, value);
                }
            }

            if (extension != DefaultDriverExtension.getInstance()) {
                Properties p = extension.getDefaultConfig();
                final String prefix = new StringBuilder(PROPERTY_PREFIX).append(getExtensionName(extension)).append('.')
                        .toString();
                for (String key : p.stringPropertyNames()) {
                    String name = prefix.concat(key);
                    String value = properties.getProperty(name);
                    if (value != null) {
                        p.setProperty(key, value);
                    }
                }

                props.putAll(p);
            }
            return props;
        }

        static DriverExtension getDriverExtension(String url, Map<String, DriverExtension> extensions) {
            if (extensions == null) {
                extensions = Collections.emptyMap();
            }

            DriverExtension ext = null;
            if (url != null) {
                int index = url.indexOf(':', JDBCX_PREFIX.length());
                if (index > 0) {
                    String extName = url.substring(JDBCX_PREFIX.length(), index);
                    ext = extensions.get(extName);
                }
            }
            return ext != null ? ext : DefaultDriverExtension.getInstance();
        }

        static String getExtensionName(DriverExtension extension) {
            String className = extension.getClass().getSimpleName();
            return className.substring(0, className.length() - DriverExtension.class.getSimpleName().length())
                    .toLowerCase(Locale.ROOT);
        }

        static String normalizeUrl(DriverExtension extension, String url) {
            if (Checker.isNullOrEmpty(url) || !url.startsWith(JDBCX_PREFIX)) { // invalid
                return url;
            }

            if (extension == DefaultDriverExtension.getInstance()) {
                return JDBC_PREFIX.concat(url.substring(JDBCX_PREFIX.length()));
            }

            String className = extension.getClass().getSimpleName();
            String extName = className.substring(0,
                    className.length() - DriverExtension.class.getSimpleName().length());
            return JDBC_PREFIX.concat(url.substring(JDBCX_PREFIX.length() + extName.length() + 1));
        }

        URLClassLoader getCustomClassLoader(String customClassPath) {
            URLClassLoader l = driver.loader.get();
            if (!Checker.isNullOrEmpty(customClassPath)) {
                if (!(l instanceof ExpandedUrlClassLoader)
                        || !((ExpandedUrlClassLoader) l).getOriginalUrls().equals(customClassPath)) {
                    closeUrlClassLoader(l);

                    final URLClassLoader newLoader = new ExpandedUrlClassLoader(getClass(), customClassPath);
                    if (driver.loader.compareAndSet(l, newLoader)) {
                        l = newLoader;
                    } else {
                        closeUrlClassLoader(newLoader);
                        l = driver.loader.get();
                    }
                }
            } else {
                if (l instanceof ExpandedUrlClassLoader) {
                    closeUrlClassLoader(l);
                    l = null;
                }
                if (l == null) {
                    final URLClassLoader newLoader = new URLClassLoader(new URL[0], getClass().getClassLoader());
                    if (driver.loader.compareAndSet(l, newLoader)) {
                        l = newLoader;
                    } else {
                        closeUrlClassLoader(newLoader);
                        l = driver.loader.get();
                    }
                }
            }

            return l;
        }

        Map<String, DriverExtension> getExtensions() {
            Map<String, DriverExtension> map = driver.extensions.get();
            if (map == null) {
                log.debug("Loading driver extensions...");
                final String suffix = DriverExtension.class.getSimpleName();
                map = new LinkedHashMap<>();
                log.debug("Adding default extension: %s", DefaultDriverExtension.getInstance());
                map.put(Constants.EMPTY_STRING, DefaultDriverExtension.getInstance());
                final ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
                final boolean useCustomClassLoader = customClassLoader instanceof ExpandedUrlClassLoader
                        && customClassLoader != originalClassLoader;
                try {
                    if (useCustomClassLoader) {
                        // FIXME this is tricky, perhaps it's better to explicitly initialize extension
                        Thread.currentThread().setContextClassLoader(customClassLoader);
                        log.debug("Changed context class loader from [%s] to [%s]...", originalClassLoader,
                                customClassLoader);
                    }
                    for (DriverExtension ext : ServiceLoader.load(DriverExtension.class, customClassLoader)) {
                        final String className = ext.getClass().getSimpleName();
                        if (className.endsWith(suffix)) {
                            final String name = className.substring(0, className.length() - suffix.length())
                                    .toLowerCase(Locale.ROOT);
                            log.debug("Adding extension \"%s\": %s", name, ext);
                            map.put(name, ext);
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
                if (!driver.extensions.compareAndSet(null, map)) {
                    map = driver.extensions.get();
                }
                log.debug("Loaded %d driver extension(s)", map.size());
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

            for (DriverExtension ext : driver.extensions.get().values()) {
                final String prefix = ext == DefaultDriverExtension.getInstance() ? PROPERTY_PREFIX
                        : new StringBuilder(PROPERTY_PREFIX).append(getExtensionName(ext)).append('.').toString();
                for (Option option : ext.getOptions(extensionProps)) {
                    list.add(create(prefix, option, extensionProps));
                }
            }
            return list.toArray(new DriverPropertyInfo[0]);
        }

        Properties loadDefaultConfig(String fileName) {
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

        DriverInfo() {
            this(new WrappedDriver(), null, null);
        }

        DriverInfo(WrappedDriver driver, String url, Properties info) {
            this.driver = driver;
            this.normalizedUrl = url != null ? url : Constants.EMPTY_STRING;

            if (info == null) {
                info = new Properties();
            }
            final String configPath = Utils
                    .normalizePath(info.getProperty(PROPERTY_PREFIX.concat(Option.CONFIG_PATH.getName()),
                            Option.CONFIG_PATH.getDefaultValue()));
            final Properties defaultConfig = loadDefaultConfig(configPath);
            if (!defaultConfig.isEmpty()) {
                for (Entry<Object, Object> entry : info.entrySet()) {
                    String name = (String) entry.getKey();
                    String value = (String) entry.getValue();
                    if (name.startsWith(PROPERTY_PREFIX) && value.isEmpty()) {
                        continue;
                    }
                    defaultConfig.setProperty(name, value);
                }
                info = defaultConfig;
            }
            this.normalizedInfo = info;

            final String customClassPath = Utils
                    .normalizePath(info.getProperty(PROPERTY_PREFIX.concat(Option.CUSTOM_CLASSPATH.getName()),
                            Option.CUSTOM_CLASSPATH.getEffectiveDefaultValue(PROPERTY_PREFIX)));
            this.customClassLoader = getCustomClassLoader(customClassPath);
            this.extension = getDriverExtension(this.normalizedUrl, getExtensions());
            this.actualUrl = normalizeUrl(this.extension, this.normalizedUrl);
            this.extensionProps = extractExtendedProperties(this.extension, info);
        }
    }

    static DriverPropertyInfo create(String prefix, Option option, Properties props) {
        DriverPropertyInfo propInfo = new DriverPropertyInfo(prefix.concat(option.getName()),
                props.getProperty(option.getName(), option.getEffectiveDefaultValue(prefix)));
        propInfo.required = false;
        propInfo.description = option.getDescription();
        propInfo.choices = option.getChoices();
        return propInfo;
    }

    private final AtomicReference<Driver> cache = new AtomicReference<>();
    private final AtomicReference<Map<String, DriverExtension>> extensions = new AtomicReference<>();
    private final AtomicReference<URLClassLoader> loader = new AtomicReference<>();

    /**
     * Gets the actual driver.
     *
     * @param driverInfo non-null {@link DriverInfo}
     * @return actual driver
     * @throws SQLException when failed to get the actual driver
     */
    protected Driver getActualDriver(DriverInfo driverInfo) throws SQLException {
        Driver d = cache.get();
        if (d == null || !d.acceptsURL(driverInfo.actualUrl)) {
            boolean found = false;
            for (Iterator<Driver> it = ServiceLoader.load(Driver.class, driverInfo.customClassLoader).iterator(); it
                    .hasNext();) {
                Driver driver;
                try {
                    driver = it.next();
                } catch (Throwable t) { // NOSONAR
                    // usually just ServiceConfigurationError for not able to load this driver
                    continue;
                }
                if (driver.acceptsURL(driverInfo.actualUrl)) {
                    if (cache.compareAndSet(d, driver)) {
                        d = driver;
                    } else {
                        d = cache.get();
                    }
                    found = true;
                    break;
                }
            }

            if (!found) {
                Driver newDriver = new InvalidDriver(driverInfo.extensionProps);
                try {
                    newDriver = DriverManager.getDriver(driverInfo.actualUrl);
                } catch (SQLException e) {
                    // ignore
                }
                if (cache.compareAndSet(d, newDriver)) {
                    d = newDriver;
                } else {
                    d = cache.get();
                }
            }
        }

        return d;
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return url != null && url.length() >= JDBCX_PREFIX.length()
                && url.substring(0, JDBCX_PREFIX.length()).equalsIgnoreCase(JDBCX_PREFIX);
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        final DriverInfo driverInfo = new DriverInfo(this, url, info);
        return acceptsURL(url) ? new WrappedConnection(driverInfo)
                : getActualDriver(driverInfo).connect(driverInfo.actualUrl, driverInfo.normalizedInfo);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        final DriverInfo driverInfo = new DriverInfo(this, url, info);

        DriverPropertyInfo[] driverPropInfo = getActualDriver(driverInfo)
                .getPropertyInfo(driverInfo.actualUrl, driverInfo.normalizedInfo);
        if (driverPropInfo == null) {
            driverPropInfo = new DriverPropertyInfo[0];
        }
        int index = driverPropInfo.length;
        DriverPropertyInfo[] extInfo = driverInfo.getExtensionInfo();

        DriverPropertyInfo[] merged = new DriverPropertyInfo[index + extInfo.length];
        System.arraycopy(driverPropInfo, 0, merged, 0, index);
        System.arraycopy(extInfo, 0, merged, index, extInfo.length);
        return merged;
    }

    @Override
    public int getMajorVersion() {
        Driver d = cache.get();
        return d != null ? d.getMajorVersion() : 0;
    }

    @Override
    public int getMinorVersion() {
        Driver d = cache.get();
        return d != null ? d.getMinorVersion() : 0;
    }

    @Override
    public boolean jdbcCompliant() {
        Driver d = cache.get();
        return d != null && d.jdbcCompliant();
    }

    @Override
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
        Driver d = cache.get();
        return d != null ? d.getParentLogger() : null;
    }

    @Override
    public void deregister() {
        this.cache.set(null);
        this.extensions.set(null);
        URLClassLoader l = loader.getAndSet(null);
        if (l != null) {
            try {
                l.close();
            } catch (IOException e) {
                // ignore
            }
        }
    }
}
