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
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
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

    private static final Map<String, DriverExtension> extensions;

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

        log.debug("Loading driver extensions...");
        final String suffix = DriverExtension.class.getSimpleName();
        final Map<String, DriverExtension> map = new LinkedHashMap<>();
        log.debug("Adding default extension: %s", DefaultDriverExtension.getInstance());
        map.put(Constants.EMPTY_STRING, DefaultDriverExtension.getInstance());
        for (DriverExtension extension : ServiceLoader.load(DriverExtension.class,
                DriverExtension.class.getClassLoader())) {
            final String className = extension.getClass().getSimpleName();
            if (className.endsWith(suffix)) {
                final String name = className.substring(0, className.length() - suffix.length())
                        .toLowerCase(Locale.ROOT);
                log.debug("Adding extension(%s): %s", name, extension);
                map.put(name, extension);
            } else {
                log.warn("Skip extension(%s) as its name does not end with \"%s\"", extension, suffix);
            }
        }
        extensions = Collections.unmodifiableMap(map);
        log.debug("Loaded %d driver extension(s)", extensions.size());
    }

    static DriverPropertyInfo create(String prefix, Option option, Properties props) {
        DriverPropertyInfo propInfo = new DriverPropertyInfo(prefix.concat(option.getName()),
                props.getProperty(option.getName(), option.getEffectiveDefaultValue(prefix)));
        propInfo.required = false;
        propInfo.description = option.getDescription();
        propInfo.choices = option.getChoices();
        return propInfo;
    }

    static String getExtensionName(DriverExtension extension) {
        String className = extension.getClass().getSimpleName();
        return className.substring(0, className.length() - DriverExtension.class.getSimpleName().length())
                .toLowerCase(Locale.ROOT);
    }

    /**
     * Gets extension-specific driver properties.
     *
     * @param info non-null properties
     * @return non-null driver propperties
     */
    static DriverPropertyInfo[] getExtPropertyInfo(Properties info) {
        List<DriverPropertyInfo> list = new ArrayList<>(15);

        for (DriverExtension ext : extensions.values()) {
            final String prefix = ext == DefaultDriverExtension.getInstance() ? PROPERTY_PREFIX
                    : new StringBuilder(PROPERTY_PREFIX).append(getExtensionName(ext)).append('.').toString();
            for (Option option : ext.getOptions(info)) {
                list.add(create(prefix, option, info));
            }
        }
        return list.toArray(new DriverPropertyInfo[0]);
    }

    private final AtomicReference<Driver> cache = new AtomicReference<>(null);
    private final AtomicReference<Properties> config = new AtomicReference<>(null);
    private final AtomicReference<URLClassLoader> loader = new AtomicReference<>(null);

    private void closeUrlClassLoader(URLClassLoader loader) {
        if (loader != null) {
            try {
                loader.close();
            } catch (IOException e) {
                // ignore
            }
        }
    }

    Properties loadDefaultConfig(String fileName) {
        Properties defaultConfig = config.get();
        if (defaultConfig == null) {
            defaultConfig = new Properties();
            if (Checker.isNullOrEmpty(fileName)) {
                log.debug("No default config file specified");
            } else {
                Path path = Paths.get(fileName);
                if (!path.isAbsolute()) {
                    path = Paths.get(Constants.CURRENT_DIR, fileName).normalize();
                }
                File file = path.toFile();
                if (file.exists() && file.canRead()) {
                    try (FileReader reader = new FileReader(file, StandardCharsets.UTF_8)) {
                        defaultConfig.load(reader);
                        log.debug("Loaded default config from file \"%s\".", fileName);
                    } catch (IOException e) {
                        log.warn("Failed to load default config from file \"%s\"", fileName, e);
                    }
                } else {
                    log.debug("Skip loading default config as file \"%s\" is not accessible.", fileName);
                }
            }
            if (!config.compareAndSet(null, defaultConfig)) {
                defaultConfig = config.get();
            }
        }
        return defaultConfig;
    }

    /**
     * Extracts extended properties from the given properties.
     *
     * @param extension  extension
     * @param properties properties
     * @return non-null properties
     */
    protected Properties extractExtendedProperties(DriverExtension extension, Properties properties) {
        if (extension == null) {
            return new Properties();
        }

        Properties props = DefaultDriverExtension.getInstance().getDefaultConfig();
        if (properties != null && !properties.isEmpty()) {
            String configPath = properties.getProperty(PROPERTY_PREFIX.concat(Option.CONFIG_PATH.getName()),
                    Option.CONFIG_PATH.getDefaultValue());
            Properties defaultConfig = loadDefaultConfig(Utils.normalizePath(configPath));
            if (!defaultConfig.isEmpty()) {
                Properties newProps = new Properties(defaultConfig);
                newProps.putAll(properties);
                properties = newProps;
            }

            for (String key : props.stringPropertyNames()) {
                String name = PROPERTY_PREFIX.concat(key);
                String value = properties.getProperty(name);
                if (value != null) {
                    props.put(key, value);
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
                        p.put(key, value);
                    }
                }

                props.putAll(p);
            }
        } else if (extension != DefaultDriverExtension.getInstance()) {
            props.putAll(extension.getDefaultConfig());
        }
        return props;
    }

    protected DriverExtension getDriverExtension(String url) {
        DriverExtension extension = null;
        int index = url.indexOf(':', JDBCX_PREFIX.length());
        if (index > 0) {
            String extName = url.substring(JDBCX_PREFIX.length(), index);
            extension = extensions.get(extName);
        }
        return extension != null ? extension : DefaultDriverExtension.getInstance();
    }

    protected String normalizeUrl(DriverExtension extension, String url) {
        if (Checker.isNullOrEmpty(url) || !url.startsWith(JDBCX_PREFIX)) { // invalid
            return url;
        }

        if (extension == DefaultDriverExtension.getInstance()) {
            return JDBC_PREFIX.concat(url.substring(JDBCX_PREFIX.length()));
        }

        String className = extension.getClass().getSimpleName();
        String extName = className.substring(0, className.length() - DriverExtension.class.getSimpleName().length());
        return JDBC_PREFIX.concat(url.substring(JDBCX_PREFIX.length() + extName.length() + 1));
    }

    /**
     * Gets the actual driver.
     *
     * @param url   normalized JDBC connection URL
     * @param props PRQL related properties
     * @return actual driver
     * @throws SQLException when failed to get the actual driver
     */
    protected Driver getActualDriver(String url, Properties props) throws SQLException {
        Driver d = cache.get();
        URLClassLoader l = this.loader.get();
        if (d == null || !d.acceptsURL(url)) {
            String customClassPath = Utils.getPath(Option.CUSTOM_CLASSPATH.getValue(props), false).toString();
            if (customClassPath != null && !customClassPath.isEmpty()) {
                if (!(l instanceof ExpandedUrlClassLoader)
                        || !((ExpandedUrlClassLoader) l).getOriginalUrls().equals(customClassPath)) {
                    closeUrlClassLoader(l);

                    final URLClassLoader newLoader = new ExpandedUrlClassLoader(getClass(), customClassPath);
                    if (loader.compareAndSet(l, newLoader)) {
                        l = newLoader;
                    } else {
                        closeUrlClassLoader(newLoader);
                        l = loader.get();
                    }
                }
            } else {
                if (l instanceof ExpandedUrlClassLoader) {
                    closeUrlClassLoader(l);
                    l = null;
                }
                if (l == null) {
                    final URLClassLoader newLoader = new URLClassLoader(new URL[0], getClass().getClassLoader());
                    if (loader.compareAndSet(l, newLoader)) {
                        l = newLoader;
                    } else {
                        closeUrlClassLoader(newLoader);
                        l = loader.get();
                    }
                }
            }

            boolean found = false;
            for (Iterator<Driver> it = ServiceLoader.load(Driver.class, l).iterator(); it.hasNext();) {
                Driver driver;
                try {
                    driver = it.next();
                } catch (Throwable t) { // NOSONAR
                    // usually just ServiceConfigurationError for not able to load this driver
                    continue;
                }
                if (driver.acceptsURL(url)) {
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
                Driver newDriver = new InvalidDriver(props);
                try {
                    newDriver = DriverManager.getDriver(url);
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
        return url != null && url.length() > JDBCX_PREFIX.length()
                && url.substring(0, JDBCX_PREFIX.length()).equalsIgnoreCase(JDBCX_PREFIX);
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            throw SqlExceptionUtils
                    .clientError("The connection URL provided is invalid. It must begin with \"jdbcx:\".");
        }

        DriverExtension extension = getDriverExtension(url);
        String actualUrl = normalizeUrl(extension, url);
        Properties props = extractExtendedProperties(extension, info);

        return new WrappedConnection(extension, getActualDriver(actualUrl, props).connect(actualUrl, info), actualUrl,
                props);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        final DriverExtension extension = getDriverExtension(url);
        final String actualUrl = normalizeUrl(extension, url);
        final Properties props = extractExtendedProperties(extension, info);

        DriverPropertyInfo[] driverInfo = getActualDriver(actualUrl, props).getPropertyInfo(actualUrl, info);
        if (driverInfo == null) {
            driverInfo = new DriverPropertyInfo[0];
        }
        int index = driverInfo.length;
        DriverPropertyInfo[] extInfo = getExtPropertyInfo(props);

        DriverPropertyInfo[] merged = new DriverPropertyInfo[index + extInfo.length];
        System.arraycopy(driverInfo, 0, merged, 0, index);
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
