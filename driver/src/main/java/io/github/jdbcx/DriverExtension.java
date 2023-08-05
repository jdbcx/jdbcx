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

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map.Entry;
import java.util.Properties;

import io.github.jdbcx.driver.DefaultActivityListener;
import io.github.jdbcx.driver.DefaultDriverExtension;

// driver extension:
// query extension:
// result extension
public interface DriverExtension {
    static String getName(DriverExtension extension) {
        if (extension == null) {
            return Constants.EMPTY_STRING;
        }

        String className = extension.getClass().getSimpleName();
        return className.substring(0, className.length() - DriverExtension.class.getSimpleName().length())
                .toLowerCase(Locale.ROOT);
    }

    static Properties extractProperties(DriverExtension extension, Properties properties) {
        if (properties == null) {
            properties = new Properties();
        }

        Properties props = DefaultDriverExtension.getInstance().getDefaultConfig();
        for (Entry<Object, Object> entry : props.entrySet()) {
            String key = entry.getKey().toString();
            String name = Option.PROPERTY_PREFIX.concat(key);
            String value = properties.getProperty(name);
            if (value != null && !value.equals(entry.getValue())) {
                props.setProperty(key, value);
            }
        }

        if (extension != null && extension != DefaultDriverExtension.getInstance()) {
            Properties p = extension.getDefaultConfig();
            final String prefix = new StringBuilder(Option.PROPERTY_PREFIX).append(getName(extension))
                    .append('.')
                    .toString();
            final int len = prefix.length();
            for (Entry<Object, Object> entry : properties.entrySet()) {
                String key = entry.getKey().toString();
                if (key.startsWith(prefix)) {
                    String name = key.substring(len);
                    Object val = entry.getValue();
                    if (val != null && !val.equals(p.getProperty(name))) {
                        p.put(name, val);
                    }
                }
            }

            props.putAll(p);
        }
        return props;
    }

    default List<String> getAliases() {
        return Collections.emptyList();
    }

    /**
     * Creates a connection listener.
     *
     * @param context query context
     * @param conn    connection to listen
     * @param props   connection properties, usually tailored for this extension
     * @return non-null connection listener
     */
    default JdbcActivityListener createListener(QueryContext context, Connection conn, Properties props) {
        return DefaultActivityListener.getInstance();
    }

    /**
     * Gets configuration for this extension.
     *
     * @param props optional connection properties to merge into the configuration,
     *              could be null
     * @return non-null configuration for this extension
     */
    default Properties getConfig(Properties props) {
        if (props == null) {
            return getDefaultConfig();
        }

        Properties config = new Properties(props);
        for (Option option : getOptions(props)) {
            config.setProperty(option.getName(), option.getDefaultValue());
        }
        return config;
    }

    /**
     * Gets the default configuration for this extension, containing all supported
     * options with their default values.
     *
     * @return non-null default configuration
     */
    default Properties getDefaultConfig() {
        Properties props = new Properties();
        for (Option option : getDefaultOptions()) {
            props.setProperty(option.getName(), option.getDefaultValue());
        }
        return props;
    }

    /**
     * Gets the consolidated configuration options supported by this extension.
     *
     * @param props optional connection properties, could be null
     * @return non-null configuration options supported by this extension
     */
    default List<Option> getOptions(Properties props) {
        final List<Option> defaults = getDefaultOptions();
        if (props == null) {
            return defaults;
        }

        List<Option> list = new ArrayList<>(defaults.size());
        for (Option option : defaults) {
            String value = props.getProperty(option.getName());
            if (value != null && !value.equals(option.getDefaultValue())) {
                list.add(option.update().defaultValue(value).build());
            } else {
                list.add(option);
            }
        }
        return Collections.unmodifiableList(list);
    }

    /**
     * Gets the default configuration options supported by this extension.
     *
     * @return non-null default options
     */
    default List<Option> getDefaultOptions() {
        return Collections.emptyList();
    }

    default boolean supportsDirectQuery() {
        return true;
    }
}
