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
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import io.github.jdbcx.impl.DefaultConnectionListener;

// driver extension:
// query extension:
// result extension
public interface DriverExtension {
    /**
     * Creates a connection listener.
     *
     * @param conn  connection to listen
     * @param url   connection string
     * @param props connection properties
     * @return non-null connection listener
     */
    default ConnectionListener createListener(Connection conn, String url, Properties props) {
        return DefaultConnectionListener.getInstance();
    }

    /**
     * Gets configuration for this extension.
     *
     * @param props optional connection properties to merge into the configuration,
     *              could be null
     * @return non-null configuration for this extension
     */
    default Properties getConfig(Properties props) {
        Properties config = new Properties();
        for (Option option : getOptions(props)) {
            config.setProperty(option.getName(), option.getDefaultValue());
        }
        if (props != null) {
            config.putAll(props);
        }
        return config;
    }

    /**
     * Gets default configuration for this extension. Same as
     * {@code getConfig(null)}.
     *
     * @return non-null default configuration
     */
    default Properties getDefaultConfig() {
        return getConfig(null);
    }

    /**
     * Gets options available for this extension.
     *
     * @param props optional connection properties, could be null
     * @return non-null options for this extension
     */
    default List<Option> getOptions(Properties props) {
        return Collections.emptyList();
    }

    /**
     * Gets default options for this extension. Same as {@code getOptions(null)}.
     *
     * @return non-null default options
     */
    default List<Option> getDefaultOptions() {
        return getOptions(null);
    }

    // Object createExecutionContext();

    // boolean execute(String content);
}
