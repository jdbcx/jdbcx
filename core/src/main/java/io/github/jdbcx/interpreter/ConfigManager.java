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
package io.github.jdbcx.interpreter;

import java.util.Collections;
import java.util.List;
import java.util.Properties;

import io.github.jdbcx.Constants;
import io.github.jdbcx.Option;
import io.github.jdbcx.Utils;

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
public class ConfigManager {
    public static final Option OPTION_CACHE = Option
            .of(new String[] { "cache",
                    "Whether to load configuration into cache and use background thread to monitor changes" });
    public static final Option OPTION_ALIAS = Option.of(new String[] { "alias", "Comma separated aliases" });
    public static final Option OPTION_MANAGED = Option
            .of(new String[] { "manage", "Whether all the configuration are managed", Constants.FALSE_EXPR,
                    Constants.TRUE_EXPR });

    private static final ConfigManager instance = Utils.getService(ConfigManager.class, new ConfigManager());

    public static final ConfigManager getInstance() {
        return instance;
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

    protected ConfigManager() {
    }

    public List<String> getAllIDs(String category) { // NOSONAR
        return Collections.emptyList();
    }

    public boolean hasConfig(String category, String id) {
        return false;
    }

    public Properties getConfig(String category, String id) {
        throw new UnsupportedOperationException();
    }

    public void reload(Properties props) {
    }
}
