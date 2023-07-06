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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Properties;

/**
 * This class defines a configuration option, which consists of a key, a default
 * value in String format, a description, and optional choices.
 */
public final class Option implements Serializable {
    private static final long serialVersionUID = -2250888694424064713L;

    // most common options
    /**
     * Custom classpath using {@link ExpandedUrlClassLoader}.
     */
    public static final Option CUSTOM_CLASSPATH = Option.of(new String[] { "custom.classpath", "Custom classpath" });

    /**
     * The Builder class is used to construct options.
     */
    public static final class Builder {
        private String name;
        private String description;
        private String defaultValue;
        private List<String> choices;

        Builder() {
        }

        Builder(Option template) {
            this.name = template.name;
            this.description = template.name;
            this.defaultValue = template.defaultValue;
            this.choices = new ArrayList<>(template.choices);
        }

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder description(String description) {
            this.description = description;
            return this;
        }

        public Builder defaultValue(String defaultValue) {
            this.defaultValue = defaultValue;
            return this;
        }

        public Builder choices(String... choices) {
            this.choices = new ArrayList<>();
            this.choices.addAll(Arrays.asList(choices));
            return this;
        }

        public Builder addChoice(String choice) {
            if (this.choices == null) {
                this.choices = new ArrayList<>();
            }
            this.choices.add(choice);
            return this;
        }

        public Builder removeChoice(String choice) {
            if (this.choices != null) {
                this.choices.remove(choice);
            }
            return this;
        }

        public Option build() {
            return Option.of(name, description, defaultValue, choices.toArray(new String[0]));
        }
    }

    /**
     * Creates a builder class in order to build an option.
     *
     * @return builder class to build option
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Gets trimmed default value from environment variable. By default the
     * environment variable is named as {@code prefix + name} in upper case
     * with dot replaced to underscore.
     *
     * @param prefix optional prefix of the option
     * @return trimmed default value defined in environment variable
     */
    public Optional<String> getDefaultValueFromEnvVar(String prefix) {
        String value = System.getenv(getEnvironmentVariable(prefix));
        if (value != null) {
            value = value.trim();
        }
        return Optional.ofNullable(value);
    }

    /**
     * Gets trimmed default value from system property. By default the system
     * property is named as {@code prefix + name} in lower case.
     *
     * @param prefix optional prefix of the option
     * @return trimmed default value defined in system property
     */
    public Optional<String> getDefaultValueFromSysProp(String prefix) {
        String value = System.getProperty(getSystemProperty(prefix));
        if (value != null) {
            value = value.trim();
        }
        return Optional.ofNullable(value);
    }

    /**
     * Gets environment variable for the option.
     *
     * @param prefix optional prefix of the option
     * @return environment variable
     */
    public String getEnvironmentVariable(String prefix) {
        String key = (Checker.isNullOrEmpty(prefix) ? name : prefix.concat(name)).toUpperCase(Locale.ROOT);
        return key.replace('.', '_');
    }

    /**
     * Gets system property for the option.
     *
     * @param prefix optional prefix of the option
     * @return system property
     */
    public String getSystemProperty(String prefix) {
        return Checker.isNullOrEmpty(prefix) ? name : prefix.toLowerCase(Locale.ROOT).concat(name);
    }

    /**
     * Creates an option.
     *
     * @param info non-null array contains name, and optionally description, default
     *             value, and choices
     * @return option
     */
    public static Option of(String[] info) {
        if (info == null || info.length == 0) {
            throw new IllegalArgumentException("The given array must contain at least one element.");
        }

        String description = info.length > 1 ? info[1] : Constants.EMPTY_STRING;
        String defaultValue = info.length > 2 ? info[2] : Constants.EMPTY_STRING;
        String[] choices = Constants.EMPTY_STRING_ARRAY;
        if (info.length > 3) {
            choices = new String[info.length - 2];
            System.arraycopy(info, 2, choices, 0, choices.length);
        }
        return of(info[0], description, defaultValue, choices);
    }

    /**
     * Creates an option.
     *
     * @param name         non-null name
     * @param description  optional description
     * @param defaultValue optional default value
     * @param choices      optional choices
     * @return option
     */
    public static Option of(String name, String description, String defaultValue, String... choices) {
        name = Checker.nonEmpty(name, "name").toLowerCase(Locale.ROOT);
        description = description != null ? description : Constants.EMPTY_STRING;
        defaultValue = defaultValue != null ? defaultValue : Constants.EMPTY_STRING;

        List<String> list = null;
        if (choices != null && choices.length > 0) {
            list = new ArrayList<>(choices.length);
            for (String choice : choices) {
                if (choice != null && !list.contains(choice)) {
                    list.add(choice);
                }
            }

            if (!list.isEmpty() && !list.contains(defaultValue)) {
                throw new IllegalArgumentException(String.format(Locale.ROOT,
                        "Default value [%s] does not belong to choices [%s]", defaultValue, list));
            }
        }
        return new Option(name, description, defaultValue,
                list != null ? Collections.unmodifiableList(list) : Collections.emptyList());
    }

    private final String name;
    private final String description;
    private final String defaultValue;
    private final List<String> choices;

    private Option(String name, String description, String defaultValue, List<String> choices) {
        this.name = name;
        this.description = description;
        this.defaultValue = defaultValue;
        this.choices = choices;
    }

    /**
     * Gets name of the option.
     *
     * @return non-null name of the option, always in lower case
     */
    public String getName() {
        return name;
    }

    /**
     * Gets description of the option.
     *
     * @return non-null description of the option
     */
    public String getDescription() {
        return description;
    }

    /**
     * Gets default value of the option.
     *
     * @return non-null default value of the option
     */
    public String getDefaultValue() {
        return defaultValue;
    }

    /**
     * Gets value from the given properties.
     *
     * @param props properties, could be null
     * @return non-null value in properties if it's not null and valid, or same as
     *         {@link #getDefaultValue()}
     */
    public String getValue(Properties props) {
        if (props == null || props.isEmpty()) {
            return defaultValue;
        }

        String value = props.getProperty(name);
        if (value == null || (!choices.isEmpty() && !choices.contains(value))) {
            value = defaultValue;
        }
        return value;
    }

    /**
     * Gets effective default value by checking system property and environment
     * variables.
     *
     * @param prefix optional prefix of the option
     * @return non-null effective default value of the option
     */
    public String getEffectiveDefaultValue(String prefix) {
        Optional<String> value = getDefaultValueFromEnvVar(prefix);

        if (!value.isPresent() || (!choices.isEmpty() && !choices.contains(value.get()))) {
            value = getDefaultValueFromSysProp(prefix);
        }

        return !value.isPresent() || (!choices.isEmpty() && !choices.contains(value.get())) ? defaultValue
                : value.get();
    }

    /**
     * Gets choices of the option.
     *
     * @return non-null choices of the option
     */
    public String[] getChoices() {
        return choices.toArray(new String[0]);
    }

    /**
     * Updates option by creating a new one using {@link Builder}.
     *
     * @return builder to build the new option
     */
    public Builder update() {
        return new Builder(this);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = prime + name.hashCode();
        result = prime * result + description.hashCode();
        result = prime * result + defaultValue.hashCode();
        result = prime * result + choices.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        Option other = (Option) obj;
        return name.equals(other.name) && description.equals(other.description)
                && defaultValue.equals(other.defaultValue)
                && choices.equals(other.choices);
    }
}
