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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Properties;

/**
 * This class defines an immutable configuration option, which consists of a
 * key, a default value in String format, a description, and valid choices.
 */
public final class Option implements Serializable {
    private static final long serialVersionUID = -2250888694424064713L;

    public static final String PROPERTY_JDBCX;
    public static final String PROPERTY_PREFIX;

    static {
        final Option prefixOption = Option
                .of(new String[] { "jdbcx.prefix", "Prefix used by all JDBCX configuration properties.", "jdbcx" });

        PROPERTY_JDBCX = prefixOption.getEffectiveDefaultValue(null).toLowerCase(Locale.ROOT);
        PROPERTY_PREFIX = PROPERTY_JDBCX + ".";

        if (Option.PROPERTY_JDBCX.isEmpty() || "jdbc".equals(Option.PROPERTY_JDBCX)) {
            throw new IllegalStateException(Utils.format(
                    "The JDBCX prefix must be a non-empty string not equal to \"jdbc\". "
                            + "Please change the prefix by setting \"%s\" system property or \"%s\" environment variable.",
                    prefixOption.getSystemProperty(null),
                    prefixOption.getEnvironmentVariable(null)));
        }
    }

    /**
     * One of suggested choices for error handling, which simply ignores the error.
     */
    public static final String ERROR_HANDLING_IGNORE = "ignore";
    /**
     * One of suggested choices for error handling, which throws out an exception
     * for the error.
     */
    public static final String ERROR_HANDLING_THROW = "throw";
    /**
     * One of suggested choices for error handling, which takes the error as a
     * warning.
     */
    public static final String ERROR_HANDLING_WARN = "warn";
    /**
     * One of suggested choices for error handling, which returns the error as
     * result.
     */
    public static final String ERROR_HANDLING_RETURN = "return";

    // most common options
    public static final Option ID = Option.of(new String[] { "id", "ID for looking up configuration" });
    public static final Option CLASSPATH = Option
            .of(new String[] { "classpath", "Comma separated classpath for loading classes" });

    public static final Option PRE_QUERY = Option
            .of(new String[] { "pre.query", "Query must execute before the current one" });
    public static final Option POST_QUERY = Option
            .of(new String[] { "post.query", "Query must execute right after the current one" });

    /**
     * Path to config property file.
     */
    public static final Option CONFIG_PATH = Option
            .of(new String[] { "config.path", "The path to the configuration file to load at startup.",
                    "~/.jdbcx/config.properties" });
    /**
     * Custom classpath for {@link ExpandedUrlClassLoader} to load classes.
     */
    public static final Option CUSTOM_CLASSPATH = Option.of(new String[] { "custom.classpath", "Custom classpath" });

    /**
     * Whether to perform a dry run.
     */
    public static final Option EXEC_DRYRUN = Option
            .of(new String[] { "exec.dryrun", "Whether to perform a dry run without actual execution.",
                    Constants.FALSE_EXPR, Constants.TRUE_EXPR });
    /**
     * The error handling approach to use when an execution fails.
     */
    public static final Option EXEC_ERROR = Option
            .of(new String[] { "exec.error", "The error handling strategy to use when execution fails.",
                    ERROR_HANDLING_WARN, ERROR_HANDLING_IGNORE, ERROR_HANDLING_RETURN, ERROR_HANDLING_THROW });
    /**
     * The maximum number of threads that can be used for query execution. 0
     * enforces sequential execution.
     */
    public static final Option EXEC_PARALLELISM = Option.of(new String[] { "exec.parallelism",
            "The maximum number of threads to use for parallel query execution. 0 enforces sequential, single-threaded execution.",
            "0" });
    /**
     * The priority for executing the query.
     */
    public static final Option EXEC_PRIORITY = Option
            .of(new String[] { "exec.priority",
                    "Priority for query execution. Higher integer values indicate higher priority.", "5" });
    /**
     * The execution timeout in milliseconds.
     */
    public static final Option EXEC_TIMEOUT = Option.of("exec.timeout",
            "Execution timeout in milliseconds. A negative value or 0 disables the timeout.", "0");

    public static final Option WORK_DIRECTORY = Option.of("work.dir",
            "Path to the working directory. If left empty, the current directory will be used.", "");

    public static final Option INPUT_FILE = Option
            .of(new String[] { "input.file",
                    "Path to an input file containing the query text to execute. This property is only used if no inline query content is provided" });
    public static final Option OUTPUT_FILE = Option
            .of(new String[] { "output.file",
                    "Path to an output file for writing query results. If the file already exists, it will be overwritten without warning." });
    public static final Option INPUT_CHARSET = Option.of("input.charset",
            "Character encoding to use when reading the input stream.", Constants.DEFAULT_CHARSET.name());
    public static final Option OUTPUT_CHARSET = Option.of("output.charset",
            "Character encoding to use when writing the output stream.", Constants.DEFAULT_CHARSET.name());
    /**
     * Proxy to use.
     */
    public static final Option PROXY = Option.of(new String[] { "proxy",
            "Optional proxy server to use for network connections. If empty, no proxy will be used and connections will be made directly." });

    public static final Option RESULT_FORMAT = Option
            .of(new String[] { "result.format", "Output format to use for query results." });
    public static final Option RESULT_VAR = Option
            .of(new String[] { "result.var", "Unique variable name of the query result." });
    public static final Option RESULT_REUSE = Option.of(new String[] { "result.reuse",
            "Whether to reuse previous result by retrieving the variable from context, only works when result.var is specified.",
            Constants.TRUE_EXPR, Constants.FALSE_EXPR });
    public static final Option RESULT_SCOPE = Option
            .of(new String[] { "result.scope", "Scope of the result, only works when result.var is specified.",
                    Constants.SCOPE_QUERY, Constants.SCOPE_THREAD, Constants.SCOPE_GLOBAL });
    public static final Option RESULT_JSON_PATH = Option
            .of(new String[] { "result.json.path",
                    "The JSON path to extract a value from the JSON query results. The extracted value will be converted to a string and escaped." });
    public static final Option RESULT_STRING_ESCAPE = Option
            .of(new String[] { "result.string.escape",
                    "Whether to escape single quotes and special characters in string query results.",
                    Constants.FALSE_EXPR, Constants.TRUE_EXPR });
    public static final Option RESULT_STRING_ESCAPE_CHAR = Option
            .of(new String[] { "result.string.escape.char", "The character that will be used for escaping.", "\\" });
    public static final Option RESULT_STRING_ESCAPE_TARGET = Option
            .of(new String[] { "result.string.escape.target",
                    "The target character that will be escaped in string query results.", "'" });
    public static final Option RESULT_STRING_REPLACE = Option
            .of(new String[] { "result.string.replace", "Whether to perform variable substitution in the query result.",
                    Constants.FALSE_EXPR, Constants.TRUE_EXPR });
    public static final Option RESULT_STRING_TRIM = Option
            .of(new String[] { "result.string.trim", "Whether to trim whitespace from the query result.",
                    Constants.FALSE_EXPR, Constants.TRUE_EXPR });
    public static final Option RESULT_STRING_SPLIT = Option
            .of(new String[] { "result.string.split", "Whether to split the query result string into a list.",
                    Constants.FALSE_EXPR, Constants.TRUE_EXPR });
    public static final Option RESULT_STRING_SPLIT_CHAR = Option
            .of(new String[] { "result.string.split.char",
                    "The delimiter character(s) to use when splitting the query result.", "\n" });
    public static final Option RESULT_STRING_SPLIT_BLANK = Option
            .of(new String[] { "result.string.line.blank",
                    "Specifies how to handle blank lines in a multi-line query result.", ERROR_HANDLING_IGNORE,
                    ERROR_HANDLING_RETURN });
    public static final Option RESULT_TABLE = Option
            .of(new String[] { "result.table",
                    "The name of the database table to save the query results to. The table will be automatically created if it does not already exist." });
    /**
     * The result type, either string, json or stream.
     */
    public static final Option RESULT_TYPE = Option
            .of(new String[] { "result.type", "The result type, either string, json or stream.", "string", "json",
                    "stream" });

    public static final Option SERVER_AUTH = Option.of(new String[] { "server.auth",
            "Whether to enable server-side authentication, which requires bearer token for submitting query for execution",
            Constants.FALSE_EXPR, Constants.TRUE_EXPR });
    public static final Option SERVER_URL = Option
            .of(new String[] { "server.url", "Bridge server URL used for executing the query remotely." });
    public static final Option SERVER_HOST = Option.of(new String[] { "server.host", "Bridge server host." });
    public static final Option SERVER_PORT = Option.of("server.port", "Bridge server port.", "8080");
    public static final Option SERVER_CONTEXT = Option.of("server.context",
            "Server web context starts and ends with backslash", "/");
    public static final Option SERVER_TOKEN = Option.of(new String[] { "server.token",
            "Token required to access bridge server with authentication and authorization enabled." });
    public static final Option SERVER_FORMAT = Option.ofEnum("server.format", "Server response format", null,
            Format.class);
    public static final Option SERVER_COMPRESSION = Option.ofEnum("server.compress", "Server response compression",
            null, Compression.class);

    public static final Option SSL_CERT = Option.of(new String[] { "ssl.cert",
            "Path to the client SSL/TLS certificate file to use for authenticated requests." });
    public static final Option SSL_CERT_TYPE = Option
            .of(new String[] { "ssl.cert.type", "The type of certificate to use for SSL/TLS connections.", "X.509" });
    public static final Option SSL_KEY = Option
            .of(new String[] { "ssl.key", "Passphrase to decrypt the private key for the SSL/TLS certificate." });
    public static final Option SSL_KEY_ALGORITHM = Option
            .of(new String[] { "ssl.key.algorithm",
                    "Algorithm used to generate the private key for the SSL/TLS certificate.", "RSA" });
    public static final Option SSL_MODE = Option.of(
            new String[] { "ssl.mode", "SSL/TLS authentication mode to use for HTTPS connections.", "strict", "none" });
    public static final Option SSL_PROTOCOL = Option
            .of(new String[] { "ssl.protocol", "SSL/TLS protocol to use.", "TLS" });
    public static final Option SSL_ROOT_CERT = Option
            .of(new String[] { "ssl.root.cert", "Path to the trusted root SSL/TLS certificate file." });

    public static final Option TAG = Option.ofEnum("tag",
            "Variable tags used in SQL templating and dynamic queries.", null, VariableTag.class);

    /**
     * The Builder class is used to construct options.
     */
    public static final class Builder {
        private String name;
        private String description;
        private String defaultValue;
        private List<String> choices;

        private final Option template;

        Builder() {
            this.template = null;
        }

        Builder(Option template) {
            this.name = template.name;
            this.description = template.description;
            this.defaultValue = template.defaultValue;
            this.choices = new ArrayList<>(template.choices);

            this.template = template;
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
            if (choices == null) {
                choices = Collections.emptyList();
            }
            if (template != null && template.name.equals(name) && template.description.equals(description)
                    && template.defaultValue.equals(defaultValue) && template.choices.equals(choices)) {
                return template;
            }
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
     * @param clazz        optional enum class
     * @return option
     */
    public static Option ofEnum(String name, String description, String defaultValue, Class<? extends Enum<?>> clazz) {
        final String[] arr;
        if (clazz != null) {
            Enum<?>[] choices = clazz.getEnumConstants();
            int len = choices.length;
            arr = new String[len];
            for (int i = 0; i < len; i++) {
                arr[i] = choices[i].name();
            }
        } else {
            arr = Constants.EMPTY_STRING_ARRAY;
        }

        return of(name, description, defaultValue, arr);
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
                // FIXME or throw exception when defaultValue does not exist in choices?
                defaultValue = list.get(0);
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
     * Gets name with prefix of {@link #PROPERTY_PREFIX}.
     *
     * @return non-null name with prefix
     */
    public String getJdbcxName() {
        return PROPERTY_PREFIX.concat(name);
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
     * @param props properties, could be {@code null}
     * @return non-null value in properties if it's not null and valid, or same as
     *         {@link #getDefaultValue()}
     */
    public String getValue(Properties props) {
        return getValue(null, props, null, true);
    }

    /**
     * Gets value from the given properties along with preferred default value.
     *
     * @param props        properties, may or may not be null
     * @param defaultValue preferred default value, fallback to
     *                     {@link #getDefaultValue()} when it's {@code null}
     * @return non-null value
     */
    public String getValue(Properties props, String defaultValue) {
        return getValue(null, props, defaultValue, true);
    }

    /**
     * Gets value using {@link #PROPERTY_PREFIX}. Same as
     * {@code getValue(PROPERTY_PREFIX, props, null, true)}.
     * 
     * @param props properties
     * @return non-null value
     */
    public String getJdbcxValue(Properties props) {
        return getValue(PROPERTY_PREFIX, props, null, true);
    }

    /**
     * Gets value from the given {@link Properties}.
     *
     * @param prefix       optional prefix, usually same as {@link #PROPERTY_PREFIX}
     * @param props        properties
     * @param defaultValue optional default value
     * @param validate     whether to ensure the value is a valid choice
     * @return non-null value
     */
    public String getValue(String prefix, Properties props, String defaultValue, boolean validate) {
        if (props == null) {
            return defaultValue != null ? defaultValue : this.defaultValue;
        }

        String value = props.getProperty(getSystemProperty(prefix));
        if (value == null || (validate && !choices.isEmpty() && !choices.contains(value))) {
            value = defaultValue != null ? defaultValue : this.defaultValue;
        }
        return value;
    }

    public void setDefaultValueIfNotPresent(Properties props) {
        setValueIfNotPresent(props, null);
    }

    public void setValueIfNotPresent(Properties props, String value) {
        if (props != null && props.getProperty(name) == null) {
            props.setProperty(name, value != null ? value : defaultValue);
        }
    }

    /**
     * Sets value in the given properties. Same as
     * {@code setValue(props, getDefaultValue())}.
     *
     * @param props properties, may or may not be null
     * @return the overrided value, null means it did not exist
     */
    public String setValue(Properties props) {
        return setValue(null, props, null);
    }

    /**
     * Sets value in the given properties.
     *
     * @param props properties, may or may not be null
     * @param value preferred value, fallback to {@link #getDefaultValue()} when
     *              it's {@code null} or not a valid choice
     * @return the overrided value, null means it did not exist
     */
    public String setValue(Properties props, String value) {
        return setValue(null, props, value);
    }

    public String setJdbcxValue(Properties props, String value) {
        return setValue(PROPERTY_PREFIX, props, value);
    }

    public String setValue(String prefix, Properties props, String value) {
        if (props == null) {
            return null;
        } else if (value == null || (!choices.isEmpty() && !choices.contains(value))) {
            value = defaultValue;
        }

        return (String) (Checker.isNullOrEmpty(prefix) ? props.setProperty(name, value)
                : props.setProperty(prefix.toLowerCase(Locale.ROOT).concat(name), value));
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
