/*
 * Copyright 2022-2026, Zhichun Wu
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
package io.github.jdbcx.executor;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import io.github.jdbcx.Checker;
import io.github.jdbcx.Constants;
import io.github.jdbcx.Field;
import io.github.jdbcx.Option;
import io.github.jdbcx.ResourceManager;
import io.github.jdbcx.Result;
import io.github.jdbcx.Row;
import io.github.jdbcx.Utils;
import io.github.jdbcx.ValueFactory;
import io.github.jdbcx.VariableTag;

public class McpExecutor extends AbstractExecutor {
    private static final List<Field> dryRunFields = Collections
            .unmodifiableList(Arrays.asList(FIELD_QUERY, FIELD_TIMEOUT_MS, FIELD_OPTIONS));

    public static final Option OPTION_INIT_TIMEOUT = Option.ofInt("init.timeout",
            "MCP client initialization timeout in milliseconds.", 20000);

    public static final Option OPTION_SERVER_CONF = Option.ofOptional("conf",
            "MCP server configuration represented in a JSON object");

    public static final Option OPTION_SERVER_PROMPT = Option.ofOptional("prompt", "MCP server prompt name");
    public static final Option OPTION_SERVER_RESOURCE = Option.ofOptional("resource", "MCP server resource URI");
    public static final Option OPTION_SERVER_TOOL = Option.ofOptional("tool", "MCP server tool to use");

    public static final Option OPTION_SERVER_TARGET = Option.ofEnum("target", "MCP server target",
            McpServerTarget.info.name(), McpServerTarget.class);
    public static final Option OPTION_SERVER_CMD = Option.ofOptional("cmd", "MCP server command");
    public static final Option OPTION_SERVER_ARGS = Option.ofOptional("args",
            "Whitespace separted MCP server command line arguments");
    public static final Option OPTION_SERVER_ENV = Option.ofOptional("env",
            "Semicolon seperated environment variables for local MCP server");
    public static final Option OPTION_SERVER_KEY = Option.ofOptional("key", "MCP server API key");
    public static final Option OPTION_SERVER_URL = Option.ofOptional("url", "MCP server URL");

    protected final int defaultInitTimeout;
    protected final String defaultServerCmd;
    protected final List<String> defaultServerArgs;
    protected final Map<String, String> defaultServerEnv;
    protected final String defaultServerKey;
    protected final String defaultServerUrl;

    protected final String defaultServerPrompt;
    protected final String defaultServerResource;
    protected final String defaultServerTool;
    protected final McpServerTarget defaultServerTarget;

    public McpExecutor(VariableTag tag, Properties props) {
        super(tag, props);

        this.defaultInitTimeout = Integer.parseInt(OPTION_INIT_TIMEOUT.getValue(props));
        if (props == null) {
            this.defaultServerCmd = Constants.EMPTY_STRING;
            this.defaultServerArgs = Collections.emptyList();
            this.defaultServerEnv = Collections.emptyMap();
            this.defaultServerKey = Constants.EMPTY_STRING;
            this.defaultServerUrl = Constants.EMPTY_STRING;

            this.defaultServerPrompt = Constants.EMPTY_STRING;
            this.defaultServerResource = Constants.EMPTY_STRING;
            this.defaultServerTool = Constants.EMPTY_STRING;
            this.defaultServerTarget = McpServerTarget.valueOf(OPTION_SERVER_TARGET.getDefaultValue());
        } else {
            this.defaultServerPrompt = OPTION_SERVER_PROMPT.getValue(props);
            this.defaultServerResource = OPTION_SERVER_RESOURCE.getValue(props);
            this.defaultServerTool = OPTION_SERVER_TOOL.getValue(props);
            this.defaultServerTarget = McpServerTarget.valueOf(OPTION_SERVER_TARGET.getValue(props));

            String jsonConf = OPTION_SERVER_CONF.getValue(props);
            if (Checker.isNullOrEmpty(jsonConf)) {
                this.defaultServerCmd = OPTION_SERVER_CMD.getValue(props);
                this.defaultServerArgs = Utils.split(OPTION_SERVER_ARGS.getValue(props), ' ', true, true, false);
                this.defaultServerEnv = Utils.toKeyValuePairs(OPTION_SERVER_ENV.getValue(props), ';', false);
                this.defaultServerKey = OPTION_SERVER_KEY.getValue(props);
                this.defaultServerUrl = OPTION_SERVER_URL.getValue(props);
            } else {
                JsonObject obj = ValueFactory.fromJson(jsonConf, JsonObject.class);
                JsonPrimitive e = obj.getAsJsonPrimitive("command");
                if (e != null) {
                    this.defaultServerCmd = e.getAsString();
                    this.defaultServerKey = Constants.EMPTY_STRING;
                    this.defaultServerUrl = Constants.EMPTY_STRING;

                    JsonElement args = obj.get("args");
                    if (args == null) {
                        this.defaultServerArgs = Collections.emptyList();
                    } else if (args instanceof JsonArray) {
                        JsonArray arr = args.getAsJsonArray();
                        List<String> list = new ArrayList<>(arr.size());
                        for (JsonElement element : arr) {
                            list.add(element.getAsString());
                        }
                        this.defaultServerArgs = Collections.unmodifiableList(list);
                    } else {
                        this.defaultServerArgs = Utils.split(args.getAsString(), ' ', true, true, false);
                    }
                    args = obj.get("env");
                    if (args == null) {
                        this.defaultServerEnv = Collections.emptyMap();
                    } else if (args instanceof JsonObject) {
                        JsonObject o = args.getAsJsonObject();
                        Map<String, String> map = new HashMap<>();
                        for (Map.Entry<String, JsonElement> entry : o.entrySet()) {
                            map.put(entry.getKey(), entry.getValue().getAsString());
                        }
                        this.defaultServerEnv = Collections.unmodifiableMap(map);
                    } else {
                        this.defaultServerEnv = Utils.toKeyValuePairs(args.getAsString(), ';', false);
                    }
                } else {
                    e = obj.getAsJsonPrimitive("url");
                    if (e == null) {
                        throw new IllegalArgumentException(
                                "Either command or url must be specified in MCP server configuration");
                    } else {
                        this.defaultServerUrl = e.getAsString();
                        this.defaultServerCmd = Constants.EMPTY_STRING;
                        this.defaultServerArgs = Collections.emptyList();
                        this.defaultServerEnv = Collections.emptyMap();
                    }
                    e = obj.getAsJsonPrimitive("key");
                    this.defaultServerKey = e != null ? e.getAsString() : Constants.EMPTY_STRING;
                }
            }
        }
    }

    public int getInitTimeout(Properties props) {
        String value = props != null ? props.getProperty(OPTION_INIT_TIMEOUT.getName()) : null;
        return value != null ? Integer.parseInt(value) : defaultInitTimeout;
    }

    public String getServerPrompt(Properties props) {
        String value = props != null ? props.getProperty(OPTION_SERVER_PROMPT.getName()) : null;
        return value != null ? value : defaultServerPrompt;
    }

    public String getServerResource(Properties props) {
        String value = props != null ? props.getProperty(OPTION_SERVER_RESOURCE.getName()) : null;
        return value != null ? value : defaultServerResource;
    }

    public String getServerTool(Properties props) {
        String value = props != null ? props.getProperty(OPTION_SERVER_TOOL.getName()) : null;
        return value != null ? value : defaultServerTool;
    }

    public McpServerTarget getServerTarget(Properties props) {
        String value = props != null ? props.getProperty(OPTION_SERVER_TARGET.getName()) : null;
        return value != null ? McpServerTarget.valueOf(value.toLowerCase()) : defaultServerTarget;
    }

    public String getServerCommand(Properties props) {
        String value = props != null ? props.getProperty(OPTION_SERVER_CMD.getName()) : null;
        return value != null ? value : defaultServerCmd;
    }

    public List<String> getServerArguments(Properties props) {
        String value = props != null ? props.getProperty(OPTION_SERVER_ARGS.getName()) : null;
        return value != null ? Utils.split(value, ' ', true, true, false) : defaultServerArgs;
    }

    public Map<String, String> getServerEnvironment(Properties props) {
        String value = props != null ? props.getProperty(OPTION_SERVER_ENV.getName()) : null;
        return value != null ? Utils.toKeyValuePairs(value, ';', false) : defaultServerEnv;
    }

    public String getServerKey(Properties props) {
        String value = props != null ? props.getProperty(OPTION_SERVER_KEY.getName()) : null;
        return value != null ? value : defaultServerKey;
    }

    public String getServerUrl(Properties props) {
        String value = props != null ? props.getProperty(OPTION_SERVER_URL.getName()) : null;
        return value != null ? value : defaultServerUrl;
    }

    public Result<?> execute(String query, Properties props, ResourceManager resourceManager) throws SQLException { // NOSONAR
        final int timeout = getTimeout(props);
        if (getDryRun(props)) {
            return Result.of(Row.of(dryRunFields, new Object[] { query, timeout, props }));
        }

        return new McpSupport(this).execute(query, props, resourceManager);
    }
}
