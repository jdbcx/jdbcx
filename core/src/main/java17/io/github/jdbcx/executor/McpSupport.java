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
package io.github.jdbcx.executor;

import java.lang.reflect.Type;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;

import io.github.jdbcx.Checker;
import io.github.jdbcx.Constants;
import io.github.jdbcx.Field;
import io.github.jdbcx.Format;
import io.github.jdbcx.Logger;
import io.github.jdbcx.LoggerFactory;
import io.github.jdbcx.ResourceManager;
import io.github.jdbcx.Result;
import io.github.jdbcx.Row;
import io.github.jdbcx.Utils;
import io.github.jdbcx.Value;
import io.github.jdbcx.ValueFactory;
import io.github.jdbcx.value.DoubleValue;
import io.github.jdbcx.value.StringValue;
import io.modelcontextprotocol.client.McpClient;
import io.modelcontextprotocol.client.McpSyncClient;
import io.modelcontextprotocol.client.transport.HttpClientSseClientTransport;
import io.modelcontextprotocol.client.transport.ServerParameters;
import io.modelcontextprotocol.client.transport.StdioClientTransport;
import io.modelcontextprotocol.json.McpJsonMapper;
import io.modelcontextprotocol.spec.McpClientTransport;
import io.modelcontextprotocol.spec.McpError;
import io.modelcontextprotocol.spec.McpSchema.BlobResourceContents;
import io.modelcontextprotocol.spec.McpSchema.CallToolRequest;
import io.modelcontextprotocol.spec.McpSchema.CallToolResult;
import io.modelcontextprotocol.spec.McpSchema.ClientCapabilities;
import io.modelcontextprotocol.spec.McpSchema.Content;
import io.modelcontextprotocol.spec.McpSchema.EmbeddedResource;
import io.modelcontextprotocol.spec.McpSchema.GetPromptRequest;
import io.modelcontextprotocol.spec.McpSchema.ImageContent;
import io.modelcontextprotocol.spec.McpSchema.Implementation;
import io.modelcontextprotocol.spec.McpSchema.InitializeResult;
import io.modelcontextprotocol.spec.McpSchema.ReadResourceRequest;
import io.modelcontextprotocol.spec.McpSchema.ServerCapabilities;
import io.modelcontextprotocol.spec.McpSchema.TextContent;
import io.modelcontextprotocol.spec.McpSchema.TextResourceContents;

public final class McpSupport {
    private static final Logger log = LoggerFactory.getLogger(McpSupport.class);
    private static final Type MAP_TYPE=new TypeToken<Map<String,Object>>(){}.getType();

    private static final Field FIELD_CONTENT = Field.of("content");
    private static final Field FIELD_NAME = Field.of("name");
    private static final Field FIELD_TYPE = Field.of("type");
    private static final Field FIELD_DESC = Field.of("description");
    private static final Field FIELD_MIME_TYPE = Field.of("mimeType");
    private static final Field FIELD_URI = Field.of("uri");
    private static final Field FIELD_VERSION = Field.of("version");

    private static final List<Field> capabilityFields = Collections
            .unmodifiableList(Arrays.asList(FIELD_TYPE, Field.of("experimental"), Field.of("logging"),
                    Field.of("prompts"), Field.of("resources"), Field.of("roots"), Field.of("sampling"),
                    Field.of("tools")));
    private static final List<Field> infoFields = Collections
            .unmodifiableList(Arrays.asList(FIELD_TYPE, FIELD_NAME, FIELD_VERSION));
    private static final List<Field> promptFields = Collections
            .unmodifiableList(Arrays.asList(FIELD_NAME, FIELD_DESC, Field.of("arguments")));
    private static final List<Field> resourceFields = Collections
            .unmodifiableList(
                    Arrays.asList(FIELD_NAME, FIELD_DESC, FIELD_URI, FIELD_MIME_TYPE, Field.of("annotations")));
    private static final List<Field> resourceTemplateFields = Collections
            .unmodifiableList(Arrays.asList(FIELD_NAME, FIELD_DESC, Field.of("uriTemplate"), FIELD_MIME_TYPE,
                    Field.of("annotations")));
    private static final List<Field> toolFields = Collections
            .unmodifiableList(Arrays.asList(FIELD_NAME, FIELD_DESC, Field.of("inputSchema")));

    private static final List<Field> promptRespondFields = Collections
            .unmodifiableList(
                    Arrays.asList(Field.of("role"), Field.of("contentType"), FIELD_MIME_TYPE, Field.of("priority"),
                            Field.of("audience"), FIELD_CONTENT));
    private static final List<Field> resourceRespondFields = Collections
            .unmodifiableList(Arrays.asList(FIELD_URI, FIELD_MIME_TYPE, FIELD_CONTENT));
    private static final List<Field> toolRespondFields = Collections
            .unmodifiableList(Arrays.asList(Field.of("contentType"), FIELD_MIME_TYPE, Field.of("priority"),
                    Field.of("audience"), FIELD_CONTENT));

    static final String STR_CLIENT = "client";
    static final String STR_SERVER = "server";

    static final record ManagedSession(String cli, String url, McpSyncClient client) implements AutoCloseable {

    @Override
    public void close() {
        client.close();
    }

    }

    static final void addContentAsValues(Content obj, List<Value> values) {
        if (obj instanceof TextContent) { // NOSONAR
            final TextContent content = (TextContent) obj;
            values.add(StringValue.of(content.type()));
            values.add(StringValue.of(Format.TXT.mimeType()));
            values.add(DoubleValue.of(content.priority()));
            values.add(StringValue.ofJson(content.audience()));
            values.add(StringValue.of(content.text()));
        } else if (obj instanceof ImageContent) { // NOSONAR
            final ImageContent content = (ImageContent) obj;
            values.add(StringValue.of(content.type()));
            values.add(StringValue.of(content.mimeType()));
            values.add(DoubleValue.of(content.priority()));
            values.add(StringValue.ofJson(content.audience()));
            values.add(StringValue.of(content.data()));
        } else if (obj instanceof EmbeddedResource) { // NOSONAR
            final EmbeddedResource content = (EmbeddedResource) obj;
            values.add(StringValue.of(content.type()));
            if (content.resource() == null) {
                values.add(StringValue.of(Constants.EMPTY_STRING));
                values.add(DoubleValue.of(content.priority()));
                values.add(StringValue.ofJson(content.audience()));
                values.add(StringValue.of(Constants.EMPTY_STRING));
            } else {
                values.add(StringValue.of(content.resource().mimeType()));
                values.add(DoubleValue.of(content.priority()));
                values.add(StringValue.ofJson(content.audience()));
                values.add(StringValue.of(content.resource().uri()));
            }
        } else {
            throw new IllegalArgumentException("Unsupported content: " + obj);
        }
    }

    static final Result<?> getPrompt(McpSyncClient client, String prompt, String query) throws SQLException { // NOSONAR
        final String normalized = query.trim();
        final GetPromptRequest request;
        if (normalized.length() > 1 && normalized.charAt(0) == '{'
                && normalized.charAt(normalized.length() - 1) == '}') {
            if (prompt.isEmpty()) {
                JsonObject obj = ValueFactory.fromJson(normalized, JsonObject.class);
                JsonElement e = obj.get("name");
                if (e != null && e.isJsonPrimitive()) {
                    prompt = e.getAsString();
                }
                Map<String, Object> args = new HashMap<>();
                e = obj.get("arguments");
                if (e != null && e.isJsonObject()) {
                    for (Map.Entry<String, JsonElement> entry : e.getAsJsonObject().entrySet()) {
                        e = entry.getValue();
                        if (e != null && e.isJsonPrimitive()) {
                            args.put(entry.getKey(), e.getAsString());
                        }
                    }
                }
                request = new GetPromptRequest(prompt, args);
            } else {
                request = new GetPromptRequest(prompt, ValueFactory.fromJson(query, MAP_TYPE));
            }
        } else if (!normalized.isEmpty()) {
            request = new GetPromptRequest(normalized, Collections.emptyMap());
        } else if (!prompt.isEmpty()) {
            request = new GetPromptRequest(prompt, Collections.emptyMap());
        } else {
            throw new SQLException("Prompt name is not specified");
        }
        return Result.of(promptRespondFields, client.getPrompt(request).messages(),
                (fields, obj) -> {
                    final List<Value> values = new ArrayList<>(fields.size());
                    values.add(StringValue.of(obj.role()));
                    addContentAsValues(obj.content(), values);
                    return Row.of(fields, values.toArray(new Value[0]));
                });
    }

    static final Result<?> getResource(McpSyncClient client, String resource, String query) throws SQLException { // NOSONAR
        final String normalized = query.trim();
        if (normalized.length() > 1 && normalized.charAt(0) == '{'
                && normalized.charAt(normalized.length() - 1) == '}') {
            JsonObject obj = ValueFactory.fromJson(normalized, JsonObject.class);
            JsonElement uri = obj.get("uri");
            if (uri != null && uri.isJsonPrimitive()) {
                resource = uri.getAsString();
            }
        } else if (!normalized.isEmpty()) {
            resource = normalized;
        }

        if (resource.isEmpty()) {
            throw new SQLException("Resource URI is not specified");
        }
        return Result.of(resourceRespondFields, client.readResource(new ReadResourceRequest(resource)).contents(),
                (fields, obj) -> {
                    final List<Value> values = new ArrayList<>(fields.size());
                    values.add(StringValue.of(obj.uri()));
                    values.add(StringValue.of(obj.mimeType()));
                    if (obj instanceof TextResourceContents) { // NOSONAR
                        TextResourceContents content = (TextResourceContents) obj;
                        values.add(StringValue.of(content.text()));
                    } else if (obj instanceof BlobResourceContents) { // NOSONAR
                        BlobResourceContents content = (BlobResourceContents) obj;
                        values.add(StringValue.of(content.blob()));
                    }
                    return Row.of(fields, values.toArray(new Value[0]));
                });
    }

    static final Result<?> useTool(McpSyncClient client, String tool, String query) throws SQLException { // NOSONAR
        String normalized = query.trim();
        final Map<String, Object> args;
        if (normalized.length() > 1 && normalized.charAt(0) == '{'
                && normalized.charAt(normalized.length() - 1) == '}') {
            args = ValueFactory.fromJson(query, MAP_TYPE);
        } else {
            args = Collections.emptyMap();
        }

        final CallToolResult response = client.callTool(new CallToolRequest(tool, args));
        if (Boolean.TRUE.equals(response.isError())) {
            StringBuilder builder = new StringBuilder();
            if (response.content() != null) {
                for (Content content : response.content()) {
                    if (content instanceof TextContent) { // NOSONAR
                        String text = ((TextContent) content).text();
                        if (!Checker.isNullOrEmpty(text)) {
                            builder.append('\n').append(text);
                        }
                    }
                }
            }
            throw new SQLException(
                    builder.length() > 0 ? builder.substring(1)
                            : Utils.format("Unknown error executing tool %s.", tool));
        }

        return Result.of(toolRespondFields, response.content(),
                (fields, obj) -> {
                    final List<Value> values = new ArrayList<>(fields.size());
                    addContentAsValues(obj, values);
                    return Row.of(fields, values.toArray(new Value[0]));
                });
    }

    private final McpExecutor executor;

    McpSupport(McpExecutor executor) {
        this.executor = executor;
    }

    McpSyncClient newClient(McpClientTransport transport, int initTimeout, int timeout) throws SQLException {
        final McpSyncClient client = McpClient.sync(transport).initializationTimeout(Duration.ofMillis(initTimeout))
                .requestTimeout(Duration.ofMillis(timeout <= 0 ? initTimeout : timeout)).loggingConsumer(log::debug)
                .build();
        InitializeResult initResult = null;
        try {
            initResult = client.initialize();
            log.debug("MCP client (protocol=%s) initialized successfully, server is %s", initResult.protocolVersion(),
                    initResult.serverInfo());
        } catch (McpError e) {
            throw new SQLException(e);
        } finally {
            if (initResult == null) {
                client.close();
            }
        }
        return client;
    }

    public Result<?> execute(String query, Properties props, ResourceManager resourceManager) throws SQLException { // NOSONAR
        final int timeout = executor.getTimeout(props);
        final McpClientTransport transport;
        final String serverCmd = executor.getServerCommand(props);
        final String serverUrl = executor.getServerUrl(props);
        final String serverCli;
        if (!Checker.isNullOrEmpty(serverCmd)) {
            String cmd = serverCmd;
            List<String> args = executor.getServerArguments(props);
            if (Constants.IS_WINDOWS) {
                boolean customized = false;
                for (String c : CommandLineExecutor.WIN_CMDS) {
                    if (c.equalsIgnoreCase(serverCmd)) {
                        customized = true;
                        break;
                    }
                }
                if (!customized) {
                    cmd = CommandLineExecutor.WIN_CMDS.get(0);
                    List<String> list = new LinkedList<>();
                    list.add("/c");
                    list.add(serverCmd);
                    list.addAll(args);
                    args = Collections.unmodifiableList(list);
                }
            }
            serverCli = new StringBuilder(cmd).append(' ').append(args.toString()).toString();
            transport = new StdioClientTransport(
                    ServerParameters.builder(cmd).args(args).env(executor.getServerEnvironment(props)).build(),
                    McpJsonMapper.getDefault());
        } else if (!Checker.isNullOrEmpty(serverUrl)) {
            serverCli = Constants.EMPTY_STRING;
            final String serverKey = executor.getServerKey(props);
            final String[] parts = Utils.splitUrl(serverUrl); // base url + sse path
            final HttpClientSseClientTransport.Builder builder = (!parts[1].isEmpty()
                    ? HttpClientSseClientTransport.builder(parts[0]).sseEndpoint(parts[1])
                    : HttpClientSseClientTransport.builder(serverUrl));
            if (Checker.isNullOrEmpty(serverKey)) {
                transport = builder.build();
            } else {
                transport = builder.customizeRequest(b -> b.header(WebExecutor.HEADER_AUTHORIZATION,
                        WebExecutor.AUTH_SCHEME_BEARER + serverKey))
                        .build();
            }
        } else {
            throw new SQLException(
                    Utils.format("Either %s or %s must be specified", McpExecutor.OPTION_SERVER_CMD.getName(),
                            McpExecutor.OPTION_SERVER_URL.getName()));
        }

        int initTimeout = executor.getInitTimeout(props);
        if (initTimeout <= 0) {
            initTimeout = Integer.parseInt(McpExecutor.OPTION_INIT_TIMEOUT.getDefaultValue());
        }
        final Result<?> result;
        final McpSyncClient client;
        final ManagedSession session;
        if (resourceManager != null) {
            final ManagedSession initialized = resourceManager.get(ManagedSession.class,
                    s -> serverCli.equals(s.cli) || serverUrl.equals(s.url));
            if (initialized != null) {
                client = initialized.client();
                session = initialized;
            } else {
                client = newClient(transport, initTimeout, timeout);
                final ManagedSession newSession = resourceManager
                        .add(new ManagedSession(serverCli, serverUrl, client));
                session = resourceManager.get(ManagedSession.class, r -> r == newSession);
            }
        } else {
            client = newClient(transport, initTimeout, timeout);
            session = null;
        }
        try {
            final McpServerTarget target = executor.getServerTarget(props);
            final String prompt = executor.getServerPrompt(props);
            final String resource = executor.getServerResource(props);
            final String tool = executor.getServerTool(props);
            final String request = executor.loadInputFile(props, query);
            final boolean hasRequest = !Checker.isNullOrBlank(request);

            if (!Checker.isNullOrEmpty(prompt) || (target == McpServerTarget.prompt && hasRequest)) {
                result = getPrompt(client, prompt, request);
            } else if (!Checker.isNullOrEmpty(resource) || (target == McpServerTarget.resource && hasRequest)) {
                result = getResource(client, resource, request);
            } else if (!Checker.isNullOrEmpty(tool)) {
                result = useTool(client, tool, request);
            } else {
                switch (target) {
                    case capability:
                        final ServerCapabilities serverCaps = client.getServerCapabilities();
                        final ClientCapabilities clientCaps = client.getClientCapabilities();
                        final Value nullValue = StringValue.of(null);
                        result = Result.of(capabilityFields,
                                Row.of(capabilityFields, StringValue.of(STR_SERVER),
                                        StringValue.ofJson(serverCaps.experimental()),
                                        StringValue.ofJson(serverCaps.logging()),
                                        StringValue.ofJson(serverCaps.prompts()),
                                        StringValue.ofJson(serverCaps.resources()), nullValue, nullValue,
                                        StringValue.ofJson(serverCaps.tools())),
                                Row.of(capabilityFields, StringValue.of(STR_CLIENT),
                                        StringValue.ofJson(clientCaps.experimental()), nullValue, nullValue, nullValue,
                                        StringValue.ofJson(clientCaps.roots()),
                                        StringValue.ofJson(clientCaps.sampling()), nullValue));
                        break;
                    case info:
                        final Implementation serverInfo = client.getServerInfo();
                        final Implementation clientInfo = client.getClientInfo();
                        result = Result.of(infoFields, Row.of(infoFields, StringValue.of(STR_SERVER),
                                StringValue.of(serverInfo.name()), StringValue.of(serverInfo.version())),
                                Row.of(infoFields, StringValue.of(STR_CLIENT),
                                        StringValue.of(clientInfo.name()), StringValue.of(clientInfo.version())));
                        break;
                    case prompt:
                        result = Result.of(promptFields, client.listPrompts().prompts(),
                                (fields, obj) -> Row.of(fields, StringValue.of(obj.name()),
                                        StringValue.of(obj.description()),
                                        StringValue.ofJson(obj.arguments())));
                        break;
                    case resource:
                        result = Result.of(resourceFields, client.listResources().resources(),
                                (fields, obj) -> Row.of(fields, StringValue.of(obj.name()),
                                        StringValue.of(obj.description()),
                                        StringValue.of(obj.uri()),
                                        StringValue.of(obj.mimeType()),
                                        StringValue.ofJson(obj.annotations())));
                        break;
                    case resource_template:
                        result = Result.of(resourceTemplateFields, client.listResourceTemplates().resourceTemplates(),
                                (fields, obj) -> Row.of(fields, StringValue.of(obj.name()),
                                        StringValue.of(obj.description()),
                                        StringValue.of(obj.uriTemplate()),
                                        StringValue.of(obj.mimeType()),
                                        StringValue.ofJson(obj.annotations())));
                        break;
                    case tool:
                        result = Result.of(toolFields, client.listTools().tools(),
                                (fields, obj) -> Row.of(fields, StringValue.of(obj.name()),
                                        StringValue.of(obj.description()),
                                        StringValue.ofJson(obj.inputSchema())));
                        break;
                    default:
                        throw new SQLException(Utils.format("Unsupported target [%s], please use [%s] instead", target,
                                Arrays.toString(McpServerTarget.values())));
                }
            }
        } catch (McpError e) {
            throw new SQLException(e);
        } finally {
            if (session == null) { // unmanaged
                client.closeGracefully();
            }
        }
        return result;
    }
}
