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
package io.github.jdbcx.driver;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import java.util.Properties;

import io.github.jdbcx.Checker;
import io.github.jdbcx.Compression;
import io.github.jdbcx.Constants;
import io.github.jdbcx.DriverExtension;
import io.github.jdbcx.Format;
import io.github.jdbcx.JdbcActivityListener;
import io.github.jdbcx.JdbcDialect;
import io.github.jdbcx.Option;
import io.github.jdbcx.QueryContext;
import io.github.jdbcx.QueryMode;
import io.github.jdbcx.Result;
import io.github.jdbcx.Row;
import io.github.jdbcx.Utils;
import io.github.jdbcx.VariableTag;
import io.github.jdbcx.executor.WebExecutor;
import io.github.jdbcx.executor.jdbc.SqlExceptionUtils;
import io.github.jdbcx.interpreter.WebInterpreter;

public final class QueryBuilder {
    static final String getEncodings(JdbcDialect dialect, Properties config) {
        Compression serverCompress = Compression.valueOf(Option.SERVER_COMPRESSION.getValue(config));
        if (serverCompress == dialect.getPreferredCompression()) {
            return serverCompress.encoding();
        }
        StringBuilder builder = new StringBuilder(dialect.getPreferredCompression().encoding());
        if (dialect.supports(serverCompress)) {
            builder.append(';').append(serverCompress.encoding());
        }
        return builder.toString();
    }

    static final String getMimeTypes(JdbcDialect dialect, Properties config) {
        Format serverFormat = Format.valueOf(Option.SERVER_FORMAT.getValue(config));
        if (serverFormat == dialect.getPreferredFormat()) {
            return serverFormat.mimeType();
        }
        StringBuilder builder = new StringBuilder(dialect.getPreferredFormat().mimeType());
        if (dialect.supports(serverFormat)) {
            builder.append(';').append(serverFormat.mimeType());
        }
        return builder.toString();
    }

    private final QueryContext context;

    private final boolean directQuery;
    private final String[] parts;
    private final ExecutableBlock[] blocks;

    private final ConnectionManager manager;
    private final QueryResult queryResult;

    private SQLWarning lastWarning;

    public QueryBuilder(QueryContext context, ParsedQuery pq, ConnectionManager manager, QueryResult queryResult) {
        this.context = context;

        this.directQuery = pq.isDirectQuery();
        this.parts = pq.getStaticParts().toArray(Constants.EMPTY_STRING_ARRAY);
        this.blocks = pq.getExecutableBlocks().toArray(new ExecutableBlock[0]);
        for (int i = 0, len = blocks.length; i < len; i++) {
            ExecutableBlock block = this.blocks[i];
            if (block.useBridge()) {
                ConnectionMetaData md = manager.getMetaData();
                JdbcDialect dialect = manager.getDialect();
                Properties props = manager.getBridgeConfig();
                VariableTag tag = VariableTag.valueOf(Option.TAG.getValue(props));
                WebInterpreter.OPTION_BASE_URL.setValue(props, manager.getBridgeUrl());
                StringBuilder builder = new StringBuilder(WebExecutor.HEADER_USER_AGENT).append('=')
                        .append(Utils.escape(md.getProduct(), ',')).append(',').append(WebExecutor.HEADER_QUERY_MODE)
                        .append('=').append(QueryMode.ASYNC.code()).append(',').append(WebExecutor.HEADER_ACCEPT)
                        .append('=').append(getMimeTypes(dialect, props)).append(',')
                        .append(WebExecutor.HEADER_ACCEPT_ENCODING)
                        .append('=').append(getEncodings(dialect, props));
                if (md.hasUserName()) {
                    builder.append(',').append(WebExecutor.HEADER_QUERY_USER).append('=')
                            .append(Utils.escape(md.getUserName(), ','));
                }
                WebInterpreter.OPTION_REQUEST_HEADERS.setValue(props, builder.toString());
                final String token = manager.getBridgeToken();
                if (!Checker.isNullOrEmpty(token) && Boolean.parseBoolean(Option.SERVER_AUTH.getValue(props))) {
                    WebInterpreter.OPTION_AUTH_BEARER_TOKEN.setValue(props, token);
                }
                final String fullQuery;
                // TODO escaping when server uses different variable tag
                if (block.hasOutput()) {
                    fullQuery = tag.function(block.getContent());
                } else {
                    fullQuery = tag.procedure(block.getContent());
                }
                this.blocks[i] = new ExecutableBlock(block.getIndex(), "web", props, fullQuery, block.hasOutput());
            }
        }

        this.manager = manager;
        this.queryResult = queryResult;

        this.lastWarning = null;
    }

    public SQLWarning getLastWarning() {
        return lastWarning;
    }

    // List<String> buildQueries()
    // String buildConsolidatedQuery(String delimiter) // delimiter could be ";" or
    // even "\n union all \n"
    // or just let the caller to combine

    // public String build() throws SQLException {
    // return String.join(";\n", buildForQuery());
    // }

    // public void buildForUpdate() throws SQLException {

    // }

    public List<String> build() throws SQLException { // timeout
        final int len = blocks.length;
        final Result<?>[] results = new Result[len];
        final Properties[] properties = new Properties[len];
        final Map<Integer, List<Integer>> cachedIndices = new HashMap<>();

        final VariableTag tag = manager.getVariableTag();

        for (int i = 0; i < len; i++) {
            ExecutableBlock block = blocks[i];
            int sameBlockIndex = -1;
            for (int k = 0; k < i; k++) {
                if (blocks[k].sameAs(block)) {
                    sameBlockIndex = k;
                    break;
                }
            }
            if (sameBlockIndex >= 0) {
                List<Integer> list = cachedIndices.get(sameBlockIndex);
                if (list == null) {
                    list = new ArrayList<>();
                    cachedIndices.put(sameBlockIndex, list);
                }
                list.add(i);
                continue;
            }
            DriverExtension ext = manager.getExtension(block.getExtensionName());
            Properties p = manager.extractProperties(ext);
            properties[i] = p;
            p.putAll(context.getMergedVariables());
            for (Entry<Object, Object> entry : block.getProperties().entrySet()) {
                String key = Utils.applyVariables(entry.getKey().toString(), tag, p);
                String val = Utils.applyVariables(entry.getValue().toString(), tag, p);
                p.setProperty(key, val);
            }

            if (block.hasOutput()) {
                try {
                    if (directQuery && !ext.supportsNoArguments() && block.hasNoArguments()) {
                        queryResult.setResultSet(
                                ConnectionManager.convertTo(ConnectionManager.describe(ext, p), ResultSet.class));
                        return Collections.emptyList();
                    }

                    JdbcActivityListener cl = ext.createListener(context, manager.getConnection(), p);
                    Result<?> r = cl.onQuery(Utils.applyVariables(block.getContent(), tag, context.getVariables()));
                    if (directQuery
                            && (ext.supportsDirectQuery() || Boolean.parseBoolean(Option.EXEC_DRYRUN.getValue(p)))) {
                        queryResult.setResultSet(ConnectionManager.convertTo(r, ResultSet.class));
                        return Collections.emptyList();
                    } else {
                        results[i] = r;
                    }
                } catch (SQLWarning e) {
                    queryResult.setWarnings(lastWarning = SqlExceptionUtils.consolidate(lastWarning, e));
                    results[i] = Result.of(block.getContent());
                }
            } else {
                JdbcActivityListener cl = ext.createListener(context, manager.getConnection(), p);
                try (Result<?> r = cl.onQuery(Utils.applyVariables(block.getContent(), tag, context.getVariables()))) {
                    results[i] = Result.of(Constants.EMPTY_STRING);
                }
            }
        }

        final List<String[]> exploded = new LinkedList<>();
        exploded.add(new String[len]);
        for (int i = 0; i < len; i++) {
            Result<?> r = results[i];
            if (r == null) {
                continue;
            }
            String[][] mo = exploded.toArray(new String[0][]);
            exploded.clear();
            for (Row row : r.rows()) {
                String val = ConnectionManager.normalize(row.value(0).asString(), tag, properties[i]);
                for (int ji = 0, l = mo.length; ji < l; ji++) {
                    String[] e = mo[ji]; // 😀
                    String[] newParts = Arrays.copyOf(e, len);
                    newParts[i] = val;
                    List<Integer> list = cachedIndices.get(i);
                    if (list != null) {
                        for (Integer k : list) {
                            newParts[k] = val;
                        }
                    }
                    exploded.add(newParts);
                }
            }
        }

        final List<String> queries = new LinkedList<>();
        for (String[] arr : exploded) {
            for (int i = 0; i < len; i++) {
                parts[blocks[i].getIndex()] = arr[i];
            }
            queries.add(
                    Utils.applyVariables(String.join(Constants.EMPTY_STRING, parts), tag, context.getVariables()));
        }
        return Collections.unmodifiableList(new ArrayList<>(queries));
    }
}
