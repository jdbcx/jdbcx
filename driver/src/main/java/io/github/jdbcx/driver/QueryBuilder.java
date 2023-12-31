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
package io.github.jdbcx.driver;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;

import io.github.jdbcx.Constants;
import io.github.jdbcx.DriverExtension;
import io.github.jdbcx.JdbcActivityListener;
import io.github.jdbcx.Option;
import io.github.jdbcx.QueryContext;
import io.github.jdbcx.Result;
import io.github.jdbcx.Row;
import io.github.jdbcx.Utils;
import io.github.jdbcx.executor.jdbc.SqlExceptionUtils;

public final class QueryBuilder {
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

        for (int i = 0; i < len; i++) {
            ExecutableBlock block = blocks[i];
            DriverExtension ext = manager.getExtension(block.getExtensionName());
            Properties p = manager.extractProperties(ext);
            properties[i] = p;
            p.putAll(context.getMergedVariables());
            for (Entry<Object, Object> entry : block.getProperties().entrySet()) {
                String key = Utils.applyVariables(entry.getKey().toString(), p);
                String val = Utils.applyVariables(entry.getValue().toString(), p);
                p.setProperty(key, val);
            }

            JdbcActivityListener cl = ext.createListener(context, manager.getConnection(), p);
            if (block.hasOutput()) {
                try {
                    if (directQuery && !ext.supportsNoArguments() && block.hasNoArguments()) {
                        queryResult.setResultSet(
                                ConnectionManager.convertTo(ConnectionManager.describe(ext, p), ResultSet.class));
                        return Collections.emptyList();
                    }
                    Result<?> r = cl.onQuery(Utils.applyVariables(block.getContent(), context.getVariables()));
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
                try (Result<?> r = cl.onQuery(Utils.applyVariables(block.getContent(), context.getVariables()))) {
                    results[i] = Result.of(Constants.EMPTY_STRING);
                }
            }
        }

        final List<String[]> exploded = new LinkedList<>();
        exploded.add(new String[len]);
        for (int i = 0; i < len; i++) {
            Result<?> r = results[i];
            String[][] mo = exploded.toArray(new String[0][]);
            exploded.clear();
            for (Row row : r.rows()) {
                String val = ConnectionManager.normalize(row.value(0).asString(), properties[i]);
                for (int ji = 0, l = mo.length; ji < l; ji++) {
                    String[] e = mo[ji]; // 😀
                    String[] newParts = Arrays.copyOf(e, len);
                    newParts[i] = val;
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
                    Utils.applyVariables(String.join(Constants.EMPTY_STRING, parts), context.getVariables()));
        }

        return Collections.unmodifiableList(new ArrayList<>(queries));
    }
}
