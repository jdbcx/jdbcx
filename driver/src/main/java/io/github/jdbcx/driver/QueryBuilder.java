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

import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import io.github.jdbcx.Constants;
import io.github.jdbcx.DriverExtension;
import io.github.jdbcx.JdbcActivityListener;
import io.github.jdbcx.QueryContext;
import io.github.jdbcx.Result;
import io.github.jdbcx.Row;
import io.github.jdbcx.Utils;
import io.github.jdbcx.executor.jdbc.SqlExceptionUtils;

public final class QueryBuilder {
    private final QueryContext context;
    private final String[] parts;
    private final ExecutableBlock[] blocks;

    private final WrappedConnection conn;
    private final AtomicReference<SQLWarning> ref;

    private SQLWarning lastWarning;

    QueryBuilder(QueryContext context, ParsedQuery pq, WrappedConnection conn, AtomicReference<SQLWarning> ref,
            SQLWarning lastWarning) {
        this.context = context;
        this.parts = pq.getStaticParts().toArray(Constants.EMPTY_STRING_ARRAY);
        this.blocks = pq.getExecutableBlocks().toArray(new ExecutableBlock[0]);

        this.conn = conn;
        this.ref = ref;

        this.lastWarning = lastWarning;
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
            DriverExtension ext = conn.getExtension(block.getExtensionName());
            Properties p = conn.extractProperties(ext);
            properties[i] = p;
            p.putAll(context.getCustomVariables());
            for (Entry<Object, Object> entry : block.getProperties().entrySet()) {
                String key = Utils.applyVariables(entry.getKey().toString(), p);
                String val = Utils.applyVariables(entry.getValue().toString(), p);
                p.setProperty(key, val);
            }

            JdbcActivityListener cl = ext.createListener(context, conn, p);
            if (block.hasOutput()) {
                try {
                    results[i] = cl.onQuery(Utils.applyVariables(block.getContent(), context.getCustomVariables()));
                } catch (SQLWarning e) {
                    ref.set(lastWarning = SqlExceptionUtils.consolidate(lastWarning, e));
                    results[i] = Result.of(block.getContent());
                }
            } else {
                cl.onQuery(Utils.applyVariables(block.getContent(), context.getCustomVariables()));
                results[i] = Result.of(Constants.EMPTY_STRING);
            }
        }

        final List<String[]> exploded = new LinkedList<>();
        exploded.add(new String[len]);
        for (int i = 0; i < len; i++) {
            Result<?> r = results[i];
            String[][] mo = exploded.toArray(new String[0][]);
            exploded.clear();
            for (Row row : r.rows()) {
                String val = WrappedConnection.normalize(row.value(0).asString(), properties[i]);
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
                    Utils.applyVariables(String.join(Constants.EMPTY_STRING, parts), context.getCustomVariables()));
        }

        return Collections.unmodifiableList(new ArrayList<>(queries));
    }
}
