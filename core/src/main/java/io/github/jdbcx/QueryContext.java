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
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class represents a shared query context used for query executions
 * initiated by the same query.
 */
public final class QueryContext implements AutoCloseable, Serializable {
    private static final long serialVersionUID = -7227092865499240252L;

    private static final ThreadLocal<QueryContext> instanceHolder = ThreadLocal.withInitial(QueryContext::newContext);

    static final String PREFIX_RESULT = "result-";

    public static final String KEY_CONFIG = "config";
    public static final String KEY_CONNECTION = "connection";

    /**
     * Creates a brand new query context.
     *
     * @return non-null new query context
     */
    public static final QueryContext newContext() {
        return new QueryContext();
    }

    /**
     * Gets the query context for the current thread.
     *
     * @return non-null query context for the current thread
     */
    public static final QueryContext getCurrentContext() {
        return instanceHolder.get();
    }

    /**
     * Closes the current thread's query context and replaces it with the specified
     * context, unless they are the same. This is a no-op when the contexts are
     * identical.
     *
     * @param newContext non-null new query context
     */
    public static final void setCurrentContext(QueryContext newContext) {
        if (newContext == null) {
            throw new IllegalArgumentException("Non-null query context is required");
        }

        final QueryContext current = instanceHolder.get();
        if (current != null && current != newContext) {
            current.close();
            instanceHolder.set(newContext);
        }
    }

    /**
     * Closes and removes the query context for the current thread. Releases any
     * resources associated with the context.
     */
    public static final void removeCurrentContext() {
        final QueryContext current = instanceHolder.get();
        if (current != null) {
            current.close();
        }
        instanceHolder.remove();
    }

    private final Properties customVars;
    private final transient Map<String, Object> userObjs;

    protected QueryContext() {
        this.customVars = new Properties();
        this.userObjs = new ConcurrentHashMap<>();
    }

    public Object get(String key) {
        return this.userObjs.get(key);
    }

    public Object get(String key, Object defaultValue) {
        return this.userObjs.getOrDefault(key, defaultValue);
    }

    public Object put(String key, Object value) {
        return this.userObjs.put(key, value);
    }

    public boolean hasCustomVariable() {
        return customVars.size() > 0;
    }

    public Properties getCustomVariables() {
        return customVars;
    }

    public Object getResult(String resultId) {
        return get(PREFIX_RESULT.concat(resultId));
    }

    public Object putResult(String resultId, Object rawResult) {
        return put(PREFIX_RESULT.concat(resultId), rawResult);
    }

    @Override
    public void close() {
        this.customVars.clear();
        this.userObjs.clear();
    }
}
