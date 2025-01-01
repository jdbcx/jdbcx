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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * This class represents a shared query context used for query executions
 * initiated by the same query.
 */
public final class QueryContext implements AutoCloseable, Serializable {
    private static final long serialVersionUID = -7227092865499240252L;

    /**
     * Key for bridge configuration encapsulated in {@link Properties}.
     */
    public static final String KEY_BRIDGE = "bridge";
    /**
     * Key for {@link ConfigManager}.
     */
    public static final String KEY_CONFIG = "config";
    /**
     * Key for function to get {@link java.sql.Connection}.
     */
    public static final String KEY_CONNECTION = "connection";
    /**
     * Key for JDBC dialect.
     */
    public static final String KEY_DIALECT = "dialect";
    /**
     * Key for {@link VariableTag}.
     */
    public static final String KEY_TAG = "tag";

    private static final QueryContext globalContext = new QueryContext(Constants.SCOPE_GLOBAL, null);
    private static final ThreadLocal<QueryContext> instanceHolder = ThreadLocal
            .withInitial(QueryContext::newContextForThread);

    static final QueryContext newContextForThread() {
        return new QueryContext(Constants.SCOPE_THREAD, globalContext);
    }

    static boolean checkScope(String scope, QueryContext context) {
        do {
            if (context.scope.equals(scope)) {
                return true;
            }
        } while ((context = context.parent) != null);
        return false;
    }

    /**
     * Creates a brand new query context.
     *
     * @return non-null new query context
     */
    public static final QueryContext newContext() {
        return newContext(null, null);
    }

    public static final QueryContext newContext(String scope, QueryContext parent) {
        if (scope == null) {
            scope = Constants.SCOPE_QUERY;
        }
        if (parent == null) {
            parent = globalContext;
        }
        if (checkScope(scope, parent)) {
            throw new IllegalArgumentException(
                    Utils.format("Scope \"%s\" has been taken by one of the parent context", scope));
        }

        return new QueryContext(scope, parent);
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

    private final String scope;
    private final QueryContext parent;
    private final transient Map<String, Object> userObjs;
    private final Properties vars;

    protected List<QueryContext> getChainedContext(boolean currentIsFirst) {
        LinkedList<QueryContext> list = new LinkedList<>();
        QueryContext context = this;
        do {
            if (currentIsFirst) {
                list.addLast(context);
            } else {
                list.addFirst(context);
            }
        } while ((context = context.parent) != null);
        return list;
    }

    protected QueryContext(String scope, QueryContext parent) {
        this.scope = scope;
        if (parent != null) {
            this.parent = parent;
            this.vars = new Properties(parent.vars);
        } else {
            this.parent = null;
            this.vars = new Properties();
        }
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

    public QueryContext getParent() {
        return parent;
    }

    public String getScope() {
        return scope;
    }

    /**
     * Gets variable tag.
     *
     * @return non-null variable tag
     */
    @SuppressWarnings("unchecked")
    public VariableTag getVariableTag() {
        Supplier<VariableTag> supplier = (Supplier<VariableTag>) get(QueryContext.KEY_TAG);
        return supplier != null ? supplier.get() : VariableTag.BRACE;
    }

    public boolean hasVariable() {
        return vars.size() > 0 || (parent != null && parent.hasVariable());
    }

    public Properties getVariables() {
        return new Properties(vars);
    }

    public Properties getMergedVariables() {
        Properties props = new Properties();
        for (QueryContext context : getChainedContext(false)) {
            props.putAll(context.vars);
        }
        return props;
    }

    public void setVariable(String name, String value) {
        vars.setProperty(name, value);
    }

    public void removeVariable(String name) {
        vars.remove(name);
    }

    @SuppressWarnings("resource")
    public void setVariableInScope(String scope, String name, String value) {
        QueryContext context = this;
        do {
            if (context.scope.equals(scope)) {
                context.vars.setProperty(name, value);
                return;
            }
        } while ((context = context.parent) != null);
        throw new IllegalArgumentException(Utils.format("Unknown scope \"%s\"", scope));
    }

    public String getVariable(String name) {
        return vars.getProperty(name);
    }

    public String getVariable(String name, String defaultValue) {
        return vars.getProperty(name, defaultValue);
    }

    public String getVariableInScope(String scope, String name) {
        return getVariableInScope(scope, name, null);
    }

    @SuppressWarnings("resource")
    public String getVariableInScope(String scope, String name, String defaultValue) {
        QueryContext context = this;
        do {
            if (context.scope.equals(scope)) {
                return context.vars.getProperty(name, defaultValue);
            }
        } while ((context = context.parent) != null);
        throw new IllegalArgumentException(Utils.format("Unknown scope \"%s\"", scope));
    }

    @Override
    public void close() {
        this.userObjs.clear();
        this.vars.clear();
    }
}
