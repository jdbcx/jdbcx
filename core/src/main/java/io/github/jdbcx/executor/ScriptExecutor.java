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
package io.github.jdbcx.executor;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineFactory;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import io.github.jdbcx.Checker;
import io.github.jdbcx.Constants;
import io.github.jdbcx.Logger;
import io.github.jdbcx.LoggerFactory;
import io.github.jdbcx.Option;
import io.github.jdbcx.Utils;

public class ScriptExecutor extends AbstractExecutor {
    private static final Logger log = LoggerFactory.getLogger(ScriptExecutor.class);

    public static final String DEFAULT_SCRIPT_LANGUAGE = "rhino";

    public static final Option OPTION_LANGUAGE = Option
            .of(new String[] { "language", "Scripting language to use for evaluating the provided script.",
                    DEFAULT_SCRIPT_LANGUAGE });
    public static final Option OPTION_BINDING_ERROR = Option
            .of(new String[] { "binding.error", "Error handling strategy for bindings during script evaluation.",
                    Option.ERROR_HANDLING_THROW, Option.ERROR_HANDLING_IGNORE });

    private final String defaultLanguage;
    private final ScriptEngineManager manager;
    private final Set<String> supportedLanguages;

    private final boolean ignoreBindingError;

    public ScriptExecutor(String defaultLanguage, Properties props, Map<String, Object> vars) {
        this(defaultLanguage, props, true, vars, null);
    }

    public ScriptExecutor(String defaultLanguage, Properties props, Map<String, Object> vars, ClassLoader loader) {
        this(defaultLanguage, props, true, vars, loader);
    }

    public ScriptExecutor(String defaultLanguage, Properties props, boolean validate, Map<String, Object> vars,
            ClassLoader loader) {
        super(props);

        this.manager = new ScriptEngineManager(loader != null ? loader : ScriptExecutor.class.getClassLoader());

        this.ignoreBindingError = !OPTION_BINDING_ERROR.getDefaultValue().equals(OPTION_BINDING_ERROR.getValue(props));

        Set<String> engines = new LinkedHashSet<>();
        Set<String> langs = new LinkedHashSet<>();
        for (ScriptEngineFactory factory : this.manager.getEngineFactories()) {
            engines.add(factory.getEngineName());
            langs.add(factory.getLanguageName());
        }
        for (ScriptEngineFactory factory : ServiceLoader.load(ScriptEngineFactory.class,
                ScriptExecutor.class.getClassLoader())) {
            engines.add(factory.getEngineName());
            langs.add(factory.getLanguageName());
        }

        if (validate) {
            if (langs.isEmpty()) {
                throw new IllegalArgumentException(
                        Utils.format("No script language detected in classpath(loader=%s).", loader));
            } else if (!engines.contains(defaultLanguage) && !langs.contains(defaultLanguage)) {
                throw new IllegalArgumentException(Utils.format(
                        "Scripting language or engine \"%s\" is not supported. Available options are languages %s, or engine names %s.",
                        defaultLanguage, langs, engines));
            }
            this.defaultLanguage = defaultLanguage;
        } else {
            this.defaultLanguage = langs.contains(defaultLanguage) || engines.contains(defaultLanguage)
                    ? defaultLanguage
                    : Constants.EMPTY_STRING;
        }

        this.supportedLanguages = Collections.unmodifiableSet(langs);

        if (vars != null) {
            for (Map.Entry<String, Object> v : vars.entrySet()) {
                this.manager.put(v.getKey(), v.getValue());
            }
        }
    }

    public String getDefaultLanguage() {
        return defaultLanguage;
    }

    public boolean isSupported(String language) {
        return supportedLanguages.contains(language);
    }

    public String[] getSupportedLanguages() {
        return supportedLanguages.toArray(new String[0]);
    }

    public Object execute(String query, Properties props, Map<String, Object> vars)
            throws IOException, TimeoutException {
        if (getDryRun(props)) {
            return new StringBuilder().append("language=").append(defaultLanguage).append('\n')
                    .append("script=").append(query).append('\n')
                    .append("options=").append(props).append('\n')
                    .append("vars=").append(vars).append('\n')
                    // .append(Option.EXEC_PARALLELISM.getName()).append('=').append(getParallelism(props)).append('\n')
                    .append(Option.EXEC_TIMEOUT.getName()).append('=').append(getTimeout(props))
                    .toString();
        }

        if (Checker.isNullOrBlank(query)) {
            return Constants.EMPTY_STRING;
        }

        // final int parallelism = getParallelism(props);
        final int timeout = getTimeout(props);
        final long startTime = timeout <= 0 ? 0L : System.currentTimeMillis();
        final long timeoutMs = timeout;
        ScriptEngine engine = manager.getEngineByName(defaultLanguage);
        if (vars != null && !vars.isEmpty()) {
            // not all languages support bindings
            try {
                Bindings bindings = engine.createBindings();
                bindings.putAll(vars);
                engine.setBindings(bindings, ScriptContext.ENGINE_SCOPE);
            } catch (Exception e) {
                if (ignoreBindingError) {
                    log.warn("Failed to set bindings to %s", defaultLanguage, e);
                } else {
                    throw e;
                }
            }
        }

        CompletableFuture<?> future = supply(() -> {
            try {
                return engine.eval(query);
            } catch (ScriptException e) {
                throw new CompletionException(e);
            }
        }, props);

        return waitForTask(log, future, startTime, timeoutMs);
    }
}
