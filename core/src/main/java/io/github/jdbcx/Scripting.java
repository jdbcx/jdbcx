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

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.Set;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineFactory;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

public class Scripting {
    private static final Logger log = LoggerFactory.getLogger(Scripting.class);

    public static final String DEFAULT_SCRIPT_LANGUAGE = "javascript";
    public static final Option OPTION_LANGUAGE = Option
            .of(new String[] { "language", "Scripting language", DEFAULT_SCRIPT_LANGUAGE });
    public static final Option OPTION_BINDING_ERROR = Option
            .of(new String[] { "binding.error",
                    "Approach to handle binding error, either throw an exception or simply ignore the error", "throw",
                    "ignore" });

    private final String defaultLanguage;
    private final ScriptEngineManager manager;
    private final Set<String> supportedLanguages;

    private final boolean ignoreBindingError;

    public Scripting(String defaultLanguage, Properties props, Map<String, Object> vars) {
        this(defaultLanguage, props, true, vars);
    }

    public Scripting(String defaultLanguage, Properties props, boolean validate, Map<String, Object> vars) {
        final String customClasspath = Option.CUSTOM_CLASSPATH.getValue(props);
        if (Checker.isNullOrEmpty(customClasspath)) {
            this.manager = new ScriptEngineManager(Scripting.class.getClassLoader());
        } else {
            this.manager = new ScriptEngineManager(
                    new ExpandedUrlClassLoader(Scripting.class, customClasspath));
        }

        this.ignoreBindingError = !OPTION_BINDING_ERROR.getDefaultValue().equals(OPTION_BINDING_ERROR.getValue(props));

        Set<String> langs = new LinkedHashSet<>();
        for (ScriptEngineFactory factory : this.manager.getEngineFactories()) {
            langs.add(factory.getLanguageName());
        }
        for (ScriptEngineFactory factory : ServiceLoader.load(ScriptEngineFactory.class,
                Scripting.class.getClassLoader())) {
            langs.add(factory.getLanguageName());
        }

        if (validate) {
            if (langs.isEmpty()) {
                throw new IllegalArgumentException("No script language detected in classpath.");
            } else if (!langs.contains(defaultLanguage)) {
                throw new IllegalArgumentException(Utils.format(
                        "Scripting language \"%s\" is not supported. Available options are [%s].",
                        defaultLanguage, langs));
            }
            this.defaultLanguage = defaultLanguage;
        } else {
            this.defaultLanguage = langs.isEmpty() || langs.contains(defaultLanguage) ? Constants.EMPTY_STRING
                    : defaultLanguage;
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

    public Object execute(String query, Map<String, Object> vars) throws ScriptException {
        if (Checker.isNullOrBlank(query)) {
            return Constants.EMPTY_STRING;
        }

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
        return engine.eval(query);
    }
}
