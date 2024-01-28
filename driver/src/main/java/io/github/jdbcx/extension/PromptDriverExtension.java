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
package io.github.jdbcx.extension;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import io.github.jdbcx.DriverExtension;
import io.github.jdbcx.JdbcActivityListener;
import io.github.jdbcx.Option;
import io.github.jdbcx.QueryContext;
import io.github.jdbcx.interpreter.WebInterpreter;

public class PromptDriverExtension implements DriverExtension {
    static final Option OPTION_PROVIDER = Option
            .of(new String[] { "provider", "Service provider", "", "openai", "google" });

    static class ActivityListener extends AbstractActivityListener {
        static Properties update(Properties config) {
            String provider = OPTION_PROVIDER.getValue(config);
            if ("openai".equalsIgnoreCase(provider)) {
                Properties props = new Properties(config);
                WebInterpreter.OPTION_BASE_URL.setValue(props, "https://api.openai.com/v1");
                WebInterpreter.OPTION_URL_TEMPLATE.setValue(props, "${base.url}/${openai.api}"); // chat/completions
                props.setProperty("openai.api", "chat/completions");
                WebInterpreter.OPTION_REQUEST_HEADERS.setValue(props,
                        "Content-Type=application/json,Authorization=Bearer ${openai.api.key},OpenAI-Organization=${openai.org.key}");
                config = props;
            } else if ("google".equalsIgnoreCase(provider)) {
                Properties props = new Properties(config);
                WebInterpreter.OPTION_BASE_URL.setValue(props, "https://generativelanguage.googleapis.com/v1beta2");
                WebInterpreter.OPTION_URL_TEMPLATE.setValue(props, "${base.url}/models/${google.model}:${google.api}"); // chat/completions
                props.setProperty("google.api", "generateMessage");
                props.setProperty("google.model", "chat-bison-001");
                WebInterpreter.OPTION_REQUEST_HEADERS.setValue(props,
                        "Content-Type=application/json,x-goog-api-key=${google.api.key}");
                config = props;
            }
            return config;
        }

        ActivityListener(QueryContext context, Properties config) {
            super(new WebInterpreter(context, config), update(config));
        }
    }

    private static final List<Option> options;

    static {
        List<Option> list = new ArrayList<>(WebInterpreter.OPTIONS);
        list.add(OPTION_PROVIDER);
        options = Collections.unmodifiableList(list);
    }

    @Override
    public List<String> getAliases() {
        return Collections.singletonList("llm");
    }

    @Override
    public List<Option> getDefaultOptions() {
        return options;
    }

    @Override
    public JdbcActivityListener createListener(QueryContext context, Connection conn, Properties props) {
        return new ActivityListener(context, getConfig(props));
    }

    @Override
    public String getDescription() {
        return "Extension for prompt engineering. "
                + "Please make sure you either have a local GPT environment, "
                + "or you have API access to either OpenAI or Google MakerSuite.";
    }

    @Override
    public String getUsage() {
        return "{{ prompt(provider=google,google.api=generateMessage,google.model=chat-bison-001): "
                + "{\"prompt\":{\"messages\":[{\"content\":\"Hi there!\"}]},\"temperature\":0.1} }}";
    }
}
