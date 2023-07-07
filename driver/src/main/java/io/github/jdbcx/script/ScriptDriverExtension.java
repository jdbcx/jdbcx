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
package io.github.jdbcx.script;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import io.github.jdbcx.Checker;
import io.github.jdbcx.ConnectionListener;
import io.github.jdbcx.Constants;
import io.github.jdbcx.DriverExtension;
import io.github.jdbcx.Logger;
import io.github.jdbcx.LoggerFactory;
import io.github.jdbcx.Option;
import io.github.jdbcx.Scripting;

public class ScriptDriverExtension implements DriverExtension {
    private static final Logger log = LoggerFactory.getLogger(ScriptDriverExtension.class);

    private Option getLanguageOption(Properties props) {
        String defaultLang = Scripting.OPTION_LANGUAGE.getDefaultValue();
        String[] languages = new Scripting(null, props, false, null).getSupportedLanguages();
        Option.Builder builder = Scripting.OPTION_LANGUAGE.update();
        if (languages.length == 0) {
            if (!Checker.isNullOrEmpty(defaultLang)) {
                builder.defaultValue(Constants.EMPTY_STRING);
                log.warn("The default scripting language has been reset from \"%s\" to empty "
                        + "because no ScriptEngineFactory was found in the classpath.", defaultLang);
            }
        } else {
            builder.choices(languages);

            boolean exist = false;
            for (String lang : languages) {
                if (defaultLang.equals(lang)) {
                    exist = true;
                    break;
                }
            }
            if (!exist) {
                builder.defaultValue(languages[0]);
                log.warn("The default scripting language has been reset from \"%s\" to \"%s\" "
                        + "because no corresponding ScriptEngineFactory was found in the classpath.", defaultLang,
                        languages[0]);
            }
        }
        return builder.build();
    }

    @Override
    public List<Option> getOptions(Properties props) {
        List<Option> list = new ArrayList<>();
        list.add(getLanguageOption(props));
        list.add(Scripting.OPTION_BINDING_ERROR);
        list.add(ScriptConnectionListener.OPTION_VAR_CONNECT);
        list.add(ScriptConnectionListener.OPTION_VAR_HELPER);
        return Collections.unmodifiableList(list);
    }

    @Override
    public ConnectionListener createListener(Connection conn, String url, Properties props) {
        return new ScriptConnectionListener(conn, getConfig(props));
    }
}
