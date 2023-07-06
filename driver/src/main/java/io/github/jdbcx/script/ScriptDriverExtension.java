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

import io.github.jdbcx.ConnectionListener;
import io.github.jdbcx.DriverExtension;
import io.github.jdbcx.Option;
import io.github.jdbcx.Scripting;

public class ScriptDriverExtension implements DriverExtension {
    @Override
    public List<Option> getOptions(Properties props) {
        List<Option> list = new ArrayList<>();
        list.add(Scripting.OPTION_LANGUAGE.update()
                .choices(new Scripting(null, props, false, null).getSupportedLanguages()).build());
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
