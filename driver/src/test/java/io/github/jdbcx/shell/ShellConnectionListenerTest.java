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
package io.github.jdbcx.shell;

import java.sql.SQLException;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.github.jdbcx.Constants;

public class ShellConnectionListenerTest {
    @Test(groups = { "unit" })
    public void testOnQuery() throws SQLException {
        ShellConnectionListener listener = new ShellConnectionListener(new Properties());
        Assert.assertEquals(listener.onQuery("echo 123").trim(), "123");

        if (Constants.IS_WINDOWS) {
            Assert.assertEquals(listener.onQuery("echo %HOMEDRIVE%%HOMEPATH%").trim(), Constants.HOME_DIR);
            Assert.assertEquals(listener.onQuery("echo select 123 | findstr 2\"").trim(), "select 123");
        } else {
            Assert.assertEquals(listener.onQuery("echo $HOME").trim(), Constants.HOME_DIR);
            Assert.assertEquals(listener.onQuery("echo select \"'$(cd; pwd)'\"").trim(),
                    "select '" + Constants.HOME_DIR + "'");
        }
    }
}
