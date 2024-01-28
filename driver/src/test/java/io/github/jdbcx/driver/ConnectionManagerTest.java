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
package io.github.jdbcx.driver;

import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.github.jdbcx.Option;

public class ConnectionManagerTest {
    @Test(groups = { "unit" })
    public void testHandleString() {
        String str = " re'sult ";
        Assert.assertEquals(ConnectionManager.normalize(str, null), str);

        Properties props = new Properties();
        Assert.assertEquals(ConnectionManager.normalize(str, props), str);
        Option.RESULT_STRING_TRIM.setValue(props);
        Assert.assertEquals(ConnectionManager.normalize(str, props), str);
        Option.RESULT_STRING_TRIM.setValue(props, "true");
        Assert.assertEquals(ConnectionManager.normalize(str, props), str.trim());

        Option.RESULT_STRING_ESCAPE.setValue(props);
        Assert.assertEquals(ConnectionManager.normalize(str, props), str.trim());
        Option.RESULT_STRING_ESCAPE.setValue(props, "true");
        Assert.assertEquals(ConnectionManager.normalize(str, props), str.trim().replace("'", "\\'"));
        Option.RESULT_STRING_ESCAPE_CHAR.setValue(props);
        Assert.assertEquals(ConnectionManager.normalize(str, props), str.trim().replace("'", "\\'"));
        Option.RESULT_STRING_ESCAPE_CHAR.setValue(props, "b");
        Assert.assertEquals(ConnectionManager.normalize(str, props), str.trim().replace("'", "b'"));
        Option.RESULT_STRING_ESCAPE_TARGET.setValue(props);
        Assert.assertEquals(ConnectionManager.normalize(str, props), str.trim().replace("'", "b'"));
        Option.RESULT_STRING_ESCAPE_TARGET.setValue(props, "s");
        Assert.assertEquals(ConnectionManager.normalize(str, props), str.trim().replace("s", "bs"));
    }
}
