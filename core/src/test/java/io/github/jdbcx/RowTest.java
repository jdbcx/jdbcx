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

import org.testng.Assert;
import org.testng.annotations.Test;

import io.github.jdbcx.data.StringValue;

public class RowTest {
    @Test(groups = "unit")
    public void testConstructor() {
        Assert.assertEquals(Row.of((String) null).values().size(), 1);
        Assert.assertEquals(Row.of((String) null).value(0).asString(), "");

        Assert.assertEquals(Row.of(Collections.emptyList(), new String[] { "2", "1" }).size(), 0);
        Assert.assertEquals(Row.of(Collections.emptyList(), new String[] { "2", "1" }).values().size(), 2);

        Assert.assertEquals(Row.of(Collections.emptyList(), new StringValue("3")).value(0).asString(), "3");
    }
}
