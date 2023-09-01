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
package io.github.jdbcx.data;

import java.util.Arrays;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.github.jdbcx.Row;

public class IterableWrapperTest {
    @Test(groups = { "unit" })
    public void testConstructor() {
        int index = 0;
        for (Row r : new IterableWrapper(DefaultRow.defaultFields, Arrays.asList(1, 2))) {
            Assert.assertEquals(r.value(0).asInt(), index + 1);
            index++;
        }
        Assert.assertEquals(index, 2);
    }
}
