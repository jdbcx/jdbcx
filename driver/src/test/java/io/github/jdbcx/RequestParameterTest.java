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

import java.util.Collections;

import org.testng.Assert;
import org.testng.annotations.Test;

public class RequestParameterTest {
    @Test(groups = { "unit" })
    public void testGetValue() {
        for (RequestParameter p : RequestParameter.values()) {
            Assert.assertEquals(p.getValue(null, null), p.defaultValue());
            Assert.assertEquals(p.getValue(Collections.emptyMap(), Collections.emptyMap()), p.defaultValue());
            Assert.assertEquals(p.getValue(null, null, (String[]) null), p.defaultValue());
            Assert.assertEquals(p.getValue(null, null, null, null), p.defaultValue());
            Assert.assertEquals(p.getValue(null, null, null, "", null), "");

            Assert.assertEquals(p.getValue(Collections.singletonMap(p.header(), "header"),
                    Collections.singletonMap(p.parameter(), "parameter"), "custom"),
                    p.header().isEmpty() ? (p.parameter().isEmpty() ? "custom" : "parameter") : "header");
            Assert.assertEquals(p.getValue(Collections.singletonMap(p.header() + "1", "header"),
                    Collections.singletonMap(p.parameter(), "parameter"), "custom"),
                    p.parameter().isEmpty() ? "custom" : "parameter");
            Assert.assertEquals(p.getValue(Collections.singletonMap(p.header() + "1", "header"),
                    Collections.singletonMap(p.parameter() + "2", "parameter"), "custom"), "custom");
            Assert.assertEquals(p.getValue(Collections.singletonMap(p.header() + "2", "header"),
                    Collections.singletonMap(p.parameter() + "1", "parameter"), "", "custom", null), "");
        }
    }
}
