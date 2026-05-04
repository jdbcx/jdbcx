/*
 * Copyright 2022-2026, Zhichun Wu
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
package io.github.jdbcx.server;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.github.jdbcx.QueryContext;
import io.github.jdbcx.RequestParameter;

public class ServerUtilsTest {
    @Test(groups = { "unit" })
    public void testExtractConfig() {
        Assert.assertThrows(NullPointerException.class, () -> BridgeServer.extractConfig(null, null, false));
        Assert.assertThrows(NullPointerException.class,
                () -> BridgeServer.extractConfig(Collections.emptyMap(), null, false));
        Assert.assertThrows(NullPointerException.class,
                () -> BridgeServer.extractConfig(null, Collections.emptyMap(), true));

        Assert.assertEquals(BridgeServer.extractConfig(Collections.emptyMap(), Collections.emptyMap(), false),
                new Properties());
        Map<String, String> params = new HashMap<>();
        for (RequestParameter p : RequestParameter.values()) {
            params.put(p.parameter(), p.parameter());
        }
        Assert.assertEquals(BridgeServer.extractConfig(Collections.emptyMap(), params, false), new Properties());

        final String key = "secrets";
        Properties props = new Properties();
        params.put(key, UUID.randomUUID().toString());
        props.setProperty(key, params.get(key));
        Assert.assertEquals(BridgeServer.extractConfig(Collections.emptyMap(), params, false), props);

        Map<String, String> headers = new HashMap<>();
        headers.put(key, "321");
        Assert.assertEquals(BridgeServer.extractConfig(headers, params, false), props);
        headers.put("jdbcx_" + key, "321");
        Assert.assertEquals(BridgeServer.extractConfig(headers, params, false), props);
        Assert.assertNull(QueryContext.getCurrentContext().get(QueryContext.KEY_PASS_THRU));
        Assert.assertEquals(BridgeServer.extractConfig(headers, params, true), props);
        props.setProperty(key, "321");
        Assert.assertEquals(QueryContext.getCurrentContext().get(QueryContext.KEY_PASS_THRU), props);
    }
}