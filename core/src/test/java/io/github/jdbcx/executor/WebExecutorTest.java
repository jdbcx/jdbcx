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
package io.github.jdbcx.executor;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.github.jdbcx.BaseIntegrationTest;
import io.github.jdbcx.Option;
import io.github.jdbcx.Utils;

public class WebExecutorTest extends BaseIntegrationTest {
    @Test(groups = "unit")
    public void testConstructor() {
        WebExecutor executor;
        Assert.assertNotNull(executor = new WebExecutor(null, null));
        Assert.assertEquals(executor.getDefaultConnectTimeout(),
                Integer.parseInt(WebExecutor.OPTION_CONNECT_TIMEOUT.getDefaultValue()));
        Assert.assertEquals(executor.getDefaultSocketTimeout(),
                Integer.parseInt(Option.EXEC_TIMEOUT.getDefaultValue()));
        Assert.assertEquals(executor.getDefaultTimeout(), Integer.parseInt(Option.EXEC_TIMEOUT.getDefaultValue()));
        Assert.assertEquals(executor.getDefaultProxy(), Option.PROXY.getDefaultValue());

        Properties props = new Properties();
        Assert.assertNotNull(executor = new WebExecutor(null, props));
        Assert.assertEquals(executor.getDefaultConnectTimeout(),
                Integer.parseInt(WebExecutor.OPTION_CONNECT_TIMEOUT.getDefaultValue()));
        Assert.assertEquals(executor.getDefaultSocketTimeout(),
                Integer.parseInt(Option.EXEC_TIMEOUT.getDefaultValue()));
        Assert.assertEquals(executor.getDefaultTimeout(), Integer.parseInt(Option.EXEC_TIMEOUT.getDefaultValue()));
        Assert.assertEquals(executor.getDefaultProxy(), Option.PROXY.getDefaultValue());

        WebExecutor.OPTION_SOCKET_TIMEOUT.setValue(props, "30001");
        Assert.assertNotNull(executor = new WebExecutor(null, props));
        Assert.assertEquals(executor.getDefaultConnectTimeout(),
                Integer.parseInt(WebExecutor.OPTION_CONNECT_TIMEOUT.getDefaultValue()));
        Assert.assertEquals(executor.getDefaultSocketTimeout(), 30001);
        Assert.assertEquals(executor.getDefaultTimeout(), Integer.parseInt(Option.EXEC_TIMEOUT.getDefaultValue()));
        Assert.assertEquals(executor.getDefaultProxy(), Option.PROXY.getDefaultValue());

        WebExecutor.OPTION_CONNECT_TIMEOUT.setValue(props, "1234");
        Assert.assertNotNull(executor = new WebExecutor(null, props));
        Assert.assertEquals(executor.getDefaultConnectTimeout(), 1234);
        Assert.assertEquals(executor.getDefaultSocketTimeout(), 30001);
        Assert.assertEquals(executor.getDefaultTimeout(), Integer.parseInt(Option.EXEC_TIMEOUT.getDefaultValue()));
        Assert.assertEquals(executor.getDefaultProxy(), Option.PROXY.getDefaultValue());

        Option.EXEC_TIMEOUT.setValue(props, "999999");
        Assert.assertNotNull(executor = new WebExecutor(null, props));
        Assert.assertEquals(executor.getDefaultConnectTimeout(), 1234);
        Assert.assertEquals(executor.getDefaultSocketTimeout(), 30001);
        Assert.assertEquals(executor.getDefaultTimeout(), 999999);
        Assert.assertEquals(executor.getDefaultProxy(), Option.PROXY.getDefaultValue());

        Option.PROXY.setValue(props, "socks5h://4.3.2.1:1234");
        Assert.assertNotNull(executor = new WebExecutor(null, props));
        Assert.assertEquals(executor.getDefaultConnectTimeout(), 1234);
        Assert.assertEquals(executor.getDefaultSocketTimeout(), 30001);
        Assert.assertEquals(executor.getDefaultTimeout(), 999999);
        Assert.assertEquals(executor.getDefaultProxy(), "socks5h://4.3.2.1:1234");
    }

    @Test(groups = "unit")
    public void testGetProxy() throws IOException {
        Assert.assertEquals(new WebExecutor(null, null).getProxy(null), Proxy.NO_PROXY);
        Properties props = new Properties();
        Assert.assertEquals(new WebExecutor(null, props).getProxy(null), Proxy.NO_PROXY);
        Assert.assertEquals(new WebExecutor(null, props).getProxy(props), Proxy.NO_PROXY);

        Option.PROXY.setValue(props, "");
        Assert.assertEquals(new WebExecutor(null, props).getProxy(props), Proxy.NO_PROXY);

        Option.PROXY.setValue(props, ":");
        Assert.assertEquals(new WebExecutor(null, props).getProxy(props),
                new Proxy(WebExecutor.DEFAULT_PROXY_TYPE, InetSocketAddress
                        .createUnresolved(WebExecutor.DEFAULT_PROXY_HOST, WebExecutor.DEFAULT_PROXY_PORT)));
        Option.PROXY.setValue(props, ":8989");
        Assert.assertEquals(new WebExecutor(null, props).getProxy(props),
                new Proxy(WebExecutor.DEFAULT_PROXY_TYPE,
                        InetSocketAddress.createUnresolved(WebExecutor.DEFAULT_PROXY_HOST, 8989)));
        Option.PROXY.setValue(props, "-$-"); // authority
        Assert.assertEquals(new WebExecutor(null, props).getProxy(props),
                new Proxy(WebExecutor.DEFAULT_PROXY_TYPE, InetSocketAddress
                        .createUnresolved(WebExecutor.DEFAULT_PROXY_HOST, WebExecutor.DEFAULT_PROXY_PORT)));
        Option.PROXY.setValue(props, "www.test.com:");
        Assert.assertEquals(new WebExecutor(null, props).getProxy(props),
                new Proxy(WebExecutor.DEFAULT_PROXY_TYPE,
                        InetSocketAddress.createUnresolved("www.test.com", WebExecutor.DEFAULT_PROXY_PORT)));
        Option.PROXY.setValue(props, "www.test.com:8989");
        Assert.assertEquals(new WebExecutor(null, props).getProxy(props),
                new Proxy(WebExecutor.DEFAULT_PROXY_TYPE,
                        InetSocketAddress.createUnresolved("www.test.com", 8989)));

        Option.PROXY.setValue(props, "http://www.test.com");
        Assert.assertEquals(new WebExecutor(null, props).getProxy(props),
                new Proxy(Proxy.Type.HTTP,
                        InetSocketAddress.createUnresolved("www.test.com", WebExecutor.DEFAULT_PROXY_PORT)));
        Option.PROXY.setValue(props, "https://www.test.com");
        Assert.assertEquals(new WebExecutor(null, props).getProxy(props),
                new Proxy(Proxy.Type.HTTP,
                        InetSocketAddress.createUnresolved("www.test.com", WebExecutor.DEFAULT_PROXY_PORT)));
        Option.PROXY.setValue(props, "socks5h://4.3.2.1:1234");
        Assert.assertEquals(new WebExecutor(null, props).getProxy(props),
                new Proxy(Proxy.Type.SOCKS, InetSocketAddress.createUnresolved("4.3.2.1", 1234)));
    }

    @Test(groups = "integration")
    public void testGet() throws Exception {
        Properties props = new Properties();
        String address = getClickHouseServer();
        Assert.assertEquals(
                Stream.readAllAsString(
                        new WebExecutor(null, null).get(Utils.toURL("http://" + address), props, null)),
                "Ok.\n");
        Assert.assertEquals(
                Stream.readAllAsString(new WebExecutor(null, null)
                        .get(Utils.toURL("http://default@" + address + "/?query=select+7"), props, null)),
                "7\n");

        Option.PROXY.setValue(props, getProxyServer());
        Assert.assertEquals(
                Stream.readAllAsString(new WebExecutor(null, null)
                        .get(Utils.toURL("http://" + getDeclaredClickHouseServer() + "/?query=select+1"), props, null)),
                "1\n");
    }

    @Test(groups = "integration")
    public void testPost() throws Exception {
        Properties props = new Properties();
        Assert.assertEquals(
                Stream.readAllAsString(
                        new WebExecutor(null, null).post(Utils.toURL("http://default@" + getClickHouseServer()),
                                "select 9", props, null)),
                "9\n");
    }
}
