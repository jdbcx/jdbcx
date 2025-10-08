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
package io.github.jdbcx.server;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ServerAclTest {
    @Test(groups = { "unit" })
    public void testConstructor() throws UnknownHostException {
        ServerAcl acl = new ServerAcl(null, null);
        Assert.assertTrue(acl.allowAll);
        Assert.assertEquals(acl.allowedHosts, Collections.emptyList());
        Assert.assertEquals(acl.allowedIPs, Collections.emptyList());
        Assert.assertEquals(acl.ipRanges, Collections.emptyList());
        Assert.assertTrue(acl.isValidHost("unknown.host"));
        Assert.assertTrue(acl.isValidIP("192.168.5.6"));

        acl = new ServerAcl(" host.1 , host.2 ", null);
        Assert.assertFalse(acl.allowAll);
        Assert.assertEquals(acl.allowedHosts, Arrays.asList("host.1", "host.2"));
        Assert.assertEquals(acl.allowedIPs, Collections.emptyList());
        Assert.assertEquals(acl.ipRanges, Collections.emptyList());
        Assert.assertFalse(acl.isValidHost("unknown.host"));
        Assert.assertTrue(acl.isValidHost("host.1"));
        Assert.assertTrue(acl.isValidHost("host.2"));
        Assert.assertTrue(acl.isValidIP("192.168.5.6"));

        acl = new ServerAcl(null, " 192.168.3.1 , 192.168.1.2 / 24 ");
        Assert.assertFalse(acl.allowAll);
        Assert.assertEquals(acl.allowedHosts, Collections.emptyList());
        Assert.assertEquals(acl.allowedIPs, Arrays.asList("192.168.3.1"));
        Assert.assertEquals(acl.ipRanges, Collections.singletonList(
                new ServerAcl.IPRange(InetAddress.getByName("192.168.1.2"), InetAddress.getByName("192.168.1.255"))));
        Assert.assertTrue(acl.isValidHost("unknown.host"));
        Assert.assertTrue(acl.isValidIP("192.168.3.1"));
        Assert.assertFalse(acl.isValidIP("192.168.1.1"));
        Assert.assertTrue(acl.isValidIP("192.168.1.5"));
    }

    @Test(groups = { "unit" })
    public void testGetEndAddress() throws UnknownHostException {
        Assert.assertThrows(IllegalArgumentException.class, () -> ServerAcl.getEndAddress(null, 1));

        final InetAddress ipv4 = InetAddress.getByName("192.168.1.3");
        final InetAddress ipv6 = InetAddress.getByName("::1234:56a8:103");
        Assert.assertThrows(IllegalArgumentException.class, () -> ServerAcl.getEndAddress(ipv4, -1));
        Assert.assertThrows(IllegalArgumentException.class, () -> ServerAcl.getEndAddress(ipv4, 33));
        Assert.assertThrows(IllegalArgumentException.class, () -> ServerAcl.getEndAddress(ipv6, -1));
        Assert.assertThrows(IllegalArgumentException.class, () -> ServerAcl.getEndAddress(ipv6, 129));

        Assert.assertEquals(ServerAcl.getEndAddress(ipv4, 0), InetAddress.getByName("255.255.255.3"));
        Assert.assertEquals(ServerAcl.getEndAddress(ipv4, 1), InetAddress.getByName("255.255.255.255"));
        Assert.assertEquals(ServerAcl.getEndAddress(ipv4, 2), InetAddress.getByName("255.255.255.255"));
        Assert.assertEquals(ServerAcl.getEndAddress(ipv4, 8), InetAddress.getByName("192.255.255.255"));
        Assert.assertEquals(ServerAcl.getEndAddress(ipv4, 24), InetAddress.getByName("192.168.1.255"));

        Assert.assertEquals(ServerAcl.getEndAddress(ipv6, 0),
                InetAddress.getByName("ffff:ff00:ffff:ff00:ffff:ff34:ffff:ff03"));
        Assert.assertEquals(ServerAcl.getEndAddress(ipv6, 1),
                InetAddress.getByName("7fff:ffff:7fff:ffff:7fff:ffff:7fff:ffff"));
        Assert.assertEquals(ServerAcl.getEndAddress(ipv6, 2),
                InetAddress.getByName("3fff:ffff:3fff:ffff:3fff:ffff:7fff:ffff"));
        Assert.assertEquals(ServerAcl.getEndAddress(ipv6, 8),
                InetAddress.getByName("ff:ffff:ff:ffff:ff:ffff:56ff:ffff"));
        Assert.assertEquals(ServerAcl.getEndAddress(ipv6, 24),
                InetAddress.getByName("0:ff:ffff:ff:ffff:12ff:ffff:1ff"));
        Assert.assertEquals(ServerAcl.getEndAddress(ipv6, 33),
                InetAddress.getByName("0:0:7fff:ffff:7fff:ffff:7fff:ffff"));
        Assert.assertEquals(ServerAcl.getEndAddress(ipv6, 64),
                InetAddress.getByName("0:0:0:0:ffff:ff34:ffff:ff03"));
        Assert.assertEquals(ServerAcl.getEndAddress(ipv6, 96),
                InetAddress.getByName("0:0:0:0:0:1234:ffff:ff03"));
        Assert.assertEquals(ServerAcl.getEndAddress(ipv6, 128),
                InetAddress.getByName("0:0:0:0:0:1234:56a8:103"));
    }

    @Test(groups = { "unit" })
    public void testIsInRange() throws UnknownHostException {
        Assert.assertTrue(
                ServerAcl.isInRange(InetAddress.getByName("192.168.3.1"), InetAddress.getByName("192.168.3.2"),
                        InetAddress.getByName("192.168.3.1")));
        ServerAcl.isInRange(InetAddress.getByName("192.168.3.1"), InetAddress.getByName("192.168.3.2"),
                InetAddress.getByName("::ffff:c0a8:302"));
        Assert.assertFalse(
                ServerAcl.isInRange(InetAddress.getByName("192.168.3.1"), InetAddress.getByName("192.168.3.2"),
                        InetAddress.getByName("192.168.3.5")));
        Assert.assertFalse(
                ServerAcl.isInRange(InetAddress.getByName("192.168.3.1"), InetAddress.getByName("192.168.3.2"),
                        InetAddress.getByName("::1234:56a8:103")));

        Assert.assertTrue(
                ServerAcl.isInRange(InetAddress.getByName("::1234:56a8:101"), InetAddress.getByName("::1234:56a8:103"),
                        InetAddress.getByName("::1234:56a8:101")));
    }
}
