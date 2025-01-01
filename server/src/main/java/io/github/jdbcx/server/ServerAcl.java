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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import io.github.jdbcx.Checker;
import io.github.jdbcx.Utils;

// client1.token=123
// client1.allowedHosts=a,b,c
// client1.allowedIPs=192.168.0.0/16
final class ServerAcl {
    static class IPRange {
        final InetAddress start;
        final InetAddress end;

        IPRange(InetAddress start, InetAddress end) {
            this.start = start;
            this.end = end;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = prime + start.hashCode();
            result = prime * result + end.hashCode();
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            } else if (obj == null || getClass() != obj.getClass()) {
                return false;
            }

            IPRange other = (IPRange) obj;
            return start.equals(other.start) && end.equals(other.end);
        }

        @Override
        public String toString() {
            return new StringBuilder(IPRange.class.getSimpleName()).append('@').append(hashCode()).append('[')
                    .append(start).append(',').append(end).append(']').toString();
        }
    }

    static InetAddress getEndAddress(InetAddress start, int prefixLength) throws UnknownHostException {
        final byte[] startBytes = Checker.nonNull(start, InetAddress.class).getAddress();
        final int len = startBytes.length;
        if (prefixLength < 0 || prefixLength > len * 8) {
            throw new IllegalArgumentException(
                    Utils.format("Invalid prefix length %d, expected range is [0, %d]", prefixLength, len * 8));
        }
        final byte[] endBytes = new byte[len];
        System.arraycopy(startBytes, 0, endBytes, 0, len);

        for (int i = len - 1; prefixLength < 8 * (i + 1); i--) {
            endBytes[i] = (byte) (endBytes[i] | (1 << (8 * (i + 1) - prefixLength)) - 1);
        }
        return InetAddress.getByAddress(endBytes);
    }

    static boolean isInRange(InetAddress start, InetAddress end, InetAddress ip) {
        byte[] startBytes = start.getAddress();
        byte[] endBytes = end.getAddress();
        byte[] ipBytes = ip.getAddress();
        if (ipBytes.length != startBytes.length) {
            return false;
        }

        // Check if ipBytes is between startBytes and endBytes
        for (int i = 0, len = ipBytes.length; i < len; i++) {
            if ((ipBytes[i] & 0xFF) < (startBytes[i] & 0xFF) || (ipBytes[i] & 0xFF) > (endBytes[i] & 0xFF)) {
                return false;
            }
        }
        return true;
    }

    final String token;
    final boolean allowAll;
    final Set<String> allowedHosts;
    final Set<String> allowedIPs;
    final List<IPRange> ipRanges;

    ServerAcl(String token, String allowedHosts, String allowedIPs) {
        this.token = Checker.nonEmpty(token, "Token").trim();

        if (Checker.isNullOrEmpty(allowedHosts)) {
            this.allowedHosts = Collections.emptySet();
        } else {
            Set<String> set = new HashSet<>();
            for (String host : Utils.split(allowedHosts, ',')) {
                set.add(host.trim().toLowerCase(Locale.ROOT));
            }
            this.allowedHosts = Collections.unmodifiableSet(set);
        }

        if (Checker.isNullOrEmpty(allowedIPs)) {
            this.allowedIPs = Collections.emptySet();
            this.ipRanges = Collections.emptyList();
        } else {
            Set<String> set = new HashSet<>();
            List<IPRange> list = new ArrayList<>();
            for (String ip : Utils.split(allowedIPs, ',')) {
                if (ip.indexOf('/') == -1) {
                    set.add(ip.trim());
                    continue;
                }
                List<String> parts = Utils.split(ip, '/');
                if (parts.size() == 2) {
                    try {
                        InetAddress start = InetAddress.getByName(parts.get(0).trim());
                        InetAddress end = getEndAddress(start, Integer.parseInt(parts.get(1).trim()));
                        list.add(new IPRange(start, end));
                    } catch (Exception e) {
                        throw new IllegalArgumentException(
                                Utils.format("Failed to parse IP range [%s] due to: %s", ip, e.getMessage()));
                    }
                } else {
                    throw new IllegalArgumentException(
                            Utils.format("Invalid IP range [%s]", ip));
                }
            }
            this.allowedIPs = Collections.unmodifiableSet(set);
            this.ipRanges = Collections.unmodifiableList(list);
        }
        this.allowAll = this.allowedHosts.isEmpty() && this.allowedIPs.isEmpty() && this.ipRanges.isEmpty();
    }

    public boolean isValid(InetAddress address) {
        if (allowAll) {
            return true;
        } else if (address == null) {
            return false;
        }
        return isValidIP(address.getHostAddress()) || (!allowedHosts.isEmpty() && isValidHost(address.getHostName()));
    }

    public boolean isValidHost(String host) {
        boolean isValid = true;
        if (allowAll) {
            return isValid;
        } else if (Checker.isNullOrEmpty(host)) {
            return false;
        }
        return allowedHosts.isEmpty() || allowedHosts.contains(host.toLowerCase(Locale.ROOT));
    }

    public boolean isValidIP(String ip) {
        if (allowAll) {
            return true;
        } else if (Checker.isNullOrEmpty(ip)) {
            return false;
        }

        if (!allowedIPs.isEmpty() && allowedIPs.contains(ip)) {
            return true;
        }

        boolean isValid = true;
        if (!ipRanges.isEmpty()) {
            isValid = false;
            InetAddress address = null;
            try {
                address = InetAddress.getByName(ip);
            } catch (Exception e) {
                return false;
            }
            for (IPRange range : ipRanges) {
                if (isInRange(range.start, range.end, address)) {
                    isValid = true;
                    break;
                }
            }
        }
        return isValid;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = prime + token.hashCode();
        result = prime * result + (allowAll ? 1231 : 1237);
        result = prime * result + allowedHosts.hashCode();
        result = prime * result + allowedIPs.hashCode();
        result = prime * result + ipRanges.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        ServerAcl other = (ServerAcl) obj;
        return token.equals(other.token) && allowAll == other.allowAll && allowedHosts.equals(other.allowedHosts)
                && allowedIPs.equals(other.allowedIPs) && ipRanges.equals(other.ipRanges);
    }

}
