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
package io.github.jdbcx;

public final class Version {
    private static final Version noVersion = new Version(0, 0, 0, Constants.EMPTY_STRING);

    static final class Factory {
        static final Version currentVersion;

        static {
            String str = Version.class.getPackage().getImplementationVersion();
            if (Checker.isNullOrEmpty(str)) {
                str = Constants.EMPTY_STRING;
            } else {
                char[] chars = str.toCharArray();
                for (int i = 0, len = chars.length; i < len; i++) {
                    if (Character.isDigit(chars[i])) {
                        str = str.substring(i);
                        break;
                    }
                }
            }
            currentVersion = Version.of(str);
        }

        private Factory() {
        }
    }

    public static Version current() {
        return Factory.currentVersion;
    }

    public static Version of(String version) {
        if (Checker.isNullOrBlank(version)) {
            return noVersion;
        }

        int[] array = new int[] { -1, -1, -1 };
        String additional = null;
        int index = 0;
        StringBuilder builder = new StringBuilder();
        for (int i = 0, l = array.length, len = version.length(); i < len; i++) {
            char ch = version.charAt(i);
            if (index < l && Character.isDigit(ch)) {
                builder.append(ch);
                for (int j = i + 1; j < len; j++) {
                    ch = version.charAt(j);
                    if (Character.isDigit(ch)) {
                        builder.append(ch);
                    } else {
                        break;
                    }
                }
                i += builder.length(); // NOSONAR
                array[index++] = Integer.parseInt(builder.toString());
                builder.setLength(0);
            }

            if (index > 0 && ch != '.' && !Character.isWhitespace(ch)) {
                additional = version.substring(i).trim();
                break;
            }
        }

        return new Version(array[0] < 0 ? 0 : array[0], array[1] < 0 ? 0 : array[1], array[2] < 0 ? 0 : array[2],
                additional == null ? Constants.EMPTY_STRING : additional);
    }

    private final int major;
    private final int minor;
    private final int patch;
    private final String additional;

    private Version(int major, int minor, int patch, String additional) {
        this.major = major;
        this.minor = minor;
        this.patch = patch;
        this.additional = additional;
    }

    public int getMajorVersion() {
        return major;
    }

    public int getMinorVersion() {
        return minor;
    }

    public int getPatchVersion() {
        return patch;
    }

    public String getAdditionalInfo() {
        return additional;
    }

    public String toShortString() {
        StringBuilder builder = new StringBuilder().append(major);
        if (minor != 0 || patch != 0) {
            builder.append('.').append(minor);
        }
        if (patch != 0) {
            builder.append('.').append(patch);
        }
        return builder.toString();
    }

    public String toCompactString() {
        StringBuilder builder = new StringBuilder().append(major);
        if (minor != 0 || patch != 0) {
            builder.append('.').append(minor);
        }
        if (patch != 0) {
            builder.append('.').append(patch);
        }
        if (!additional.isEmpty()) {
            builder.append(' ').append(additional);
        }
        return builder.toString();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = prime + major;
        result = prime * result + minor;
        result = prime * result + patch;
        result = prime * result + additional.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        Version other = (Version) obj;
        return major == other.major && minor == other.minor && patch == other.patch
                && additional.equals(other.additional);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder("Version ").append(major).append('.').append(minor).append('.')
                .append(patch);
        if (!additional.isEmpty()) {
            builder.append(' ').append(additional);
        }
        return builder.toString();
    }
}
