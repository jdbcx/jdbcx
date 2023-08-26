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

import java.io.IOException;

import org.testng.Assert;
import org.testng.annotations.Test;

public class VersionTest {
    private void check(Version v, int expectedMajorVersion, int expectedMinorVersion, int expectedPatchVersion,
            String expectedInfo) {
        Assert.assertEquals(v.getMajorVersion(), expectedMajorVersion);
        Assert.assertEquals(v.getMinorVersion(), expectedMinorVersion);
        Assert.assertEquals(v.getPatchVersion(), expectedPatchVersion);
        Assert.assertEquals(v.getAdditionalInfo(), expectedInfo);
    }

    @Test(groups = "unit")
    public void testConstructor() throws IOException {
        check(Version.of(null), 0, 0, 0, "");
        check(Version.of(""), 0, 0, 0, "");
        check(Version.of("version one dot two"), 0, 0, 0, "");

        check(Version.of("1"), 1, 0, 0, "");
        check(Version.of("v1"), 1, 0, 0, "");
        check(Version.of("v1 & v2"), 1, 0, 0, "& v2");
        check(Version.of("version 1.2"), 1, 2, 0, "");
        check(Version.of("version 1.2.3.4.5"), 1, 2, 3, "4.5");

        check(Version.of("1.2.3 (rv: 12345678)"), 1, 2, 3, "(rv: 12345678)");
    }
}
