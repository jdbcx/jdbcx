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

import java.io.File;
import java.io.IOException;

import org.testng.Assert;
import org.testng.annotations.Test;

public class UtilsTest {
    @Test(groups = "unit")
    public void testNormalizePath() throws IOException {
        Assert.assertEquals(Utils.normalizePath(null), "");
        Assert.assertEquals(Utils.normalizePath(""), "");
        Assert.assertEquals(Utils.normalizePath("http://s.com/term?w=1"), "http://s.com/term?w=1");

        Assert.assertEquals(Utils.normalizePath("~/"), Constants.HOME_DIR);
        Assert.assertEquals(Utils.normalizePath("~/a"), Constants.HOME_DIR + File.separatorChar + "a");
        Assert.assertEquals(Utils.normalizePath("~/a b c"), Constants.HOME_DIR + File.separatorChar + "a b c");
        Assert.assertEquals(Utils.normalizePath("~/a:b-c"), Constants.HOME_DIR + File.separatorChar + "a:b-c");
        Assert.assertEquals(Utils.normalizePath("~/test/dir,/a"),
                Constants.HOME_DIR + File.separatorChar + "test/dir,/a");

        if (Constants.IS_WINDOWS) {
            Assert.assertEquals(Utils.normalizePath("~/\\a"), Constants.HOME_DIR + File.separatorChar + "\\a");
            Assert.assertEquals(Utils.normalizePath("~/D:\\a\\b\\c"),
                    Constants.HOME_DIR + File.separatorChar + "D:\\a\\b\\c");
        } else {
            Assert.assertEquals(Utils.normalizePath("~//a"), Constants.HOME_DIR + File.separatorChar + "/a");
        }
    }
}
