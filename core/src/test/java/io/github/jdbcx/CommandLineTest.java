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

public class CommandLineTest {
    @Test(groups = "unit")
    public void testCheck() throws IOException {
        Assert.assertFalse(CommandLine.check("non_existing_command", 0));
        Assert.assertTrue(CommandLine.check("ls", 0));
        Assert.assertTrue(CommandLine.check("ls", 0, "-a"));
        Assert.assertTrue(CommandLine.check("ls", 500));
        Assert.assertTrue(CommandLine.check("ls", 500, "-a"));
    }

    @Test(groups = "unit")
    public void testConstructor() throws IOException {
        Assert.assertNotNull(new CommandLine("ls", null, null, 0, null, null, null, true));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> new CommandLine("ls1", null, null, 0, null, null, null, true));
    }

    @Test(groups = "unit")
    public void testExecute() throws IOException {
        Assert.assertTrue(new CommandLine("ls", null, null, 0, null, null, null, true).execute().length() > 0);
        Assert.assertThrows(IOException.class,
                () -> new CommandLine("ls", null, null, 0, null, null, null, true).execute("-12345"));
        Assert.assertEquals(
                new CommandLine("curl", null, null, 0, null, null, null, true, "-V")
                        .execute("-s", "https://play.clickhouse.com").trim(),
                "Ok.");
    }
}
