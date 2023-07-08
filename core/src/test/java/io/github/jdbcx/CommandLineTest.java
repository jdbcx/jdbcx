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

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class CommandLineTest {
    @DataProvider(name = "echoCommand")
    public static Object[][] getEchoCommand() {
        return new Object[][] { { Constants.IS_WINDOWS ? "cmd /c echo" : "echo" } };
    }

    @Test(groups = "unit")
    public void testToArray() throws IOException {
        Assert.assertThrows(NullPointerException.class, () -> CommandLine.toArray(null));
        Assert.assertEquals(CommandLine.toArray(""), new String[0]);
        Assert.assertEquals(CommandLine.toArray("\r \n \t"), new String[0]);

        Assert.assertEquals(CommandLine.toArray("a"), new String[] { "a" });
        Assert.assertEquals(CommandLine.toArray("a\t-1\n"), new String[] { "a", "-1" });
    }

    @Test(dataProvider = "echoCommand", groups = "unit")
    public void testCheck(String echoCommand) throws IOException {
        Assert.assertFalse(CommandLine.check("non_existing_command", 0));
        Assert.assertTrue(CommandLine.check(echoCommand, 0));
        Assert.assertTrue(CommandLine.check(echoCommand, 0, "Y"));
        Assert.assertTrue(CommandLine.check(echoCommand, 500));
        Assert.assertTrue(CommandLine.check(echoCommand, 500, "Y"));
    }

    @Test(dataProvider = "echoCommand", groups = "unit")
    public void testConstructor(String echoCommand) throws IOException {
        Assert.assertThrows(IllegalArgumentException.class,
                () -> new CommandLine("non_existing_command", null, null, 0, null, null, null, true));
        Assert.assertNotNull(new CommandLine("non_existing_command", null, null, 0, null, null, null, false));

        Assert.assertNotNull(new CommandLine(echoCommand, null, null, 0, null, null, null, true));
    }

    @Test(dataProvider = "echoCommand", groups = "unit")
    public void testExecute(String echoCommand) throws IOException {
        Assert.assertTrue(new CommandLine(echoCommand, null, null, 0, null, null, null, true).execute().length() > 0);
        Assert.assertThrows(IOException.class,
                () -> new CommandLine(Constants.IS_WINDOWS ? echoCommand : "ls", null, null, 0, null, null, null, true)
                        .execute("|"));
        Assert.assertEquals(
                new CommandLine(echoCommand, null, null, 0, null, null, null, true, "Y").execute("o", "k").trim(),
                "o k");
    }

    @Test
    public void testWslCli() throws IOException {
        if (!Constants.IS_WINDOWS) {
            throw new SkipException("Skip this is for windows only");
        }
        Assert.assertTrue(new CommandLine("wsl -- echo", null, null, 0, null, null, null, true).execute().length() > 0);
        Assert.assertThrows(IOException.class,
                () -> new CommandLine("wsl -- echo", null, null, 0, null, null, null, true).execute("|"));
        try (ByteArrayOutputStream out = new ByteArrayOutputStream(2048)) {
            CommandLine prqlc = new CommandLine("wsl -- /home/zhicwu/.cargo/bin/prqlc", null, null, 0, null, null, null,
                    true, "-V");
            prqlc.execute(3000, null, "from t", null, out, null, "compile", "-t", "sql.clickhouse");
            String sql = new String(out.toByteArray());
            sql = sql.replaceAll("[\\s\\t\\r\\n]*", "");
            sql = sql.substring(0, sql.indexOf("--"));
            Assert.assertEquals(sql, "SELECT*FROMt");
        }
    }
}
