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
import java.nio.charset.Charset;
import java.util.Properties;

import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class CommandLineTest {
    static Properties newProperties(Charset inputCharset, Charset outputCharset, int timeout, String workDir,
            String dockerCliPath, String dockerImage, String... testArgs) {
        Properties props = new Properties();
        if (inputCharset != null) {
            props.setProperty(CommandLine.OPTION_INPUT_CHARSET.getName(), inputCharset.name());
        }
        if (outputCharset != null) {
            props.setProperty(CommandLine.OPTION_OUTPUT_CHARSET.getName(), outputCharset.name());
        }
        props.setProperty(CommandLine.OPTION_CLI_TIMEOUT.getName(), String.valueOf(timeout));
        if (workDir != null) {
            props.setProperty(CommandLine.OPTION_WORK_DIRECTORY.getName(), workDir);
        }
        if (dockerCliPath != null) {
            props.setProperty(CommandLine.OPTION_DOCKER_PATH.getName(), dockerCliPath);
        }
        if (dockerImage != null) {
            props.setProperty(CommandLine.OPTION_DOCKER_IMAGE.getName(), dockerImage);
        }
        if (testArgs != null) {
            props.setProperty(CommandLine.OPTION_CLI_TEST_ARGS.getName(),
                    testArgs.length == 0 ? "" : String.join(" ", testArgs));
        }
        return props;
    }

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
                () -> new CommandLine("non_existing_command", true, newProperties(null, null, 0, null, null, null)));
        Assert.assertNotNull(
                new CommandLine("non_existing_command", false, newProperties(null, null, 0, null, null, null)));

        Assert.assertNotNull(new CommandLine(echoCommand, newProperties(null, null, 0, null, null, null)));
    }

    @Test(dataProvider = "echoCommand", groups = "unit")
    public void testExecute(String echoCommand) throws IOException {
        Assert.assertTrue(new CommandLine(echoCommand, true, newProperties(null, null, 0, null, null, null)).execute()
                .length() > 0);
        Assert.assertThrows(IOException.class,
                () -> new CommandLine(Constants.IS_WINDOWS ? echoCommand : "ls", true,
                        newProperties(null, null, 0, null, null, null))
                        .execute("|"));
        Assert.assertEquals(
                new CommandLine(echoCommand, true, newProperties(null, null, 0, null, null, null, "Y"))
                        .execute("o", "k").trim(),
                "o k");
    }

    @Test
    public void testWslCli() throws IOException {
        if (!Constants.IS_WINDOWS) {
            throw new SkipException("Skip this is for windows only");
        }
        Assert.assertTrue(new CommandLine("wsl -- echo", true, newProperties(null, null, 0, null, null, null)).execute()
                .length() > 0);
        Assert.assertThrows(IOException.class,
                () -> new CommandLine("wsl -- echo", true, newProperties(null, null, 0, null, null, null))
                        .execute("|"));
        try (ByteArrayOutputStream out = new ByteArrayOutputStream(Constants.DEFAULT_BUFFER_SIZE)) {
            CommandLine prqlc = new CommandLine("wsl -- /home/zhicwu/.cargo/bin/prqlc", true,
                    newProperties(null, null, 0, null, null, null, "-V"));
            prqlc.execute(3000, null, "from t", null, out, null, "compile", "-t", "sql.clickhouse");
            String sql = new String(out.toByteArray());
            sql = sql.replaceAll("[\\s\\t\\r\\n]*", "");
            sql = sql.substring(0, sql.indexOf("--"));
            Assert.assertEquals(sql, "SELECT*FROMt");
        }
    }
}
