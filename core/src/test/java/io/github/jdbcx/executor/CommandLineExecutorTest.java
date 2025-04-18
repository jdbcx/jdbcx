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
package io.github.jdbcx.executor;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import io.github.jdbcx.Constants;
import io.github.jdbcx.Option;

public class CommandLineExecutorTest {
    static Properties newProperties(Charset inputCharset, Charset outputCharset, int timeout, String workDir,
            String dockerCliPath, String dockerImage, String... testArgs) {
        Properties props = new Properties();
        if (inputCharset != null) {
            props.setProperty(Option.INPUT_CHARSET.getName(), inputCharset.name());
        }
        if (outputCharset != null) {
            props.setProperty(Option.OUTPUT_CHARSET.getName(), outputCharset.name());
        }
        props.setProperty(Option.EXEC_TIMEOUT.getName(), String.valueOf(timeout));
        if (workDir != null) {
            props.setProperty(Option.WORK_DIRECTORY.getName(), workDir);
        }
        if (dockerCliPath != null) {
            props.setProperty(CommandLineExecutor.OPTION_DOCKER_PATH.getName(), dockerCliPath);
        }
        if (dockerImage != null) {
            props.setProperty(CommandLineExecutor.OPTION_DOCKER_IMAGE.getName(), dockerImage);
        }
        if (testArgs != null) {
            props.setProperty(CommandLineExecutor.OPTION_CLI_TEST_ARGS.getName(),
                    testArgs.length == 0 ? "" : String.join(" ", testArgs));
        }
        return props;
    }

    @DataProvider(name = "echoCommand")
    public static Object[][] getEchoCommand() {
        return new Object[][] { { Constants.IS_WINDOWS ? "cmd /c echo" : "echo" } };
    }

    @DataProvider(name = "echoScript")
    public static Object[][] getEchoScript() {
        return new Object[][] { { Constants.IS_WINDOWS ? "cmd /c" : "sh -c", "echo_jdbcx.sh" } };
    }

    @Test(groups = "unit")
    public void testToArray() {
        Assert.assertThrows(NullPointerException.class, () -> CommandLineExecutor.toArray(null));
        Assert.assertEquals(CommandLineExecutor.toArray(""), new String[0]);
        Assert.assertEquals(CommandLineExecutor.toArray("\r \n \t"), new String[0]);

        Assert.assertEquals(CommandLineExecutor.toArray("a"), new String[] { "a" });
        Assert.assertEquals(CommandLineExecutor.toArray("a\t-1\n"), new String[] { "a", "-1" });
    }

    @Test(dataProvider = "echoCommand", groups = "unit")
    public void testCheck(String echoCommand) {
        Assert.assertFalse(CommandLineExecutor.check("non_existing_command", 0));
        Assert.assertTrue(CommandLineExecutor.check(echoCommand, 0));
        Assert.assertTrue(CommandLineExecutor.check(echoCommand, 0, "Y"));
        Assert.assertTrue(CommandLineExecutor.check(echoCommand, 500));
        Assert.assertTrue(CommandLineExecutor.check(echoCommand, 500, "Y"));
    }

    @Test(dataProvider = "echoCommand", groups = "unit")
    public void testConstructor(String echoCommand) {
        Assert.assertThrows(IllegalArgumentException.class,
                () -> new CommandLineExecutor("non_existing_command", true,
                        null, newProperties(null, null, 0, null, null, null)));
        Assert.assertNotNull(
                new CommandLineExecutor("non_existing_command", false, null,
                        newProperties(null, null, 0, null, null, null)));

        Assert.assertNotNull(
                new CommandLineExecutor(echoCommand, null, newProperties(null, null, 0, null, null, null)));
    }

    @Test(dataProvider = "echoCommand", groups = "unit")
    public void testExecute(String echoCommand) throws IOException, TimeoutException {
        Assert.assertTrue(
                Stream.readAllAsString(
                        new CommandLineExecutor(echoCommand, true, null, newProperties(null, null, 0, null, null, null))
                                .execute(null, null))
                        .length() > 0);
        Assert.assertThrows(IOException.class,
                () -> Stream.readAllAsString(new CommandLineExecutor(Constants.IS_WINDOWS ? echoCommand : "ls", true,
                        null, newProperties(null, null, 0, null, null, null))
                        .execute(null, null, "|")));
        Assert.assertEquals(
                Stream.readAllAsString(
                        new CommandLineExecutor(echoCommand, true, null,
                                newProperties(null, null, 0, null, null, null, "Y"))
                                .execute(null, null, "o", "k"))
                        .trim(),
                "o k");
    }

    @Test(dataProvider = "echoScript", groups = "unit")
    public void testInputFile(String cmd, String echoScript) throws IOException, TimeoutException {
        Properties props = newProperties(null, null, 0, null, null, null);

        Option.INPUT_FILE.setValue(props, "target/test-classes/non-existent.file");
        Assert.assertThrows(IllegalArgumentException.class,
                () -> new CommandLineExecutor(cmd, false, null, props)
                        .execute(props, null));

        Option.INPUT_FILE.setValue(props, "target/test-classes/queries/" + echoScript);
        Assert.assertEquals(
                Stream.readAllAsString(
                        new CommandLineExecutor(cmd, false, null, props)
                                .execute(props, null, "o", "k"))
                        .trim(),
                "jdbcx");
    }

    @Test
    public void testWslCli() throws IOException, TimeoutException {
        if (!Constants.IS_WINDOWS) {
            throw new SkipException("Skip this is for windows only");
        }
        Assert.assertTrue(
                Stream.readAllAsString(
                        new CommandLineExecutor("wsl -- echo", true, null,
                                newProperties(null, null, 0, null, null, null))
                                .execute(null, null))
                        .length() > 0);
        Assert.assertThrows(IOException.class,
                () -> new CommandLineExecutor("wsl -- echo", true, null, newProperties(null, null, 0, null, null, null))
                        .execute(null, null, "|"));
        try (ByteArrayOutputStream out = new ByteArrayOutputStream(Constants.DEFAULT_BUFFER_SIZE)) {
            CommandLineExecutor prqlc = new CommandLineExecutor("wsl -- /home/zhicwu/.cargo/bin/prqlc", true,
                    null, newProperties(null, null, 0, null, null, null, "-V"));
            prqlc.execute(0, true, 3000, null, "from t", null, out, null, "compile", "-t", "sql.clickhouse");
            String sql = new String(out.toByteArray());
            sql = sql.replaceAll("[\\s\\t\\r\\n]*", "");
            sql = sql.substring(0, sql.indexOf("--"));
            Assert.assertEquals(sql, "SELECT*FROMt");
        }
    }
}
