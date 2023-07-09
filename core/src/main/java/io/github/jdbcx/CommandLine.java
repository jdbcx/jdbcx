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
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.WeakHashMap;
import java.util.concurrent.TimeUnit;

/**
 * This class encapsulate a command line providing the ability to execute it
 * with arguments.
 */
public class CommandLine {
    private static final Logger log = LoggerFactory.getLogger(CommandLine.class);

    private static final Map<String, Boolean> cache = Collections.synchronizedMap(new WeakHashMap<>(8));

    public static final Option OPTION_CLI_ERROR = Option.of(new String[] { "cli.error", "The approach to handle error",
            Option.ERROR_HANDLING_IGNORE, Option.ERROR_HANDLING_THROW, Option.ERROR_HANDLING_WARN });
    public static final Option OPTION_CLI_PATH = Option.of("cli.path", "Command line, for examples: ", "");
    public static final Option OPTION_CLI_TIMEOUT = Option.of("cli.timeout",
            "Command line timeout in milliseconds, a negative number or zero disables timeout", "0");
    public static final Option OPTION_CLI_TEST_ARGS = Option.of("cli.test.args",
            "Arguments to validate both the correctness and existence of the command line, usually '-V' or '-v'", "");
    public static final Option OPTION_DOCKER_PATH = Option.of("docker.path",
            "Path to the Docker or Podman command line, leave it empty to attempt using Docker first and, if not available, fallback to Podman.",
            "");
    public static final Option OPTION_DOCKER_IMAGE = Option.of("docker_image",
            "Docker image to use, leave it empty if you don't want to use Docker or Podman", "");
    public static final Option OPTION_INPUT_CHARSET = Option.of("input.charset", "Charset used for command line input",
            "utf-8");
    public static final Option OPTION_OUTPUT_CHARSET = Option.of("output.charset",
            "Charset used for command line output", "utf-8");
    public static final Option OPTION_WORK_DIRECTORY = Option.of("work.dir",
            "Work directory, leave it empty to use current directory", "");

    static String[] toArray(String command) {
        String[] array = command.split("\\s");
        if (array.length == 0) {
            return array;
        }

        List<String> list = new ArrayList<>(array.length);
        for (String str : array) {
            if (!Checker.isNullOrBlank(str)) {
                list.add(str);
            }
        }
        return list.toArray(Constants.EMPTY_STRING_ARRAY);
    }

    public static final boolean check(String command, int timeout, String... args) {
        if (Checker.isNullOrBlank(command) || args == null) {
            throw new IllegalArgumentException("Non-blank command and non-null arguments are required");
        }

        StringBuilder builder = new StringBuilder(command);
        for (String str : args) {
            builder.append(' ').append(str);
        }
        String commandLine = builder.toString();
        Boolean value = cache.get(commandLine);
        if (value == null) {
            value = Boolean.FALSE;

            String[] array = toArray(command);
            List<String> list = new ArrayList<>(array.length + args.length);
            Collections.addAll(list, array);
            Collections.addAll(list, args);
            Process process = null;
            try {
                process = new ProcessBuilder(list).start();
                process.getOutputStream().close();

                if (timeout <= 0) {
                    value = process.waitFor() == 0;
                } else if (process.waitFor(timeout, TimeUnit.MILLISECONDS)) {
                    int exitValue = process.exitValue();
                    if (exitValue != 0) {
                        log.debug("The command \"%s\" exited with code %d", list, exitValue);
                    }
                    value = exitValue == 0;
                } else {
                    log.debug("The command \"%s\" timed out after waiting for %d ms to complete.", list, timeout);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                log.debug("Failed to check the status of the command \"%s\" due to %s", list, e.getMessage());
            } finally {
                if (process != null && process.isAlive()) {
                    process.destroyForcibly();
                }
                process = null;
            }

            if (value) { // no negative cache
                cache.put(commandLine, value);
            }
        }

        return Boolean.TRUE.equals(value);
    }

    private final List<String> command;
    private final Charset defaultInputCharset;
    private final Charset defaultOutputCharset;
    private final int defaultTimeout;
    private final Path defaultWorkDir;

    private final String errorHandling;

    public CommandLine(String command) {
        this(command, new Properties());
    }

    public CommandLine(String command, Properties props) {
        this(command, true, props);
    }

    public CommandLine(String command, boolean validate, Properties props) {
        command = Utils.normalizePath(OPTION_CLI_PATH.getValue(props, command));

        String inputCharset = OPTION_INPUT_CHARSET.getValue(props);
        String outputCharset = OPTION_OUTPUT_CHARSET.getValue(props);
        String timeout = OPTION_CLI_TIMEOUT.getValue(props);
        String workDir = OPTION_WORK_DIRECTORY.getValue(props);
        String dockerCliPath = OPTION_DOCKER_PATH.getValue(props);
        String dockerImage = OPTION_DOCKER_IMAGE.getValue(props);
        String[] testArgs = CommandLine.toArray(OPTION_CLI_TEST_ARGS.getValue(props));

        this.defaultInputCharset = Checker.isNullOrBlank(inputCharset) ? Charset.forName(inputCharset)
                : StandardCharsets.UTF_8;
        this.defaultOutputCharset = Checker.isNullOrBlank(outputCharset) ? Charset.forName(outputCharset)
                : StandardCharsets.UTF_8;

        this.defaultTimeout = Integer.parseInt(timeout);

        if (Checker.isNullOrEmpty(workDir)) {
            this.defaultWorkDir = Paths.get(Constants.CURRENT_DIR);
        } else {
            this.defaultWorkDir = Utils.getPath(workDir, true);
        }

        if (!validate || check(command, 0, testArgs)) {
            this.command = Collections.unmodifiableList(Arrays.asList(toArray(command)));
        } else if (Checker.isNullOrEmpty(dockerImage)) { // not going to use docker/podman
            throw new IllegalArgumentException(Utils.format("The \"%s\" command was not found.", command));
        } else {
            String dockerCli;
            if (Checker.isNullOrEmpty(dockerCliPath)
                    && !check(dockerCli = Utils.getPath(dockerCliPath, false).toString(), 0, "-v")) {
                throw new IllegalArgumentException(Utils.format("The \"%s\" command was not found.", dockerCli));
            } else if (!check(dockerCli = "docker", 0, "-v")) {
                if (!check(dockerCli = "podman", 0, "-v")) {
                    throw new IllegalArgumentException("Both \"docker\" and \"podman\" commands were not found.");
                }
            }

            // now let's check if we can get the docker image or not
            if (!check(dockerCli, 0, "pull", "-q", dockerImage)) {
                throw new IllegalArgumentException(
                        Utils.format("Failed to pull the docker image \"%s\".", dockerImage));
            }
            this.command = Collections.unmodifiableList(Arrays.asList(dockerCli, "--rm", "-i", dockerImage, command));
        }

        this.errorHandling = OPTION_CLI_ERROR.getValue(props);
    }

    public List<String> getCommand() {
        return command;
    }

    public Charset getDefaultInputCharset() {
        return defaultInputCharset;
    }

    public Charset getDefaultOutputCharset() {
        return defaultOutputCharset;
    }

    public int getDefaultTimeout() {
        return defaultTimeout;
    }

    public Path getDefaultWorkDirectory() {
        return defaultWorkDir;
    }

    public boolean ignoreError() {
        return Option.ERROR_HANDLING_IGNORE.equals(errorHandling);
    }

    public boolean throwExceptionOnError() {
        return Option.ERROR_HANDLING_THROW.equals(errorHandling);
    }

    public boolean warnOnError() {
        return Option.ERROR_HANDLING_WARN.equals(errorHandling);
    }

    public String execute(String... args) throws IOException {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream(2048)) {
            execute(defaultTimeout, defaultWorkDir, null, defaultInputCharset, out, defaultOutputCharset, args);
            return new String(out.toByteArray(), StandardCharsets.UTF_8);
        }
    }

    /**
     * Executes the command with specified arguments.
     *
     * @param timeout       timeout in milliseoncds, a negative number or zero
     *                      disables timeout
     * @param workDir       optional work directory, {@code null} is same as
     *                      {@link #getDefaultWorkDirectory()}
     * @param input         optional input of the command, could be a {@link File},
     *                      {@link InputStream}, {@link Reader}, {@code byte[]},
     *                      {@link ByteBuffer}, or any non-null object can be
     *                      converted to {@link String}
     * @param inputCharset  optional charset for input, {@code null} is same as
     *                      {@link #getDefaultInputCharset()}
     * @param output        optional output of the command, could be a
     *                      {@link File},{@link OutputStream},{@link Writer},{@code byte[]},
     *                      or {@link ByteBuffer}
     * @param outputCharset optional charset for output, {@code null} is same as
     *                      {@link #getDefaultOutputCharset()}
     * @param args          optional arguments of the command
     * @return exit code, {@code 0} means success
     * @throws IOException when failed to execute the command
     */
    public int execute(int timeout, Path workDir, Object input, Charset inputCharset, Object output,
            Charset outputCharset, String... args) throws IOException {
        if (inputCharset == null) {
            inputCharset = getDefaultInputCharset();
        }
        if (outputCharset == null) {
            outputCharset = getDefaultOutputCharset();
        }

        List<String> commands = new ArrayList<>(command.size() + args.length);
        commands.addAll(command);
        for (String arg : args) {
            if (!Checker.isNullOrBlank(arg)) {
                commands.add(arg);
            }
        }
        ProcessBuilder builder = new ProcessBuilder(commands);
        if (workDir != null) {
            builder.directory(workDir.toFile());
        }

        boolean fileInput = input instanceof File;
        boolean fileOutput = output instanceof File;
        if (fileInput) {
            builder.redirectInput((File) input);
        }
        if (fileOutput) {
            builder.redirectOutput((File) output);
        }

        Process p = null;
        try {
            p = builder.start();
            try (OutputStream out = p.getOutputStream()) {
                if (input instanceof InputStream) {
                    try (InputStream in = (InputStream) input) {
                        byte[] bytes = new byte[2048];
                        int len = 0;
                        while ((len = in.read(bytes)) != -1) {
                            out.write(bytes, 0, len);
                        }
                        out.flush();
                    }
                } else if (input instanceof Reader) {
                    try (Reader reader = (Reader) input;
                            OutputStreamWriter writer = new OutputStreamWriter(out, inputCharset)) {
                        char[] chars = new char[1024];
                        int len = 0;
                        while ((len = reader.read(chars)) != -1) {
                            writer.write(chars, 0, len);
                        }
                        writer.flush();
                    }
                } else if (input instanceof byte[]) {
                    p.getOutputStream().write((byte[]) input);
                } else if (input instanceof ByteBuffer) {
                    ByteBuffer buf = (ByteBuffer) input;
                    int remaining = buf.remaining();
                    if (remaining > 0) {
                        byte[] b = new byte[remaining];
                        buf.get(b);
                        p.getOutputStream().write(b);
                    }
                } else if (!fileInput && input != null) { // treat as string
                    p.getOutputStream().write(input.toString().getBytes(inputCharset));
                }
            }

            try (InputStream in = p.getInputStream()) {
                if (output instanceof OutputStream) {
                    try (OutputStream out = (OutputStream) output) {
                        byte[] bytes = new byte[2048];
                        int len = 0;
                        while ((len = in.read(bytes)) != -1) {
                            out.write(bytes, 0, len);
                        }
                        out.flush();
                    }
                } else if (output instanceof Writer) {
                    try (Reader reader = new InputStreamReader(in, outputCharset); Writer writer = (Writer) output) {
                        char[] chars = new char[1024];
                        int len = 0;
                        while ((len = reader.read(chars)) != -1) {
                            writer.write(chars, 0, len);
                        }
                        writer.flush();
                    }
                } else if (output instanceof byte[]) {
                    byte[] bytes = (byte[]) output;
                    if (bytes.length > 0) {
                        int len = in.read(bytes);
                        if (len < bytes.length) {
                            log.warn("Only %d bytes were read into the byte array, which has a capacity of %d.", len,
                                    bytes.length);
                        }
                    }
                } else if (output instanceof ByteBuffer) {
                    ByteBuffer buf = (ByteBuffer) output;
                    int remaining = buf.remaining();
                    if (remaining > 0) {
                        byte[] bytes = new byte[remaining];
                        int len = in.read(bytes);
                        buf.put(bytes, 0, len);
                    }
                } else if (!fileOutput && output != null) {
                    log.warn("Unsupported output object: [%s]", output);
                }
            }

            InputStream stdErr = p.getErrorStream();
            int exitValue;
            if (timeout <= 0) {
                exitValue = p.waitFor();
            } else if (p.waitFor(timeout, TimeUnit.MILLISECONDS)) {
                exitValue = p.exitValue();
            } else {
                throw new InterruptedIOException(
                        Utils.format("The execution was interrupted by a timeout after waiting for %d ms.", timeout));
            }

            if (stdErr != null) {
                byte[] bytes = new byte[2048];
                try (InputStream in = stdErr; ByteArrayOutputStream out = new ByteArrayOutputStream(bytes.length)) {
                    int len = 0;
                    while ((len = in.read(bytes)) != -1) {
                        out.write(bytes, 0, len);
                    }

                    if (out.size() > 0) {
                        if (exitValue != 0) {
                            throw new IOException(new String(out.toByteArray(), StandardCharsets.UTF_8));
                        } else {
                            log.debug(
                                    "The command line executed successfully, and the following output was captured in STDERR.\n",
                                    new String(out.toByteArray(), StandardCharsets.UTF_8));
                        }
                    }
                }
            }

            return exitValue;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new InterruptedIOException("The execution was interrupted due to " + e.getMessage());
        } finally {
            if (p != null && p.isAlive()) {
                p.destroyForcibly();
            }
            p = null;
        }
    }
}
