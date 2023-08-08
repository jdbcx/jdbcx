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
package io.github.jdbcx.executor;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PipedOutputStream;
import java.io.Reader;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.WeakHashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import io.github.jdbcx.Checker;
import io.github.jdbcx.Constants;
import io.github.jdbcx.Logger;
import io.github.jdbcx.LoggerFactory;
import io.github.jdbcx.Option;
import io.github.jdbcx.Utils;

/**
 * This class encapsulate a command line providing the ability to execute it
 * with arguments.
 */
public class CommandLineExecutor extends AbstractExecutor {
    private static final Logger log = LoggerFactory.getLogger(CommandLineExecutor.class);

    private static final Map<String, Boolean> cache = Collections.synchronizedMap(new WeakHashMap<>(8));

    public static final Option OPTION_CLI_PATH = Option.of("cli.path", "Command line, for examples: ", "");
    public static final Option OPTION_CLI_TEST_ARGS = Option.of("cli.test.args",
            "Arguments to validate both the correctness and existence of the command line, usually '-V' or '-v'", "");
    public static final Option OPTION_DOCKER_PATH = Option.of("docker.path",
            "Path to the Docker or Podman command line, leave it empty to attempt using Docker first and, if not available, fallback to Podman.",
            "");
    public static final Option OPTION_DOCKER_IMAGE = Option.of("docker_image",
            "Docker image to use, leave it empty if you don't want to use Docker or Podman", "");
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
    private final Path defaultWorkDir;

    public CommandLineExecutor(String command) {
        this(command, new Properties());
    }

    public CommandLineExecutor(String command, Properties props) {
        this(command, true, props);
    }

    public CommandLineExecutor(String command, boolean validate, Properties props) {
        super(props);

        command = Utils.normalizePath(OPTION_CLI_PATH.getValue(props, command));

        String inputCharset = Option.INPUT_CHARSET.getValue(props);
        String outputCharset = Option.OUTPUT_CHARSET.getValue(props);
        String workDir = OPTION_WORK_DIRECTORY.getValue(props);
        String dockerCliPath = OPTION_DOCKER_PATH.getValue(props);
        String dockerImage = OPTION_DOCKER_IMAGE.getValue(props);
        String[] testArgs = CommandLineExecutor.toArray(OPTION_CLI_TEST_ARGS.getValue(props));

        this.defaultInputCharset = Checker.isNullOrBlank(inputCharset) ? Charset.forName(inputCharset)
                : Constants.DEFAULT_CHARSET;
        this.defaultOutputCharset = Checker.isNullOrBlank(outputCharset) ? Charset.forName(outputCharset)
                : Constants.DEFAULT_CHARSET;

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

    public Path getDefaultWorkDirectory() {
        return defaultWorkDir;
    }

    public Charset getInputCharset(Properties props) {
        String value = props != null ? props.getProperty(Option.INPUT_CHARSET.getName()) : null;
        return value != null ? Charset.forName(value) : defaultInputCharset;
    }

    public Charset getOutputCharset(Properties props) {
        String value = props != null ? props.getProperty(Option.OUTPUT_CHARSET.getName()) : null;
        return value != null ? Charset.forName(value) : defaultOutputCharset;
    }

    public int getParallelism(Properties props) {
        String value = props != null ? props.getProperty(Option.EXEC_PARALLELISM.getName()) : null;
        return value != null ? Integer.parseInt(value) : defaultParallelism;
    }

    public int getTimeout(Properties props) {
        String value = props != null ? props.getProperty(Option.EXEC_TIMEOUT.getName()) : null;
        return value != null ? Integer.parseInt(value) : defaultTimeout;
    }

    public Path getWorkDirectory(Properties props) {
        final Path workDir;
        String value = props != null ? props.getProperty(OPTION_WORK_DIRECTORY.getName()) : null;
        if (value != null) {
            if (Checker.isNullOrEmpty(value)) {
                workDir = Paths.get(Constants.CURRENT_DIR);
            } else {
                workDir = Utils.getPath(value, true);
            }
        } else {
            workDir = defaultWorkDir;
        }
        return workDir;
    }

    @SuppressWarnings("resource")
    public InputStream execute(Properties props, InputStream input, String... args)
            throws IOException, TimeoutException {
        final int parallelism = getParallelism(props);
        final int timeout = getTimeout(props);
        final Path workDir = getWorkDirectory(props);
        final Charset inputCharset = getInputCharset(props);
        final Charset outputCharset = getOutputCharset(props);

        if (parallelism <= 0) {
            try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
                int exitCode = execute(parallelism, timeout, workDir, input, inputCharset, out, outputCharset, args);
                if (exitCode != 0) {
                    throw new IllegalStateException(new String(out.toByteArray(), outputCharset));
                }
                return new ByteArrayInputStream(out.toByteArray());
            }
        }

        final PipedOutputStream out = new PipedOutputStream();
        return new CustomPipedInputStream(out, Constants.DEFAULT_BUFFER_SIZE, timeout)
                .attach(runAsync(() -> {
                    try (OutputStream o = out) {
                        int exitCode = execute(parallelism, timeout, workDir, input, inputCharset, o, outputCharset,
                                args);
                        if (exitCode != 0) {
                            throw new IllegalStateException(
                                    Utils.format("Comamnd exited with code %d", exitCode));
                        }
                    } catch (TimeoutException | IOException e) {
                        throw new CompletionException(e);
                    }
                }));
    }

    protected CompletableFuture<?> handleProcessOutput(int parallelism, OutputStream processOutput, Object input,
            Charset inputCharset) throws IOException {
        CompletableFuture<?> future = null;
        if (input instanceof InputStream) {
            if (parallelism > 0) {
                future = runAsync(() -> Stream.pipeAsync((InputStream) input, processOutput, true));
            } else {
                Stream.pipe((InputStream) input, processOutput, true);
            }
        } else if (input instanceof Reader) {
            if (parallelism > 0) {
                future = runAsync(
                        () -> Stream.pipeAsync((Reader) input, new OutputStreamWriter(processOutput, inputCharset),
                                true));
            } else {
                Stream.pipe((Reader) input, new OutputStreamWriter(processOutput, inputCharset), true);
            }
        } else if (input instanceof byte[]) {
            try (OutputStream out = processOutput) {
                processOutput.write((byte[]) input);
            }
        } else if (input instanceof ByteBuffer) {
            try (OutputStream out = processOutput) {
                ByteBuffer buf = (ByteBuffer) input;
                int remaining = buf.remaining();
                if (remaining > 0) {
                    byte[] b = new byte[remaining];
                    buf.get(b);
                    processOutput.write(b);
                }
            }
        } else if (input != null) { // treat as string
            try (OutputStream out = processOutput) {
                processOutput.write(input.toString().getBytes(inputCharset));
            }
        }
        return future;
    }

    protected CompletableFuture<?> handleProcessInput(int parallelism, InputStream processInput, Object output,
            Charset outputCharset)
            throws IOException {
        CompletableFuture<?> future = null;
        if (output instanceof OutputStream) {
            if (parallelism > 0) {
                future = runAsync(() -> Stream.pipeAsync(processInput, (OutputStream) output));
            } else {
                Stream.pipe(processInput, (OutputStream) output);
            }
        } else if (output instanceof Writer) {
            if (parallelism > 0) {
                future = runAsync(
                        () -> Stream.pipeAsync(new InputStreamReader(processInput, outputCharset), (Writer) output));
            } else {
                Stream.pipe(new InputStreamReader(processInput, outputCharset), (Writer) output);
            }
        } else if (output instanceof byte[]) {
            try (InputStream in = processInput) {
                byte[] bytes = (byte[]) output;
                if (bytes.length > 0) {
                    int len = in.read(bytes);
                    if (len < bytes.length) {
                        log.warn("Only %d bytes were read into the byte array, which has a capacity of %d.", len,
                                bytes.length);
                    }
                }
            }
        } else if (output instanceof ByteBuffer) {
            try (InputStream in = processInput) {
                ByteBuffer buf = (ByteBuffer) output;
                int remaining = buf.remaining();
                if (remaining > 0) {
                    byte[] bytes = new byte[remaining];
                    int len = in.read(bytes);
                    buf.put(bytes, 0, len);
                }
            }
        } else if (output != null) {
            try (InputStream in = processInput) {
                log.warn("Unsupported output object: [%s]", output);
            }
        }
        return future;
    }

    /**
     * Executes the command with specified arguments.
     *
     * @param parallelism   parallelism
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
     * @throws IOException      when failed to execute the command
     * @throws TimeoutException when execution timed out
     */
    public int execute(int parallelism, int timeout, Path workDir, Object input, Charset inputCharset, Object output,
            Charset outputCharset, String... args) throws IOException, TimeoutException {
        if (inputCharset == null) {
            inputCharset = getDefaultInputCharset();
        }
        if (outputCharset == null) {
            outputCharset = getDefaultOutputCharset();
        }

        final long startTime = timeout <= 0 ? 0L : System.currentTimeMillis();
        final long timeoutMs = timeout;

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

        List<CompletableFuture<?>> tasks = new ArrayList<>(2);
        CompletableFuture<?> future = null;
        Process p = null;
        try {
            p = builder.start();

            // timeout is better handled when parallelism=1
            future = handleProcessOutput(parallelism--, p.getOutputStream(), fileInput ? null : input, inputCharset);
            tasks.add(future);
            if (parallelism <= 0) {
                tasks.remove(future);
                waitForTask(log, future, startTime, timeoutMs);
            }

            future = handleProcessInput(parallelism--, p.getInputStream(), fileOutput ? null : output, outputCharset);
            tasks.add(future);
            if (parallelism <= 0) {
                tasks.remove(future);
                waitForTask(log, future, startTime, timeoutMs);
            }

            final long remain = checkTimeout(log, startTime, timeoutMs);

            InputStream stdErr = p.getErrorStream();
            int exitValue;
            if (timeout <= 0) {
                exitValue = p.waitFor();
            } else if (p.waitFor(remain, TimeUnit.MILLISECONDS)) {
                exitValue = p.exitValue();
            } else {
                throw handleTimeout(log, timeout, tasks.toArray(new CompletableFuture[0]));
            }

            if (stdErr != null) {
                try (InputStream in = stdErr;
                        ByteArrayOutputStream out = new ByteArrayOutputStream(Constants.DEFAULT_BUFFER_SIZE)) {
                    if (Stream.pipe(in, out) > 0L) {
                        if (exitValue != 0) {
                            throw new IOException(new String(out.toByteArray(), Constants.DEFAULT_CHARSET));
                        } else {
                            log.debug(
                                    "The command line executed successfully, and the following output was captured in STDERR.\n%s",
                                    new String(out.toByteArray(), Constants.DEFAULT_CHARSET));
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
