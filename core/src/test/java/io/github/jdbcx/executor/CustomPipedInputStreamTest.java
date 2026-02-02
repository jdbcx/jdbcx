/*
 * Copyright 2022-2026, Zhichun Wu
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

import java.io.IOException;
import java.io.OutputStream;
import java.io.PipedOutputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import org.testng.Assert;
import org.testng.annotations.Test;

public class CustomPipedInputStreamTest {
    @Test(groups = { "unit" })
    public void testPipe() throws IOException {
        final PipedOutputStream output = new PipedOutputStream();
        final int total = 1024001;

        final int size = 63;
        final byte[] bytes = new byte[size];
        int index = 0;
        int len = 0;
        try (CustomPipedInputStream in = new CustomPipedInputStream(output, 1023, 0)) {
            in.attach(CompletableFuture.runAsync(
                    () -> {
                        try (OutputStream out = output) {
                            Thread.sleep(3000L);
                            for (int i = 0; i < total; i++) {
                                output.write((byte) (i % 256));
                                if (i == total / 2) {
                                    Thread.sleep(2000L);
                                }
                            }
                            Thread.sleep(3000L);
                        } catch (Throwable t) {
                            throw new CompletionException(t);
                        }
                    }));
            while ((len = in.read(bytes)) != -1) {
                for (int i = 0; i < len; i++) {
                    Assert.assertEquals(bytes[i], (byte) (index++ % 256));
                }
                // uncomment to enable random failure
                // Assert.assertEquals(len, total - index >= size ? size : total - index);
            }
            Assert.assertEquals(in.read(bytes), -1);
            Assert.assertEquals(in.read(), -1);
        }
    }

    @Test(groups = { "unit" })
    public void testPipeWithErrorOnRead() throws IOException {
        final PipedOutputStream output = new PipedOutputStream();
        final long magic = System.currentTimeMillis() % 99999 + 1;
        try (CustomPipedInputStream in = new CustomPipedInputStream(output, 1024, 0).attach(CompletableFuture.runAsync(
                () -> {
                    try (OutputStream out = output) {
                        for (int i = 0; i < magic; i++) {
                            output.write((byte) (i % 256));
                        }
                    } catch (Throwable t) {
                        Assert.assertEquals(t.getMessage(), "Surprise" + magic);
                        throw new CompletionException(t);
                    }
                }))) {
            int b = 0;
            int index = 0;
            while ((b = in.read()) != -1) {
                Assert.assertEquals((byte) b, (byte) (index++ % 256));
                throw new IllegalStateException("Surprise" + magic);
            }
        } catch (IllegalStateException e) {
            Assert.assertEquals(e.getMessage(), "Surprise" + magic);
        }
    }

    @Test(groups = { "unit" })
    public void testPipeWithErrorOnWrite() throws IOException {
        final PipedOutputStream output = new PipedOutputStream();
        final long magic = System.currentTimeMillis() % 99999 + 1;
        Throwable error = null;
        try (CustomPipedInputStream in = new CustomPipedInputStream(output, 1024, 0).attach(CompletableFuture.runAsync(
                () -> {
                    try (OutputStream out = output) {
                        for (int i = 0; i < magic; i++) {
                            output.write((byte) (i % 256));
                        }
                        throw new IllegalStateException("Surprise" + magic);
                    } catch (IOException t) {
                        throw new CompletionException(t);
                    }
                }))) {
            int b = 0;
            int index = 0;
            while ((b = in.read()) != -1) {
                Assert.assertEquals((byte) b, (byte) (index++ % 256));
            }
            Assert.assertEquals(in.read(), -1);
            Assert.assertEquals(in.read(new byte[5]), -1);
        } catch (IOException e) {
            error = e.getCause();
        }
        Assert.assertNotNull(error, "Should end up with an exception");
        Assert.assertEquals(error.getClass(), IllegalStateException.class);
        Assert.assertEquals(error.getMessage(), "Surprise" + magic);
    }

    @SuppressWarnings("resource")
    @Test(groups = { "unit" })
    public void testSmallPipe() throws IOException {
        final PipedOutputStream output = new PipedOutputStream();
        try (CustomPipedInputStream in = new CustomPipedInputStream(output, 2, 0).attach(CompletableFuture.runAsync(
                () -> {
                    try (OutputStream out = output) {
                        output.write(new byte[] { 1, 2, 3 });
                    } catch (IOException e) {
                        throw new CompletionException(e);
                    }
                }))) {
            byte[] bytes = new byte[5];
            Assert.assertEquals(in.read(bytes), 2);
            Assert.assertEquals(bytes[0], 1);
            Assert.assertEquals(bytes[1], 2);
            Assert.assertEquals(in.read(bytes), 1);
            Assert.assertEquals(bytes[0], 3);
            Assert.assertEquals(in.read(bytes), -1);
            Assert.assertEquals(in.read(bytes), -1);
        }
    }
}
