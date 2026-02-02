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
package io.github.jdbcx.compress;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import io.github.jdbcx.Compression;
import io.github.jdbcx.CompressionProvider;
import io.github.jdbcx.Utils;

public final class SnappySupport {
    public static final int DEFAULT_BUFFER_SIZE = 8192;

    public static final int MIN_BLOCK_SIZE = 1024;
    public static final int DEFAULT_BLOCK_SIZE = 32 * 1024;
    public static final int MAX_BLOCK_SIZE = 512 * 1024 * 1024;

    public static class DefaultProvider implements CompressionProvider {
        @Override
        public OutputStream compress(OutputStream output, int level, int bufferSize) throws IOException {
            return new org.apache.commons.compress.compressors.snappy.SnappyCompressorOutputStream(output,
                    normalizeBuffer(bufferSize));
        }

        @Override
        public InputStream decompress(InputStream input, int level, int bufferSize) throws IOException {
            return new org.apache.commons.compress.compressors.snappy.SnappyCompressorInputStream(input,
                    normalizeLevel(level));
        }

        @Override
        public Compression getAlgorithm() {
            return Compression.SNAPPY;
        }
    }

    public static class PreferredProvider implements CompressionProvider {
        public PreferredProvider() {
            new org.xerial.snappy.Snappy(); // NOSONAR
        }

        @Override
        public OutputStream compress(OutputStream output, int level, int bufferSize) throws IOException {
            return new org.xerial.snappy.SnappyOutputStream(output, normalizeLevel(level));
        }

        @Override
        public InputStream decompress(InputStream input, int level, int bufferSize) throws IOException {
            return new org.xerial.snappy.SnappyInputStream(input);
        }

        @Override
        public Compression getAlgorithm() {
            return Compression.SNAPPY;
        }
    }

    static final class Factory {
        private static final CompressionProvider instance = Utils.createInstance(PreferredProvider.class,
                DefaultProvider.class, CompressionProvider.autoDetect());

        private Factory() {
        }
    }

    static int normalizeBuffer(int bufferSize) { // NOSONAR
        return bufferSize < DEFAULT_BUFFER_SIZE ? DEFAULT_BUFFER_SIZE : bufferSize;
    }

    static int normalizeLevel(int level) {
        return level < MIN_BLOCK_SIZE || level > MAX_BLOCK_SIZE ? DEFAULT_BLOCK_SIZE : (level - level % 1024);
    }

    public static CompressionProvider getInstance() {
        return Factory.instance;
    }

    private SnappySupport() {
    }
}
