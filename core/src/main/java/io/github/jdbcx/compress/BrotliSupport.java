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
package io.github.jdbcx.compress;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import io.github.jdbcx.Compression;
import io.github.jdbcx.CompressionProvider;
import io.github.jdbcx.Utils;

public final class BrotliSupport {
    public static final int DEFAULT_BUFFER_SIZE = 16384;

    public static final int MIN_LEVEL = 0;
    public static final int MAX_LEVEL = 11;

    public static class DefaultProvider implements CompressionProvider {
        @Override
        public InputStream decompress(InputStream input, int level, int bufferSize) throws IOException {
            return new org.brotli.dec.BrotliInputStream(input, normalizeBuffer(bufferSize));
        }

        @Override
        public Compression getAlgorithm() {
            return Compression.BROTLI;
        }
    }

    public static class PreferredProvider implements CompressionProvider {
        public PreferredProvider() {
            com.aayushatharva.brotli4j.Brotli4jLoader.ensureAvailability();
        }

        @Override
        public OutputStream compress(OutputStream output, int level, int bufferSize) throws IOException {
            return new com.aayushatharva.brotli4j.encoder.BrotliOutputStream(output,
                    new com.aayushatharva.brotli4j.encoder.Encoder.Parameters().setQuality(normalizeLevel(level)),
                    normalizeBuffer(bufferSize));
        }

        @Override
        public InputStream decompress(InputStream input, int level, int bufferSize) throws IOException {
            return new com.aayushatharva.brotli4j.decoder.BrotliInputStream(input, normalizeBuffer(bufferSize));
        }

        @Override
        public Compression getAlgorithm() {
            return Compression.BROTLI;
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

    static int normalizeLevel(int level) { // NOSONAR
        // https://quixdb.github.io/squash-benchmark/#results-table
        return level < -1 || level > MAX_LEVEL ? 4 : level;
    }

    public static CompressionProvider getInstance() {
        return Factory.instance;
    }

    private BrotliSupport() {
    }
}
