/*
 * Copyright 2022-2024, Zhichun Wu
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

public final class Lz4Support {
    static class DefaultProvider implements CompressionProvider {
        protected int normalize(int level) {
            return level < 0 ? -1 : level;
        }

        @Override
        public OutputStream compress(OutputStream output, int level, int bufferSize) throws IOException {
            return new org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorOutputStream(output);
        }

        @Override
        public InputStream decompress(InputStream input, int level, int bufferSize) throws IOException {
            return new org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorInputStream(input, true);
        }

        @Override
        public Compression getAlgorithm() {
            return Compression.LZ4;
        }
    }

    static final class Factory {
        private static final CompressionProvider instance = new DefaultProvider();

        private Factory() {
        }
    }

    public static CompressionProvider getInstance() {
        return Factory.instance;
    }

    private Lz4Support() {
    }
}
