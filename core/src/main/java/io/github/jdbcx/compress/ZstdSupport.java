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

public final class ZstdSupport {
    public static final int MIN_LEVEL = 0;
    public static final int MAX_LEVEL = 22;

    static class DefaultProvider implements CompressionProvider {
        protected int normalize(int level) {
            return level < MIN_LEVEL || level > MAX_LEVEL ? com.github.luben.zstd.Zstd.defaultCompressionLevel()
                    : level;
        }

        @Override
        public OutputStream compress(OutputStream output, int level, int bufferSize) throws IOException {
            return new com.github.luben.zstd.ZstdOutputStream(output, normalize(level));
        }

        @Override
        @SuppressWarnings("resource")
        public InputStream decompress(InputStream input, int level, int bufferSize) throws IOException {
            return new com.github.luben.zstd.ZstdInputStream(input).setContinuous(true);
        }

        @Override
        public Compression getAlgorithm() {
            return Compression.ZSTD;
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

    private ZstdSupport() {
    }
}
