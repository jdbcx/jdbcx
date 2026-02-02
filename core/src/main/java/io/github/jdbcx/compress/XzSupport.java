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

public final class XzSupport {
    public static final int DEFAULT_LEVEL = 6; // LZMA2Options.PRESET_DEFAULT
    public static final int MIN_LEVEL = 0; // LZMA2Options.PRESET_MIN
    public static final int MAX_LEVEL = 9; // LZMA2Options.PRESET_MAX

    static class DefaultProvider implements CompressionProvider {
        @Override
        public OutputStream compress(OutputStream output, int level, int bufferSize) throws IOException {
            return new org.tukaani.xz.XZOutputStream(output, new org.tukaani.xz.LZMA2Options(normalizeLevel(level)),
                    org.tukaani.xz.XZ.CHECK_CRC64);
        }

        @Override
        public InputStream decompress(InputStream input, int level, int bufferSize) throws IOException {
            return new org.tukaani.xz.XZInputStream(input);
        }

        @Override
        public Compression getAlgorithm() {
            return Compression.XZ;
        }
    }

    static final class Factory {
        private static final CompressionProvider instance = new DefaultProvider();

        private Factory() {
        }
    }

    static int normalizeLevel(int level) {
        return level < MIN_LEVEL || level > MAX_LEVEL ? DEFAULT_LEVEL : level;
    }

    public static CompressionProvider getInstance() {
        return Factory.instance;
    }

    private XzSupport() {
    }
}
