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
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;

import io.github.jdbcx.Compression;
import io.github.jdbcx.CompressionProvider;
import io.github.jdbcx.Utils;

public final class DeflateSupport {
    public static final int DEFAULT_BUFFER_SIZE = 512;

    public static final int MIN_LEVEL = Deflater.NO_COMPRESSION;
    public static final int MAX_LEVEL = Deflater.BEST_COMPRESSION;

    public static class DefaultProvider implements CompressionProvider {
        @Override
        public OutputStream compress(OutputStream output, int level, int bufferSize) throws IOException {
            return new DeflaterOutputStream(output, new Deflater(normalizeLevel(level)), normalizeBuffer(bufferSize),
                    false);
        }

        @Override
        public InputStream decompress(InputStream input, int level, int bufferSize) throws IOException {
            return new InflaterInputStream(input, new Inflater(), normalizeBuffer(bufferSize));
        }

        @Override
        public Compression getAlgorithm() {
            return Compression.DEFLATE;
        }
    }

    public static class PreferredProvider implements CompressionProvider {
        public PreferredProvider() {
            new org.apache.commons.compress.compressors.deflate.DeflateParameters();
        }

        @Override
        public OutputStream compress(OutputStream output, int level, int bufferSize) throws IOException {
            org.apache.commons.compress.compressors.deflate.DeflateParameters params = new org.apache.commons.compress.compressors.deflate.DeflateParameters();
            params.setWithZlibHeader(true);
            params.setCompressionLevel(normalizeLevel(level));
            return new org.apache.commons.compress.compressors.deflate.DeflateCompressorOutputStream(output, params);
        }

        @Override
        public InputStream decompress(InputStream input, int level, int bufferSize) throws IOException {
            org.apache.commons.compress.compressors.deflate.DeflateParameters params = new org.apache.commons.compress.compressors.deflate.DeflateParameters();
            params.setWithZlibHeader(true);
            params.setCompressionLevel(normalizeLevel(level));
            return new org.apache.commons.compress.compressors.deflate.DeflateCompressorInputStream(input, params);
        }

        @Override
        public Compression getAlgorithm() {
            return Compression.DEFLATE;
        }
    }

    static final class Factory {
        private static final CompressionProvider instance = Utils.createInstance(PreferredProvider.class,
                DefaultProvider.class,
                CompressionProvider.autoDetect());

        private Factory() {
        }
    }

    static int normalizeBuffer(int bufferSize) { // NOSONAR
        return bufferSize < DEFAULT_BUFFER_SIZE ? DEFAULT_BUFFER_SIZE : bufferSize;
    }

    static int normalizeLevel(int level) {
        return level < Deflater.DEFAULT_COMPRESSION || level > Deflater.BEST_COMPRESSION ? Deflater.BEST_SPEED : level;
    }

    public static CompressionProvider getInstance() {
        return Factory.instance;
    }

    private DeflateSupport() {
    }
}
