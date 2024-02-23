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
package io.github.jdbcx;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * This interface defines the ability to compress and decompress data using a
 * specific algorithm. It provides methods for compressing and decompressing
 * data, as well as getting the compression algorithm.
 */
public interface CompressionProvider {
    static final Option OPTION_USE_DEFAULT = Option.of(new String[] { "compression.use.default",
            "Whether to use the default provider for compressing and decompressing data, or to attempt to detect a preferred library at runtime for better performance.",
            Constants.FALSE_EXPR, Constants.TRUE_EXPR });

    static boolean autoDetect() {
        return !Boolean.parseBoolean(OPTION_USE_DEFAULT.getEffectiveDefaultValue(Option.PROPERTY_PREFIX));
    }

    default OutputStream compress(OutputStream output) throws IOException {
        return compress(output, -1, 0);
    }

    /**
     * Compresses data into the output stream.
     *
     * @param output     output stream
     * @param level      suggested compression level, which may or may not be used
     * @param bufferSize suggested buffer size, which may or may not be used
     * @return non-null output stream
     * @throws IOException when failed to compress or write data into the output
     *                     stream
     */
    default OutputStream compress(OutputStream output, int level, int bufferSize) throws IOException {
        throw new UnsupportedOperationException(
                Utils.format("Compression using %s is not supported.", getAlgorithm()));
    }

    /**
     * Decompresses data from the input stream.
     *
     * @param input      input stream
     * @param level      suggested compression level, which may or may not be used
     * @param bufferSize suggested buffer size, which may or may not be used
     * @return non-null input stream
     * @throws IOException when failed to decompress or read data from the input
     *                     stream
     */
    default InputStream decompress(InputStream input, int level, int bufferSize) throws IOException {
        throw new UnsupportedOperationException(
                Utils.format("Decompression using %s is not supported.", getAlgorithm()));
    }

    /**
     * Gets the compression algorithm.
     *
     * @return non-null compression algorithm
     */
    Compression getAlgorithm();
}
