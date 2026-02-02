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

public final class NoneSupport implements CompressionProvider { // NOSONAR
    private static final NoneSupport instance = new NoneSupport();

    @Override
    public OutputStream compress(OutputStream output, int level, int bufferSize) throws IOException {
        return output;
    }

    @Override
    public InputStream decompress(InputStream input, int level, int bufferSize) throws IOException {
        return input;
    }

    @Override
    public Compression getAlgorithm() {
        return Compression.NONE;
    }

    public static NoneSupport getInstance() {
        return instance;
    }

    private NoneSupport() {
    }
}
