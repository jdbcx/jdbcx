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
package io.github.jdbcx.executor;

import java.util.Properties;

import io.github.jdbcx.Compression;
import io.github.jdbcx.Format;

final class FileConfiguration {
    final String name;
    final Format format;
    final Properties params;
    final Compression compressionAlg;
    final int compressionLevel;
    final int compressionBuffer;

    FileConfiguration(String name, Format format, Properties params, Compression compressionAlg, int compressionLevel,
            int compressionBuffer) {
        this.name = name;
        this.format = format;
        this.params = params;
        this.compressionAlg = compressionAlg;
        this.compressionLevel = compressionLevel;
        this.compressionBuffer = compressionBuffer;
    }
}
