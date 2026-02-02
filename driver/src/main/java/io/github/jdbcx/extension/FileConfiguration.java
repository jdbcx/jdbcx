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
package io.github.jdbcx.extension;

import java.util.List;
import java.util.Locale;
import java.util.Properties;

import io.github.jdbcx.Compression;
import io.github.jdbcx.Format;
import io.github.jdbcx.Option;
import io.github.jdbcx.Utils;

final class FileConfiguration {
    static FileConfiguration ofOutputFile(Properties props) {
        Properties params = new Properties(props);

        final String name = Option.OUTPUT_FILE.getValue(params);
        final boolean skipFileExistenceCheck = Boolean.parseBoolean(Option.OUTPUT_FILE_OVERWRITABLE.getValue(params));

        final List<String> parts = Utils.split(name, '.');
        final int size = parts.size();
        final String fileFormat;
        final String fileCompress;
        if (size > 2) {
            fileFormat = parts.get(size - 2).toUpperCase();
            fileCompress = parts.get(size - 1).toUpperCase();
        } else if (size > 1) {
            fileFormat = parts.get(size - 1).toUpperCase();
            fileCompress = Compression.NONE.name();
        } else {
            fileFormat = Format.CSV.name();
            fileCompress = Compression.NONE.name();
        }

        String value = Option.OUTPUT_FILE_FORMAT.getValue(params);
        final Format format = value.isEmpty() ? Format.fromFileExtension(fileFormat, null)
                : Format.valueOf(value.toUpperCase(Locale.ROOT));

        value = Option.OUTPUT_FILE_COMPRESS_ALGORITHM.getValue(params);
        final Compression compressionAlg = value.isEmpty() ? Compression.fromFileExtension(fileCompress, null)
                : Compression.valueOf(value.toUpperCase(Locale.ROOT));

        value = Option.OUTPUT_FILE_COMPRESS_LEVEL.getValue(params);
        final int compressionLevel = value.isEmpty() ? -1 : Integer.parseInt(value);

        value = Option.OUTPUT_FILE_COMPRESS_BUFFER.getValue(params);
        final int compressionBuffer = value.isEmpty() ? 0 : Integer.parseInt(value);

        params = new Properties();
        params.putAll(Utils.toKeyValuePairs(Option.OUTPUT_FILE_COMPRESS_PARAMS.getValue(params)));
        return new FileConfiguration(name, skipFileExistenceCheck, format, params, compressionAlg, compressionLevel,
                compressionBuffer);
    }

    static FileConfiguration ofOutputFile(Properties props, FileConfiguration defaults) {
        Properties params = new Properties(props);

        final String name = Option.OUTPUT_FILE.getValue(params, defaults.name);
        final boolean skipFileExistenceCheck = Boolean.parseBoolean(
                Option.OUTPUT_FILE_OVERWRITABLE.getValue(params, String.valueOf(defaults.skipFileExistenceCheck)));

        String value = Option.OUTPUT_FILE_FORMAT.getValue(params);
        final Format format = value.isEmpty() ? defaults.format : Format.valueOf(value.toUpperCase(Locale.ROOT));

        value = Option.OUTPUT_FILE_COMPRESS_ALGORITHM.getValue(params);
        final Compression compressionAlg = value.isEmpty() ? defaults.compressionAlg
                : Compression.valueOf(value.toUpperCase(Locale.ROOT));

        value = Option.OUTPUT_FILE_COMPRESS_LEVEL.getValue(params);
        final int compressionLevel = value.isEmpty() ? defaults.compressionLevel : Integer.parseInt(value);

        value = Option.OUTPUT_FILE_COMPRESS_BUFFER.getValue(params);
        final int compressionBuffer = value.isEmpty() ? defaults.compressionBuffer : Integer.parseInt(value);

        params = new Properties(defaults.params);
        params.putAll(Utils.toKeyValuePairs(Option.OUTPUT_FILE_COMPRESS_PARAMS.getValue(params)));
        return new FileConfiguration(name, skipFileExistenceCheck, format, params, compressionAlg, compressionLevel,
                compressionBuffer);
    }

    final String name;
    final boolean skipFileExistenceCheck;
    final Format format;
    final Properties params;
    final Compression compressionAlg;
    final int compressionLevel;
    final int compressionBuffer;

    FileConfiguration(String name, boolean skipFileExistenceCheck, Format format, Properties params,
            Compression compressionAlg, int compressionLevel, int compressionBuffer) {
        this.name = name;
        this.skipFileExistenceCheck = skipFileExistenceCheck;
        this.format = format;
        this.params = params;
        this.compressionAlg = compressionAlg;
        this.compressionLevel = compressionLevel;
        this.compressionBuffer = compressionBuffer;
    }

    FileConfiguration(String name, boolean skipFileExistenceCheck, Properties params) {
        this.name = name;
        this.skipFileExistenceCheck = skipFileExistenceCheck;
        this.params = new Properties(params);

        String value = Option.OUTPUT_FILE_FORMAT.getValue(this.params);
        this.format = value.isEmpty() ? Format.fromFileName(name) : Format.valueOf(value.toUpperCase(Locale.ROOT));

        value = Option.OUTPUT_FILE_COMPRESS_ALGORITHM.getValue(this.params);
        this.compressionAlg = value.isEmpty() ? Compression.fromFileName(name)
                : Compression.valueOf(value.toUpperCase(Locale.ROOT));

        value = Option.OUTPUT_FILE_COMPRESS_LEVEL.getValue(this.params);
        this.compressionLevel = value.isEmpty() ? -1 : Integer.parseInt(value);

        value = Option.OUTPUT_FILE_COMPRESS_BUFFER.getValue(this.params);
        this.compressionBuffer = value.isEmpty() ? 0 : Integer.parseInt(value);
    }
}
