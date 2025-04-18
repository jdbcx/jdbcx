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
package io.github.jdbcx;

public interface ResultMapper {
    default String quoteIdentifier(String name) {
        return new StringBuilder().append('"').append(Utils.escape(name, '"', '"')).append('"').toString();
    }

    String toColumnType(Field f);

    default String toColumnDefinition(Field... fields) {
        final int len;
        if (fields == null || (len = fields.length) == 0) {
            return Constants.EMPTY_STRING;
        }

        Field f = fields[0];
        StringBuilder builder = new StringBuilder();
        builder.append(quoteIdentifier(f.name())).append(' ').append(toColumnType(f));
        for (int i = 1; i < len; i++) {
            f = fields[i];
            builder.append(',').append(quoteIdentifier(f.name())).append(' ').append(toColumnType(f));
        }
        return builder.toString();
    }

    default String toRemoteTable(String url, Format format, Compression compress, Result<?> result) {
        return new StringBuilder((url != null ? url.length() : 4) + 2).append('\'').append(url).append('\'').toString();
    }
}
