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
package io.github.jdbcx.dialect;

import io.github.jdbcx.Format;
import io.github.jdbcx.JdbcDialect;
import io.github.jdbcx.ResultMapper;

public class DuckDBDialect implements JdbcDialect {
    static final DuckDBMapper mapper = new DuckDBMapper();

    @Override
    public Format getPreferredFormat() {
        return Format.CSV;
    }

    @Override
    public ResultMapper getMapper() {
        return mapper;
    }

    @Override
    public boolean supports(Format format) {
        return format == Format.CSV || format == Format.JSONL || format == Format.NDJSON || format == Format.PARQUET
                || format == Format.VALUES;
    }
}
