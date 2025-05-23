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

import java.sql.JDBCType;

public interface Executor {
    static final Field FIELD_QUERY = Field.of("query");
    static final Field FIELD_TIMEOUT_MS = Field.of("timeout_ms", JDBCType.BIGINT);
    static final Field FIELD_OPTIONS = Field.of("options");

    // error handling
    // named query
    // dry-run
    // save named result in context
}
