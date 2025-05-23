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
module io.github.jdbcx.core {
    exports io.github.jdbcx;
    exports io.github.jdbcx.cache;
    exports io.github.jdbcx.data;
    exports io.github.jdbcx.executor;
    exports io.github.jdbcx.interpreter;
    exports io.github.jdbcx.logging;

    requires static java.logging;
    requires static java.net.http;
    requires static java.sql;
    requires static com.fasterxml.jackson.databind;
    requires static com.google.gson;
    requires static com.github.benmanes.caffeine;
    requires static org.slf4j;
    requires static io.modelcontextprotocol.sdk.mcp;

    uses io.github.jdbcx.LoggerFactory;
}
