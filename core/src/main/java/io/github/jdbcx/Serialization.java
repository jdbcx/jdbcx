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
package io.github.jdbcx;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface Serialization {
    static final Option OPTION_BUFFER = Option.of(new String[] { "buffer",
            "Size of the buffer for reading and writing, zero or negative number means no buffer" });

    Result<?> deserialize(InputStream in) throws IOException; // NOSONAR

    void serialize(Result<?> result, OutputStream out) throws IOException;
}
