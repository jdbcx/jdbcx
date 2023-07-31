/*
 * Copyright 2022-2023, Zhichun Wu
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

import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public interface Value extends Serializable {
    boolean isNull();

    default byte[] asBinary() {
        return asBinary(Constants.DEFAULT_CHARSET);
    }

    default byte[] asBinary(Charset charset) {
        if (charset == null) {
            charset = Constants.DEFAULT_CHARSET;
        }
        return asString(charset).getBytes(charset);
    }

    default String asString() {
        return asString(Constants.DEFAULT_CHARSET);
    }

    default String asAsciiString() {
        return asString(StandardCharsets.US_ASCII);
    }

    default String asUnicodeString() {
        return asString(StandardCharsets.UTF_8);
    }

    String asString(Charset charset);
}
