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
package io.github.jdbcx.data;

import java.nio.charset.Charset;

import io.github.jdbcx.Constants;
import io.github.jdbcx.Value;

public final class StringValue implements Value {
    private final byte[] bytes;
    private final String value;

    public StringValue(String value) {
        this.bytes = null;
        this.value = value;
    }

    public StringValue(byte[] bytes) {
        this.bytes = bytes;
        this.value = null;
    }

    public StringValue(Object value) {
        if (value instanceof byte[]) {
            this.bytes = (byte[]) value;
            this.value = null;
        } else {
            this.bytes = null;
            this.value = value != null ? value.toString() : null;
        }
    }

    @Override
    public boolean isNull() {
        return bytes == null && value == null;
    }

    @Override
    public byte[] asBinary(Charset charset) {
        if (bytes != null) {
            return bytes;
        } else if (value != null) {
            return value.getBytes(charset != null ? charset : Constants.DEFAULT_CHARSET);
        }
        return Constants.EMPTY_BYTE_ARRAY;
    }

    @Override
    public String asString(Charset charset) {
        if (value != null) {
            return value;
        } else if (bytes != null) {
            return new String(bytes, charset != null ? charset : Constants.DEFAULT_CHARSET);
        }
        return Constants.EMPTY_STRING;
    }

    @Override
    public Object asObject() {
        return asString();
    }
}
