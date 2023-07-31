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

public final class LongValue implements Value {
    private final boolean isNull;
    private final long value;

    public LongValue(long value) {
        this.isNull = false;
        this.value = value;
    }

    public LongValue(Long value) {
        if (value == null) {
            this.isNull = true;
            this.value = 0L;
        } else {
            this.isNull = false;
            this.value = value.longValue();
        }
    }

    @Override
    public boolean isNull() {
        return isNull;
    }

    @Override
    public String asString(Charset charset) {
        return isNull ? Constants.EMPTY_STRING : String.valueOf(value);
    }
}
