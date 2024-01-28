/*
 * Copyright 2022-2024, Zhichun Wu
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

import java.math.BigDecimal;
import java.math.BigInteger;
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
    public boolean asBoolean() {
        return value != 0L;
    }

    @Override
    public byte asByte() {
        return (byte) value;
    }

    @Override
    public short asShort() {
        return (short) value;
    }

    @Override
    public int asInt() {
        return (int) value;
    }

    @Override
    public long asLong() {
        return value;
    }

    @Override
    public float asFloat() {
        return value;
    }

    @Override
    public double asDouble() {
        return value;
    }

    @Override
    public BigInteger asBigInteger() {
        return isNull() ? null : BigInteger.valueOf(value);
    }

    @Override
    public BigDecimal asBigDecimal() {
        return isNull() ? null : BigDecimal.valueOf(value);
    }

    @Override
    public String asString(Charset charset) {
        return isNull ? Constants.EMPTY_STRING : String.valueOf(value);
    }

    @Override
    public Object asObject() {
        return isNull ? null : value;
    }
}
