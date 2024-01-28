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
package io.github.jdbcx;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

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

    default boolean asBoolean() {
        return isNull() && Boolean.parseBoolean(asString());
    }

    default byte asByte() {
        return isNull() ? (byte) 0 : Byte.parseByte(asString());
    }

    default short asShort() {
        return isNull() ? (short) 0 : Short.parseShort(asString());
    }

    default int asInt() {
        return isNull() ? 0 : Integer.parseInt(asString());
    }

    default long asLong() {
        return isNull() ? 0L : Long.parseLong(asString());
    }

    default float asFloat() {
        return isNull() ? 0F : Float.parseFloat(asString());
    }

    default double asDouble() {
        return isNull() ? 0D : Double.parseDouble(asString());
    }

    default BigInteger asBigInteger() {
        return isNull() ? null : new BigInteger(asString());
    }

    default BigDecimal asBigDecimal() {
        return isNull() ? null : new BigDecimal(asString());
    }

    default BigDecimal asBigDecimal(int scale) {
        return isNull() ? null : new BigDecimal(asBigInteger(), scale);
    }

    default <T> T asObject(Class<T> clazz) {
        return clazz.cast(asObject());
    }

    default Date asSqlDate() {
        return new Date(asLong());
    }

    default Time asSqlTime() {
        return new Time(asLong());
    }

    default Timestamp asSqlTimestamp() {
        return new Timestamp(asLong());
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

    Object asObject();

    String asString(Charset charset);
}
