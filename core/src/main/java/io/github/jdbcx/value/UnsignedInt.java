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
package io.github.jdbcx.value;

/**
 * A wrapper class for unsigned {@code int}.
 */
public final class UnsignedInt extends Number implements Comparable<UnsignedInt> {
    public static final int BYTES = Integer.BYTES;

    public static final UnsignedInt ZERO = new UnsignedInt(0);
    public static final UnsignedInt ONE = new UnsignedInt(1);
    public static final UnsignedInt TWO = new UnsignedInt(2);
    public static final UnsignedInt TEN = new UnsignedInt(10);

    public static final UnsignedInt MIN_VALUE = ZERO;
    public static final UnsignedInt MAX_VALUE = new UnsignedInt(-1);

    /**
     * Returns a {@code UnsignedInteger} instance representing the specified
     * {@code int} value.
     *
     * @param i an int value
     * @return a {@code UnsignedInteger} instance representing {@code l}
     */
    public static UnsignedInt valueOf(int i) {
        if (i == -1L) {
            return MAX_VALUE;
        } else if (i == 0L) {
            return ZERO;
        } else if (i == 1L) {
            return ONE;
        } else if (i == 2L) {
            return TWO;
        } else if (i == 10L) {
            return TEN;
        }

        return new UnsignedInt(i);
    }

    /**
     * Returns a {@code UnsignedInteger} object holding the value
     * of the specified {@code String}.
     *
     * @param s non-empty string
     * @return a {@code UnsignedInteger} instance representing {@code s}
     */
    public static UnsignedInt valueOf(String s) {
        return valueOf(s, 10);
    }

    /**
     * Returns a {@code UnsignedInteger} object holding the value
     * extracted from the specified {@code String} when parsed
     * with the radix given by the second argument.
     *
     * @param s     the {@code String} containing the unsigned integer
     *              representation to be parsed
     * @param radix the radix to be used while parsing {@code s}
     * @return the unsigned {@code long} represented by the string
     *         argument in the specified radix
     * @throws NumberFormatException if the {@code String} does not contain a
     *                               parsable {@code int}
     */
    public static UnsignedInt valueOf(String s, int radix) {
        return valueOf(Integer.parseUnsignedInt(s, radix));
    }

    private final int value;

    private UnsignedInt(int value) {
        this.value = value;
    }

    /**
     * Returns an {@code UnsignedInteger} whose value is {@code (this + val)}. If
     * the
     * result would have more than 32 bits, returns the low 32 bits of the result.
     *
     * @param val value to be added to this unsigned integer, null is treated as
     *            zero
     * @return {@code this + val}
     */
    public UnsignedInt add(UnsignedInt val) {
        if (val == null || val.value == 0L) {
            return this;
        }
        return valueOf(this.value + val.value);
    }

    /**
     * Returns an {@code UnsignedInteger} whose value is {@code (this - val)}. If
     * the
     * result would have more than 32 bits, returns the low 32 bits of the result.
     *
     * @param val value to be subtracted from this unsigned integer, null is treated
     *            as
     *            zero
     * @return {@code this - val}
     */
    public UnsignedInt subtract(UnsignedInt val) {
        if (val == null || val.value == 0L) {
            return this;
        }
        return valueOf(this.value - val.value);
    }

    /**
     * Returns an {@code UnsignedInteger} whose value is {@code (this * val)}. If
     * the
     * result would have more than 32 bits, returns the low 32 bits of the result.
     *
     * @param val value to be multiplied by this unsigned integer, null is treated
     *            as
     *            zero
     * @return {@code this * val}
     */
    public UnsignedInt multiply(UnsignedInt val) {
        if (this.value == 0L || val == null || val.value == 0L) {
            return ZERO;
        }
        return valueOf(this.value * val.value);
    }

    /**
     * Returns an {@code UnsignedInteger} whose value is {@code (this / val)}.
     *
     * @param val value by which this unsigned is to be divided, null is treated as
     *            zero
     * @return {@code this / val}
     * @throws ArithmeticException if {@code val} is null or zero.
     */
    public UnsignedInt divide(UnsignedInt val) {
        return valueOf(Integer.divideUnsigned(this.value, val == null ? 0 : val.value));
    }

    /**
     * Returns an {@code UnsignedInteger} whose value is {@code (this % val)}.
     *
     * @param val value by which this unsigned integer is to be divided, and the
     *            remainder computed
     * @return {@code this % val}
     * @throws ArithmeticException if {@code val} is null or zero.
     */
    public UnsignedInt remainder(UnsignedInt val) {
        return valueOf(Integer.remainderUnsigned(value, val == null ? 0 : val.value));
    }

    @Override
    public int intValue() {
        return value;
    }

    @Override
    public long longValue() {
        return value >= 0 ? value : 0xFFFFFFFFL & value;
    }

    @Override
    public float floatValue() {
        return longValue();
    }

    @Override
    public double doubleValue() {
        return longValue();
    }

    @Override
    public int compareTo(UnsignedInt o) {
        return Integer.compareUnsigned(value, o == null ? 0 : o.value);
    }

    @Override
    public int hashCode() {
        return 31 + value;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        return value == ((UnsignedInt) obj).value;
    }

    @Override
    public String toString() {
        return Integer.toUnsignedString(value);
    }

    /**
     * Returns a string representation of the first argument as an
     * unsigned integer value in the radix specified by the second
     * argument.
     *
     * @param radix the radix to use in the string representation
     * @return an unsigned string representation of the argument in the specified
     *         radix
     */
    public String toString(int radix) {
        return Integer.toUnsignedString(value, radix);
    }
}