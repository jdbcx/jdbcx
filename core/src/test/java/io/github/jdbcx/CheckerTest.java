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

import java.math.BigInteger;

import org.testng.Assert;
import org.testng.annotations.Test;

public class CheckerTest {
    @Test(groups = { "unit" })
    public void testBetween() {
        // int
        Assert.assertEquals(Checker.between(0, "value", 0, 0), 0);
        Assert.assertEquals(Checker.between(0, "value", -1, 1), 0);
        Assert.assertThrows(IllegalArgumentException.class, () -> Checker.between(1, "value", 2, 3));

        // long
        Assert.assertEquals(Checker.between(0L, "value", 0L, 0L), 0L);
        Assert.assertEquals(Checker.between(0L, "value", -1L, 1L), 0L);
        Assert.assertThrows(IllegalArgumentException.class, () -> Checker.between(1L, "value", 2L, 3L));

        // bigint
        Assert.assertEquals(Checker.between(BigInteger.ZERO, "value", BigInteger.ZERO, BigInteger.ZERO),
                BigInteger.ZERO);
        Assert.assertEquals(
                Checker.between(BigInteger.ZERO, "value", BigInteger.valueOf(-1L), BigInteger.ONE),
                BigInteger.ZERO);
        Assert.assertThrows(IllegalArgumentException.class, () -> Checker.between(BigInteger.ONE, "value",
                BigInteger.valueOf(2), BigInteger.valueOf(3L)));
    }

    @Test(groups = { "unit" })
    public void testIsNullOrEmpty() {
        Assert.assertTrue(Checker.isNullOrEmpty(null));
        Assert.assertTrue(Checker.isNullOrEmpty(new StringBuilder()));
        Assert.assertTrue(Checker.isNullOrEmpty(new StringBuffer()));
        Assert.assertTrue(Checker.isNullOrEmpty(""));
        Assert.assertFalse(Checker.isNullOrEmpty(" "));
    }

    @Test(groups = { "unit" })
    public void testIsNullOrBlank() {
        Assert.assertTrue(Checker.isNullOrBlank(null));
        Assert.assertTrue(Checker.isNullOrEmpty(new StringBuilder()));
        Assert.assertTrue(Checker.isNullOrEmpty(new StringBuffer()));
        Assert.assertTrue(Checker.isNullOrBlank(""));
        Assert.assertTrue(Checker.isNullOrBlank(" \t\r\n  "));

        Assert.assertTrue(Checker.isNullOrBlank(" \t-- test\r\n  ", true));
        Assert.assertTrue(Checker.isNullOrBlank(" /*\t--/* /*t*/*/est\r*/\n  ", true));
    }

    @Test(groups = { "unit" })
    public void testNonBlank() {
        Assert.assertEquals(Checker.nonBlank(" 1", "value"), " 1");

        Assert.assertThrows(IllegalArgumentException.class, () -> Checker.nonBlank(null, null));
        Assert.assertThrows(IllegalArgumentException.class, () -> Checker.nonBlank("", ""));
        Assert.assertThrows(IllegalArgumentException.class, () -> Checker.nonBlank(" ", ""));
    }

    @Test(groups = { "unit" })
    public void testNonEmpty() {
        Assert.assertEquals(Checker.nonEmpty(" ", "value"), " ");

        Assert.assertThrows(IllegalArgumentException.class, () -> Checker.nonEmpty(null, null));
        Assert.assertThrows(IllegalArgumentException.class, () -> Checker.nonEmpty("", ""));
    }

    @Test(groups = { "unit" })
    public void testNonNull() {
        Object obj;
        Assert.assertEquals(Checker.nonNull(obj = new Object(), "value"), obj);
        Assert.assertEquals(Checker.nonNull(obj = 1, "value"), obj);

        Assert.assertThrows(IllegalArgumentException.class, () -> Checker.nonNull(null, (String) null));
    }

    @Test(groups = { "unit" })
    public void testNotLessThan() {
        // int
        Assert.assertEquals(Checker.notLessThan(1, "value", 0), 1);
        Assert.assertEquals(Checker.notLessThan(0, "value", 0), 0);
        Assert.assertEquals(Checker.notLessThan(0, "value", -1), 0);
        Assert.assertThrows(IllegalArgumentException.class, () -> Checker.notLessThan(0, "value", 1));

        // long
        Assert.assertEquals(Checker.notLessThan(1L, "value", 0L), 1L);
        Assert.assertEquals(Checker.notLessThan(0L, "value", 0L), 0L);
        Assert.assertEquals(Checker.notLessThan(0L, "value", -1L), 0L);
        Assert.assertThrows(IllegalArgumentException.class, () -> Checker.notLessThan(0L, "value", 1L));

        // bigint
        Assert.assertEquals(Checker.notLessThan(BigInteger.ONE, "value", BigInteger.ZERO), BigInteger.ONE);
        Assert.assertEquals(Checker.notLessThan(BigInteger.ZERO, "value", BigInteger.ZERO), BigInteger.ZERO);
        Assert.assertEquals(Checker.notLessThan(BigInteger.ZERO, "value", BigInteger.valueOf(-1L)),
                BigInteger.ZERO);
        Assert.assertThrows(IllegalArgumentException.class,
                () -> Checker.notLessThan(BigInteger.ZERO, "value", BigInteger.ONE));
    }

    @Test(groups = { "unit" })
    public void testNotLongerThan() {
        byte[] bytes;
        Assert.assertEquals(Checker.notLongerThan(bytes = null, "value", 0), bytes);
        Assert.assertEquals(Checker.notLongerThan(bytes = new byte[0], "value", 0), bytes);
        Assert.assertEquals(Checker.notLongerThan(bytes = new byte[1], "value", 1), bytes);

        Assert.assertThrows(IllegalArgumentException.class, () -> Checker.notLongerThan(null, null, -1));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> Checker.notLongerThan(new byte[0], null, -1));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> Checker.notLongerThan(new byte[2], null, 1));
    }
}
