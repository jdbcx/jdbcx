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

import org.testng.Assert;
import org.testng.annotations.Test;

public class UnsignedIntTest {
    @Test(groups = { "unit" })
    public void testConversion() {
        Assert.assertEquals(UnsignedInt.ZERO.byteValue(), (byte) 0);
        Assert.assertEquals(UnsignedInt.ZERO.shortValue(), (short) 0);
        Assert.assertEquals(UnsignedInt.ZERO.intValue(), 0);
        Assert.assertEquals(UnsignedInt.ZERO.longValue(), 0L);
        Assert.assertEquals(UnsignedInt.ZERO.floatValue(), 0F);
        Assert.assertEquals(UnsignedInt.ZERO.doubleValue(), 0D);
        Assert.assertEquals(UnsignedInt.ZERO.toString(), "0");

        Assert.assertEquals(UnsignedInt.ONE.byteValue(), (byte) 1);
        Assert.assertEquals(UnsignedInt.ONE.shortValue(), (short) 1);
        Assert.assertEquals(UnsignedInt.ONE.intValue(), 1);
        Assert.assertEquals(UnsignedInt.ONE.longValue(), 1L);
        Assert.assertEquals(UnsignedInt.ONE.floatValue(), 1F);
        Assert.assertEquals(UnsignedInt.ONE.doubleValue(), 1D);
        Assert.assertEquals(UnsignedInt.ONE.toString(), "1");

        Assert.assertEquals(UnsignedInt.valueOf(Integer.MIN_VALUE).byteValue(), (byte) 0);
        Assert.assertEquals(UnsignedInt.valueOf(Integer.MIN_VALUE).shortValue(), (short) 0);
        Assert.assertEquals(UnsignedInt.valueOf(Integer.MIN_VALUE).intValue(), -2147483648);
        Assert.assertEquals(UnsignedInt.valueOf(Integer.MIN_VALUE).longValue(), 2147483648L);
        Assert.assertEquals(UnsignedInt.valueOf(Integer.MIN_VALUE).floatValue(), 2147483648F);
        Assert.assertEquals(UnsignedInt.valueOf(Integer.MIN_VALUE).doubleValue(), 2147483648D);
        Assert.assertEquals(UnsignedInt.valueOf(Integer.MIN_VALUE).toString(), "2147483648");

        Assert.assertEquals(UnsignedInt.valueOf(Integer.MAX_VALUE).byteValue(), (byte) -1);
        Assert.assertEquals(UnsignedInt.valueOf(Integer.MAX_VALUE).shortValue(), (short) -1);
        Assert.assertEquals(UnsignedInt.valueOf(Integer.MAX_VALUE).intValue(), 2147483647);
        Assert.assertEquals(UnsignedInt.valueOf(Integer.MAX_VALUE).longValue(), 2147483647L);
        Assert.assertEquals(UnsignedInt.valueOf(Integer.MAX_VALUE).floatValue(), 2147483647F);
        Assert.assertEquals(UnsignedInt.valueOf(Integer.MAX_VALUE).doubleValue(), 2147483647D);
        Assert.assertEquals(UnsignedInt.valueOf(Integer.MAX_VALUE).toString(), "2147483647");

        Assert.assertEquals(UnsignedInt.MAX_VALUE.byteValue(), (byte) 255);
        Assert.assertEquals(UnsignedInt.MAX_VALUE.shortValue(), (short) 65535);
        Assert.assertEquals(UnsignedInt.MAX_VALUE.intValue(), -1);
        Assert.assertEquals(UnsignedInt.MAX_VALUE.longValue(), 4294967295L);
        Assert.assertEquals(UnsignedInt.MAX_VALUE.floatValue(), 4294967295F);
        Assert.assertEquals(UnsignedInt.MAX_VALUE.doubleValue(), 4294967295D);
        Assert.assertEquals(UnsignedInt.MAX_VALUE.toString(), "4294967295");
    }

    @Test(groups = { "unit" })
    public void testValueOf() {
        Assert.assertTrue(UnsignedInt.ZERO == UnsignedInt.MIN_VALUE);
        Assert.assertTrue(UnsignedInt.ZERO == UnsignedInt.valueOf(0));
        Assert.assertTrue(UnsignedInt.ONE == UnsignedInt.valueOf(1));
        Assert.assertTrue(UnsignedInt.TWO == UnsignedInt.valueOf(2));
        Assert.assertTrue(UnsignedInt.TEN == UnsignedInt.valueOf(10));
        Assert.assertTrue(UnsignedInt.MAX_VALUE == UnsignedInt.valueOf(-1));
        // for (long i = 0; i <= 0xFFFFFFFFL; i++) {
        // Assert.assertEquals(UnsignedInteger.valueOf((int) i),
        // UnsignedInteger.valueOf(String.valueOf(i)));
        // Assert.assertEquals(UnsignedInteger.valueOf((int) i).longValue(), i);
        // Assert.assertEquals(UnsignedInteger.valueOf((int) i).toString(),
        // String.valueOf(i));
        // }

        Assert.assertThrows(NumberFormatException.class, () -> UnsignedInt.valueOf((String) null));
        Assert.assertThrows(NumberFormatException.class, () -> UnsignedInt.valueOf(""));
        Assert.assertThrows(NumberFormatException.class, () -> UnsignedInt.valueOf("-1"));
        Assert.assertThrows(NumberFormatException.class, () -> UnsignedInt.valueOf("4294967296"));
    }

    @Test(groups = { "unit" })
    public void testAdd() {
        Assert.assertEquals(UnsignedInt.ZERO.add(null), UnsignedInt.ZERO);
        Assert.assertEquals(UnsignedInt.ZERO.add(UnsignedInt.MIN_VALUE), UnsignedInt.ZERO);
        Assert.assertEquals(UnsignedInt.ONE.add(UnsignedInt.ONE), UnsignedInt.valueOf(2));
        Assert.assertEquals(UnsignedInt.MAX_VALUE.add(UnsignedInt.ONE), UnsignedInt.ZERO);
        Assert.assertEquals(UnsignedInt.MAX_VALUE.add(UnsignedInt.MAX_VALUE), UnsignedInt.valueOf(-2));

        Assert.assertEquals(UnsignedInt.valueOf(Integer.MIN_VALUE).add(UnsignedInt.ZERO),
                UnsignedInt.valueOf("2147483648"));
        Assert.assertEquals(UnsignedInt.valueOf(Integer.MAX_VALUE).add(UnsignedInt.ZERO),
                UnsignedInt.valueOf(Integer.MAX_VALUE));
        Assert.assertEquals(UnsignedInt.valueOf(Integer.MAX_VALUE).add(UnsignedInt.ONE),
                UnsignedInt.valueOf("2147483648"));
    }

    @Test(groups = { "unit" })
    public void testSubtract() {
        Assert.assertEquals(UnsignedInt.ZERO.subtract(null), UnsignedInt.ZERO);
        Assert.assertEquals(UnsignedInt.ZERO.subtract(UnsignedInt.MIN_VALUE), UnsignedInt.ZERO);
        Assert.assertEquals(UnsignedInt.ONE.subtract(UnsignedInt.ONE), UnsignedInt.ZERO);
        Assert.assertEquals(UnsignedInt.valueOf(2).subtract(UnsignedInt.ONE), UnsignedInt.ONE);
        Assert.assertEquals(UnsignedInt.MAX_VALUE.subtract(UnsignedInt.ONE),
                UnsignedInt.valueOf("4294967294"));
        Assert.assertEquals(UnsignedInt.MAX_VALUE.subtract(UnsignedInt.MAX_VALUE), UnsignedInt.ZERO);

        Assert.assertEquals(UnsignedInt.valueOf(Integer.MIN_VALUE).subtract(UnsignedInt.ZERO),
                UnsignedInt.valueOf("2147483648"));
        Assert.assertEquals(UnsignedInt.valueOf(Integer.MAX_VALUE).subtract(UnsignedInt.ZERO),
                UnsignedInt.valueOf(Integer.MAX_VALUE));
        Assert.assertEquals(UnsignedInt.valueOf(Integer.MAX_VALUE).subtract(UnsignedInt.ONE),
                UnsignedInt.valueOf((Integer.MAX_VALUE - 1)));
    }

    @Test(groups = { "unit" })
    public void testMultiply() {
        Assert.assertEquals(UnsignedInt.ONE.multiply(null), UnsignedInt.ZERO);
        Assert.assertEquals(UnsignedInt.ONE.multiply(UnsignedInt.MIN_VALUE), UnsignedInt.ZERO);
        Assert.assertEquals(UnsignedInt.ONE.multiply(UnsignedInt.ONE), UnsignedInt.ONE);
        Assert.assertEquals(UnsignedInt.valueOf(3).multiply(UnsignedInt.ONE),
                UnsignedInt.valueOf(3));
        Assert.assertEquals(UnsignedInt.MAX_VALUE.multiply(UnsignedInt.ONE), UnsignedInt.MAX_VALUE);
        Assert.assertEquals(UnsignedInt.MAX_VALUE.multiply(UnsignedInt.MAX_VALUE), UnsignedInt
                .valueOf((int) (0xFFFFFFFFL * 0xFFFFFFFFL)));

        Assert.assertEquals(UnsignedInt.valueOf(Integer.MIN_VALUE).multiply(UnsignedInt.ZERO),
                UnsignedInt.ZERO);
        Assert.assertEquals(UnsignedInt.valueOf(Integer.MAX_VALUE).multiply(UnsignedInt.ZERO),
                UnsignedInt.ZERO);
        Assert.assertEquals(UnsignedInt.valueOf(Integer.MAX_VALUE).multiply(UnsignedInt.ONE),
                UnsignedInt.valueOf(Integer.MAX_VALUE));
    }

    @Test(groups = { "unit" })
    public void testDivide() {
        Assert.assertThrows(ArithmeticException.class, () -> UnsignedInt.ONE.divide(null));
        Assert.assertThrows(ArithmeticException.class, () -> UnsignedInt.ONE.divide(UnsignedInt.ZERO));
        Assert.assertThrows(ArithmeticException.class, () -> UnsignedInt.ONE.divide(UnsignedInt.MIN_VALUE));

        Assert.assertEquals(UnsignedInt.ONE.divide(UnsignedInt.ONE), UnsignedInt.ONE);
        Assert.assertEquals(UnsignedInt.valueOf(3).divide(UnsignedInt.ONE),
                UnsignedInt.valueOf(3));
        Assert.assertEquals(UnsignedInt.ONE.divide(UnsignedInt.valueOf(3)), UnsignedInt.ZERO);
        Assert.assertEquals(UnsignedInt.MAX_VALUE.divide(UnsignedInt.ONE), UnsignedInt.MAX_VALUE);
        Assert.assertEquals(UnsignedInt.MAX_VALUE.divide(UnsignedInt.MAX_VALUE), UnsignedInt.ONE);

        Assert.assertEquals(UnsignedInt.valueOf(Integer.MIN_VALUE).divide(UnsignedInt.ONE),
                UnsignedInt.valueOf(Integer.MIN_VALUE));
        Assert.assertEquals(UnsignedInt.valueOf(Integer.MAX_VALUE).divide(UnsignedInt.ONE),
                UnsignedInt.valueOf(Integer.MAX_VALUE));
    }

    @Test(groups = { "unit" })
    public void testRemainder() {
        Assert.assertThrows(ArithmeticException.class, () -> UnsignedInt.ONE.remainder(null));
        Assert.assertThrows(ArithmeticException.class, () -> UnsignedInt.ONE.remainder(UnsignedInt.ZERO));
        Assert.assertThrows(ArithmeticException.class, () -> UnsignedInt.ONE.remainder(UnsignedInt.MIN_VALUE));

        Assert.assertEquals(UnsignedInt.ONE.remainder(UnsignedInt.ONE), UnsignedInt.ZERO);
        Assert.assertEquals(UnsignedInt.valueOf(3).remainder(UnsignedInt.ONE), UnsignedInt.ZERO);
        Assert.assertEquals(UnsignedInt.ONE.remainder(UnsignedInt.valueOf(3)), UnsignedInt.ONE);
        Assert.assertEquals(UnsignedInt.MAX_VALUE.remainder(UnsignedInt.ONE), UnsignedInt.ZERO);
        Assert.assertEquals(UnsignedInt.MAX_VALUE.remainder(UnsignedInt.MAX_VALUE), UnsignedInt.ZERO);

        Assert.assertEquals(UnsignedInt.valueOf(Integer.MIN_VALUE).remainder(UnsignedInt.ONE),
                UnsignedInt.ZERO);
        Assert.assertEquals(UnsignedInt.valueOf(Integer.MAX_VALUE).remainder(UnsignedInt.ONE),
                UnsignedInt.ZERO);
    }
}
