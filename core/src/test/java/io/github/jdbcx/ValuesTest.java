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

import java.io.ByteArrayInputStream;
import java.io.StringReader;
import java.math.BigDecimal;
import java.math.BigInteger;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ValuesTest {
    @Test(groups = { "unit" })
    public void testConstructor() {
        Object v;
        Assert.assertEquals(Values.of(v = null).asObject(), v);
        Assert.assertEquals(Values.of(v = new byte[] { 2, 4, 6 }).asBinary(), v);
        Assert.assertEquals(Values.of(v = new char[] { 'x', 'y', 'Z' }).asString(), new String((char[]) v));
        Assert.assertEquals(Values.of(v = 'X').asObject(), v.toString()); // fine for now
        Assert.assertEquals(Values.of(v = (byte) 3).asObject(), v);
        Assert.assertEquals(Values.of(v = (short) 4).asObject(), v);
        Assert.assertEquals(Values.of(v = 5).asObject(), v);
        Assert.assertEquals(Values.of(v = 9L).asObject(), v);
        Assert.assertEquals(Values.of(v = 1.23F).asObject(), v);
        Assert.assertEquals(Values.of(v = 456.7890D).asObject(), v);
        Assert.assertEquals(Values.of(v = BigInteger.valueOf(124578L)).asObject(), v);
        Assert.assertEquals(Values.of(v = BigDecimal.valueOf(124578L, 3)).asObject(), v);

        Assert.assertEquals(Values.of(new ByteArrayInputStream(new byte[] { 1, 2, 3, 4, 5 }, 2, 2)).asBinary(),
                new byte[] { 3, 4 });
        Assert.assertEquals(Values.of(new StringReader("xYz")).asObject(), "xYz");

        Assert.assertEquals(Values.of(v = new Object()).asObject(), v.toString());
    }
}
