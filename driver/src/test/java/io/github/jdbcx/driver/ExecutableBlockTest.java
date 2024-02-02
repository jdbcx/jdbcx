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
package io.github.jdbcx.driver;

import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ExecutableBlockTest {
    @Test(groups = { "unit" })
    public void testConstructor() {
        Assert.assertThrows(IllegalArgumentException.class,
                () -> new ExecutableBlock(0, null, null, null, false));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> new ExecutableBlock(0, "", null, null, false));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> new ExecutableBlock(0, "", new Properties(), null, false));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> new ExecutableBlock(0, null, new Properties(), null, false));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> new ExecutableBlock(0, null, new Properties(), "", false));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> new ExecutableBlock(0, null, null, "", false));

        ExecutableBlock block = new ExecutableBlock(-1, "ex", new Properties(), "...", true);
        Assert.assertEquals(block, new ExecutableBlock(-1, "ex", new Properties(), "...", true));
        Assert.assertEquals(block.getIndex(), -1);
        Assert.assertEquals(block.getExtensionName(), "ex");
        Assert.assertEquals(block.getProperties(), new Properties());
        Assert.assertEquals(block.getContent(), "...");
        Assert.assertEquals(block.hasOutput(), true);

        block.getProperties().setProperty("a", "c");
        Properties props = new Properties();
        props.setProperty("a", "c");
        Assert.assertEquals(block, new ExecutableBlock(-1, "ex", props, "...", true));
    }

    @Test(groups = { "unit" })
    public void testSameAs() {
        Properties props = new Properties();
        ExecutableBlock block = new ExecutableBlock(1, "b1", props, "...", true);
        Assert.assertFalse(block.sameAs(new ExecutableBlock(1, "b2", props, "...", true)),
                "Should be false since extension is different");
        Assert.assertFalse(block.sameAs(new ExecutableBlock(1, "b1", props, ". .", true)),
                "Should be false since content is different");
        Assert.assertFalse(block.sameAs(new ExecutableBlock(1, "b1", props, "...", false)),
                "Should be false since output is different");

        Assert.assertTrue(block.sameAs(new ExecutableBlock(2, "b1", props, "...", true)),
                "Should be true because index does not matter");
        Assert.assertTrue(block.sameAs(new ExecutableBlock(1, "b1", new Properties(), "...", true)),
                "Should be true because properties do not matter");
        props.setProperty("a", "b");
        Assert.assertTrue(block.sameAs(new ExecutableBlock(1, "b1", new Properties(), "...", true)),
                "Should be true because properties do not matter");
    }
}
