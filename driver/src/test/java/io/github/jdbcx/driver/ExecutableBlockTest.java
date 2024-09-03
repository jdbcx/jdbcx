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

import io.github.jdbcx.Option;
import io.github.jdbcx.VariableTag;

public class ExecutableBlockTest {
    @Test(groups = { "unit" })
    public void testHasBuiltInVariable() {
        Assert.assertThrows(NullPointerException.class, () -> ExecutableBlock.hasBuiltInVariable("", null));
        Assert.assertThrows(NullPointerException.class,
                () -> ExecutableBlock.hasBuiltInVariable(null, VariableTag.ANGLE_BRACKET));

        Assert.assertFalse(ExecutableBlock.hasBuiltInVariable("", VariableTag.BRACE));
        Assert.assertFalse(ExecutableBlock.hasBuiltInVariable("_", VariableTag.BRACE));
        Assert.assertFalse(ExecutableBlock.hasBuiltInVariable("$\\{_}", VariableTag.BRACE));
        Assert.assertFalse(ExecutableBlock.hasBuiltInVariable("${__}", VariableTag.BRACE));
        Assert.assertFalse(ExecutableBlock.hasBuiltInVariable("${_.}", VariableTag.BRACE));
        Assert.assertFalse(ExecutableBlock.hasBuiltInVariable("${_.:}", VariableTag.BRACE));
        Assert.assertFalse(ExecutableBlock.hasBuiltInVariable("${.:_}", VariableTag.BRACE));

        Assert.assertTrue(ExecutableBlock.hasBuiltInVariable("${_}", VariableTag.BRACE));
        Assert.assertTrue(ExecutableBlock.hasBuiltInVariable("${_:}", VariableTag.BRACE));
        Assert.assertTrue(ExecutableBlock.hasBuiltInVariable("${_:x}", VariableTag.BRACE));
        Assert.assertTrue(ExecutableBlock.hasBuiltInVariable("this is ${ _ }!", VariableTag.BRACE));
        Assert.assertTrue(ExecutableBlock.hasBuiltInVariable("${_.a}", VariableTag.BRACE));
        Assert.assertTrue(ExecutableBlock.hasBuiltInVariable("${_.a:}", VariableTag.BRACE));
        Assert.assertTrue(ExecutableBlock.hasBuiltInVariable("${_.a:b}", VariableTag.BRACE));
        Assert.assertTrue(ExecutableBlock.hasBuiltInVariable("this is ${ _.a }!", VariableTag.BRACE));
    }

    @Test(groups = { "unit" })
    public void testIsForBridge() {
        Assert.assertFalse(ExecutableBlock.isForBridge(null));
        Assert.assertFalse(ExecutableBlock.isForBridge(""));
        Assert.assertFalse(ExecutableBlock.isForBridge("db"));

        Assert.assertTrue(ExecutableBlock.isForBridge(ExecutableBlock.KEYWORD_TABLE));
        Assert.assertTrue(ExecutableBlock.isForBridge(ExecutableBlock.KEYWORD_VALUES));
    }

    @Test(groups = { "unit" })
    public void testConstructor() {
        Assert.assertThrows(NullPointerException.class,
                () -> new ExecutableBlock(0, null, null, null, null, false));
        Assert.assertThrows(NullPointerException.class,
                () -> new ExecutableBlock(0, "", null, null, null, false));
        Assert.assertThrows(NullPointerException.class,
                () -> new ExecutableBlock(0, "", null, new Properties(), null, false));
        Assert.assertThrows(NullPointerException.class,
                () -> new ExecutableBlock(0, null, null, new Properties(), null, false));
        Assert.assertThrows(NullPointerException.class,
                () -> new ExecutableBlock(0, null, null, new Properties(), "", false));
        Assert.assertThrows(NullPointerException.class,
                () -> new ExecutableBlock(0, null, null, null, "", false));

        ExecutableBlock block = new ExecutableBlock(-1, "ex", VariableTag.BRACE, new Properties(), "...", true);
        Assert.assertEquals(block, new ExecutableBlock(-1, "ex", VariableTag.BRACE, new Properties(), "...", true));
        Assert.assertEquals(block.getIndex(), -1);
        Assert.assertEquals(block.getExtensionName(), "ex");
        Assert.assertEquals(block.getProperties(), new Properties());
        Assert.assertEquals(block.getContent(), "...");
        Assert.assertEquals(block.hasOutput(), true);

        block.getProperties().setProperty("a", "c");
        Properties props = new Properties();
        props.setProperty("a", "c");
        Assert.assertEquals(block, new ExecutableBlock(-1, "ex", VariableTag.BRACE, props, "...", true));
    }

    @Test(groups = { "unit" })
    public void testConstructorWithBuiltInVariables() {
        Properties props = new Properties();
        ExecutableBlock block = new ExecutableBlock(1, "x", VariableTag.BRACE, props, "${_}", false);
        Assert.assertEquals(block, new ExecutableBlock(1, "x", VariableTag.BRACE, props, "x", false));

        props.setProperty("id", "duckdb1");
        block = new ExecutableBlock(2, "db", VariableTag.ANGLE_BRACKET, props, "$<_>.$<_.id> ($<_.name:unknown>)",
                true);
        Assert.assertEquals(block,
                new ExecutableBlock(2, "db", VariableTag.ANGLE_BRACKET, props, "db.duckdb1 (unknown)", true));

        block = new ExecutableBlock(3, "bridge", VariableTag.SQUARE_BRACKET, props, "select '$[_.id]'", true);
        Assert.assertEquals(block.getContent(), "select '$[_.id]'");
        block = new ExecutableBlock(4, "Bridge", VariableTag.SQUARE_BRACKET, props, "select '$[_.id]'", true);
        Assert.assertEquals(block.getContent(), "select 'duckdb1'");
    }

    @Test(groups = { "unit" })
    public void testSameAs() {
        Properties props = new Properties();
        ExecutableBlock block = new ExecutableBlock(1, "b1", VariableTag.BRACE, props, "...", true);
        Assert.assertFalse(block.sameAs(new ExecutableBlock(1, "b2", VariableTag.BRACE, props, "...", true)),
                "Should be false since extension is different");
        Assert.assertFalse(block.sameAs(new ExecutableBlock(1, "b1", VariableTag.BRACE, props, ". .", true)),
                "Should be false since content is different");
        Assert.assertFalse(block.sameAs(new ExecutableBlock(1, "b1", VariableTag.BRACE, props, "...", false)),
                "Should be false since output is different");

        Assert.assertTrue(block.sameAs(new ExecutableBlock(2, "b1", VariableTag.BRACE, props, "...", true)),
                "Should be true because index does not matter");
        Assert.assertTrue(block.sameAs(new ExecutableBlock(1, "b1", VariableTag.BRACE, new Properties(), "...", true)),
                "Should be true because properties do not matter");
        props.setProperty("a", "b");
        Assert.assertTrue(block.sameAs(new ExecutableBlock(1, "b1", VariableTag.BRACE, new Properties(), "...", true)),
                "Should be true because properties do not matter");

        Properties newProps = new Properties();
        Option.ID.setValue(props, "bb");
        block = new ExecutableBlock(1, "b1", VariableTag.BRACE, props, "...", true);
        Assert.assertFalse(block.sameAs(new ExecutableBlock(2, "b1", VariableTag.BRACE, newProps, "...", true)),
                "Should be true because index does not matter");
        Option.ID.setValue(newProps, "bb");
        Assert.assertTrue(block.sameAs(new ExecutableBlock(2, "b1", VariableTag.BRACE, newProps, "...", true)),
                "Should be true because index does not matter");
        Option.ID.setValue(newProps, "aa");
        Assert.assertFalse(block.sameAs(new ExecutableBlock(2, "b1", VariableTag.BRACE, newProps, "...", true)),
                "Should be true because index does not matter");
    }
}
