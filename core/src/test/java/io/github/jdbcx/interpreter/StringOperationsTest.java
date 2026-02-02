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
package io.github.jdbcx.interpreter;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.github.jdbcx.VariableTag;

public class StringOperationsTest {
    @Test(groups = { "unit" })
    public void testEscape() {
        Assert.assertEquals(StringOperations.escape(new String[] { "" }, '"', '\\'), new String[] { "" });
        Assert.assertEquals(StringOperations.escape(new String[] { "", "\"x\"='a'", "" }, '"', '\\'),
                new String[] { "", "\\\"x\\\"='a'", "" });
    }

    @Test(groups = { "unit" })
    public void testExtract() {
        Assert.assertEquals(StringOperations.extract(new StringReader("{\"a\":2}"), "a"), new String[] { "2" });
        Assert.assertEquals(
                StringOperations.extract(new ByteArrayInputStream("{\"a\":2}".getBytes()), StandardCharsets.UTF_8, "a"),
                new String[] { "2" });
    }

    @Test(groups = { "unit" })
    public void testRead() throws IOException {
        Assert.assertThrows(UncheckedIOException.class, () -> StringOperations.read("non-existent.file"));

        Assert.assertEquals(StringOperations.read("target/test-classes/docker-compose.yml"),
                StringOperations.read(new FileInputStream("target/test-classes/docker-compose.yml")));
        Assert.assertTrue(StringOperations.read("target/test-classes/docker-compose.yml").length == 1);
    }

    @Test(groups = { "unit" })
    public void testReplace() {
        Properties props = new Properties();
        Assert.assertEquals(StringOperations.replace(new String[] { "${x}" }, null, props, false),
                new String[] { "${x}" });
        Assert.assertEquals(
                StringOperations.replace(new String[] { "${x}" }, VariableTag.ANGLE_BRACKET, props, false),
                new String[] { "${x}" });
        Assert.assertEquals(
                StringOperations.replace(new String[] { "$<x>" }, VariableTag.ANGLE_BRACKET, props, false),
                new String[] { "$<x>" });
        Assert.assertEquals(StringOperations.replace(new String[] { "${x:1}" }, null, props, true),
                new String[] { "1" });
        Assert.assertEquals(StringOperations.replace(new String[] { "${x:1}" }, VariableTag.ANGLE_BRACKET, props, true),
                new String[] { "${x:1}" });

        props.setProperty("x", "233");
        Assert.assertEquals(StringOperations.replace(new String[] { "${x}" }, null, props, false),
                new String[] { "233" });
        Assert.assertEquals(
                StringOperations.replace(new String[] { "${x}" }, VariableTag.ANGLE_BRACKET, props, false),
                new String[] { "${x}" });
        Assert.assertEquals(
                StringOperations.replace(new String[] { "$<x>" }, VariableTag.ANGLE_BRACKET, props, false),
                new String[] { "233" });
        Assert.assertEquals(
                StringOperations.replace(new String[] { "${x:1}" }, VariableTag.ANGLE_BRACKET, props, false),
                new String[] { "${x:1}" });
        Assert.assertEquals(StringOperations.replace(new String[] { "${x:1}" }, null, props, true),
                new String[] { "233" });
        Assert.assertEquals(StringOperations.replace(new String[] { "${x:1}" }, VariableTag.ANGLE_BRACKET, props, true),
                new String[] { "${x:1}" });
    }

    @Test(groups = { "unit" })
    public void testSplit() {
        Assert.assertEquals(StringOperations.split(new String[] { "a\n\nb \nc\n" }, "\n", false, false),
                new String[] { "a", "", "b ", "c" });
        Assert.assertEquals(StringOperations.split(new String[] { "a\n\nb \nc\n" }, "\n", true, false),
                new String[] { "a", "", "b", "c" });
        Assert.assertEquals(StringOperations.split(new String[] { "a\n\nb \nc\n" }, "\n", false, true),
                new String[] { "a", "b ", "c" });
        Assert.assertEquals(StringOperations.split(new String[] { "a\n\nb \nc\n" }, "\n", true, true),
                new String[] { "a", "b", "c" });
    }

    @Test(groups = { "unit" })
    public void testTrim() {
        Assert.assertEquals(StringOperations.trim(new String[] { " ", " a\t\n\r " }, false), new String[] { "", "a" });
        Assert.assertEquals(StringOperations.trim(new String[] { " ", " a\t\n\r " }, true), new String[] { "a" });
    }
}
