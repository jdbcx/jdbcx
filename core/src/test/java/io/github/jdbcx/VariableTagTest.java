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

import org.testng.Assert;
import org.testng.annotations.Test;

public class VariableTagTest {
    @Test(groups = { "unit" })
    public void testExpression() {
        String name = "test";
        for (VariableTag tag : VariableTag.values()) {
            Assert.assertEquals(tag.functionLeft(), new String(new char[] { tag.leftChar(), tag.leftChar() }));
            Assert.assertEquals(tag.functionRight(), new String(new char[] { tag.rightChar(), tag.rightChar() }));

            Assert.assertEquals(tag.procedureLeft(), new String(new char[] { tag.leftChar(), tag.procedureChar() }));
            Assert.assertEquals(tag.procedureRight(), new String(new char[] { tag.procedureChar(), tag.rightChar() }));

            Assert.assertEquals(tag.variableLeft(), new String(new char[] { tag.variableChar(), tag.leftChar() }));

            Assert.assertEquals(tag.function(name), new StringBuilder().append(tag.leftChar()).append(tag.leftChar())
                    .append(name).append(tag.rightChar()).append(tag.rightChar()).toString());
            Assert.assertEquals(tag.procedure(name),
                    new StringBuilder().append(tag.leftChar()).append(tag.procedureChar())
                            .append(name).append(tag.procedureChar()).append(tag.rightChar()).toString());
            Assert.assertEquals(tag.variable(name), new StringBuilder().append(tag.variableChar())
                    .append(tag.leftChar()).append(name).append(tag.rightChar()).toString());
        }
    }

    @Test(groups = { "unit" })
    public void testIsValidForEscaping() {
        for (VariableTag tag : VariableTag.values()) {
            Assert.assertFalse(tag.isValidForEscaping(tag.leftChar()),
                    Utils.format("'%s' should NOT be good for escaping", tag.leftChar()));
            Assert.assertFalse(tag.isValidForEscaping(tag.rightChar()),
                    Utils.format("'%s' should NOT be good for escaping", tag.rightChar()));
            Assert.assertFalse(tag.isValidForEscaping(tag.procedureChar()),
                    Utils.format("'%s' should NOT be good for escaping", tag.procedureChar()));
            Assert.assertTrue(tag.isValidForEscaping(tag.escapeChar()),
                    Utils.format("'%s' should be good for escaping", tag.escapeChar()));
        }
    }
}
