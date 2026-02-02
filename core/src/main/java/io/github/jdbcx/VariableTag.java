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

/**
 * Enum representing different types of variable tags can be used in SQL
 * template and dynamic queries.
 */
public enum VariableTag {
    /**
     * Curly brace-based variable tags, similar to Jinja2 syntax.
     * Use <code>{{ ... }}</code> for function calls with return values,
     * <code>{% ... %}</code> for procedures without return values, and
     * <code>${...}</code> for variables.
     */
    BRACE('{', '}', '%', '$', '\\'),
    /**
     * Angle bracket-based variable tags, similar to JSP syntax.
     * Use <code>&lt;&lt; ... &gt;&gt;</code> for function calls with return values,
     * <code>&lt;% ... %&gt;</code> for procedures without return values, and
     * <code>$&lt;...&gt;</code> for variables.
     */
    ANGLE_BRACKET('<', '>', '%', '$', '\\'),
    /**
     * Square bracket-based variable tags, not recommended due to conflicts
     * with nested arrays. Use <code>[[ ... ]]</code> for function calls
     * with return values, <code>[% ... %]</code> for procedures without return
     * values, and <code>$[...]</code> for variables.
     */
    SQUARE_BRACKET('[', ']', '%', '$', '\\');

    private final char left;
    private final char right;
    private final char proc;
    private final char val;
    private final char escape;

    private final String funcLeft;
    private final String funcRight;
    private final String procLeft;
    private final String procRight;
    private final String varLeft;

    /**
     * Constructor for VariableTag enum.
     *
     * @param left   The left delimiter character
     * @param right  The right delimiter character
     * @param proc   The delimiter character used in procedure expression
     * @param val    The variable delimiter character
     * @param escape The escape character
     */
    private VariableTag(char left, char right, char proc, char val, char escape) {
        this.left = left;
        this.right = right;
        this.proc = proc;
        this.val = val;
        this.escape = escape;

        this.funcLeft = new String(new char[] { left, left });
        this.funcRight = new String(new char[] { right, right });

        this.procLeft = new String(new char[] { left, proc });
        this.procRight = new String(new char[] { proc, right });

        this.varLeft = new String(new char[] { val, left });
    }

    /**
     * Gets the left delimiter character.
     *
     * @return The left delimiter character
     */
    public char leftChar() {
        return left;
    }

    /**
     * Gets the right delimiter character.
     *
     * @return The right delimiter character
     */
    public char rightChar() {
        return right;
    }

    /**
     * Gets the delimiter character used in procedure expression.
     *
     * @return The delimiter character used in procedure expression
     */
    public char procedureChar() {
        return proc;
    }

    /**
     * Gets the variable delimiter character.
     *
     * @return The variable delimiter character
     */
    public char variableChar() {
        return val;
    }

    /**
     * Gets the escape character.
     *
     * @return The escape character
     */
    public char escapeChar() {
        return escape;
    }

    /**
     * Checks if the given character is valid for escaping or not.
     *
     * @param escape character for escaping
     * @return true if the given character is valid for escaping; false otherwise
     */
    public boolean isValidForEscaping(char escape) {
        return escape != left && escape != right && escape != proc;
    }

    /**
     * Gets the function left delimiter.
     *
     * @return The function left delimiter
     */
    public String functionLeft() {
        return funcLeft;
    }

    /**
     * Gets the function right delimiter.
     *
     * @return The function right delimiter
     */
    public String functionRight() {
        return funcRight;
    }

    /**
     * Gets the procedure left delimiter.
     *
     * @return The procedure left delimiter
     */
    public String procedureLeft() {
        return procLeft;
    }

    /**
     * Gets the procedure right delimiter.
     *
     * @return The procedure right delimiter
     */
    public String procedureRight() {
        return procRight;
    }

    /**
     * Gets the variable left delimiter.
     *
     * @return The variable left delimiter
     */
    public String variableLeft() {
        return varLeft;
    }

    /**
     * Constructs a function expression with the provided content.
     *
     * @param content The content of the function
     * @return The constructed function string
     */
    public String function(String content) {
        StringBuilder builder = new StringBuilder(content.length() + 4);
        return builder.append(funcLeft).append(content).append(funcRight).toString();
    }

    /**
     * Constructs a procedure expression with the provided content.
     *
     * @param content The content of the procedure
     * @return The constructed procedure string
     */
    public String procedure(String content) {
        StringBuilder builder = new StringBuilder(content.length() + 4);
        return builder.append(procLeft).append(content).append(procRight).toString();
    }

    /**
     * Constructs a variable expression with the provided name.
     *
     * @param name The name of the variable
     * @return The constructed variable string
     */
    public String variable(String name) {
        StringBuilder builder = new StringBuilder(name.length() + 3);
        return builder.append(varLeft).append(name).append(right).toString();
    }
}
