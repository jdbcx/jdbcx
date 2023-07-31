/*
 * Copyright 2022-2023, Zhichun Wu
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

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import io.github.jdbcx.Checker;
import io.github.jdbcx.Constants;
import io.github.jdbcx.Logger;
import io.github.jdbcx.LoggerFactory;
import io.github.jdbcx.Utils;

/**
 * This class parses the given query string to extract discrete parts and
 * executable code blocks. It can only recognize function(with a return value,
 * usually string) "{{[-] [func[(key=value)]:] &lt;...&gt; }}" or
 * procedure(void function) "{%[-] [proc[(key=value)]:] &lt;...&gt; %}". Block
 * starts with "{{-" or "{%-"" will be skipped and replaced with empty string.
 */
public final class QueryParser {
    private static final Logger log = LoggerFactory.getLogger(QueryParser.class);

    public static final class Part {
        public final int endPosition;
        public final String escaped;

        Part(int endPosition, String escaped) {
            this.endPosition = endPosition;
            this.escaped = escaped;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = prime + endPosition;
            result = prime * result + escaped.hashCode();
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            } else if (obj == null || getClass() != obj.getClass()) {
                return false;
            }

            Part other = (Part) obj;
            return endPosition == other.endPosition && escaped.equals(other.escaped);
        }

        @Override
        public String toString() {
            return new StringBuilder(Part.class.getSimpleName()).append('(').append(endPosition).append(',')
                    .append(escaped).append(')').toString();
        }
    }

    static int indexOf(String str, int startIndex, String pattern, char escapeChar) {
        int index = startIndex;
        while ((index = str.indexOf(pattern, index)) != -1) {
            if (index == 0 || str.charAt(index - 1) != escapeChar) {
                return index + pattern.length();
            } else {
                index++;
            }
        }
        return -1;
    }

    public static Part newPart(int endPosition, String escapedString) {
        return new Part(endPosition, escapedString);
    }

    static Part extractPropertyNamePart(String str, int startIndex, int len, Properties props) {
        StringBuilder builder = new StringBuilder();
        for (int i = startIndex; i < len; i++) {
            char ch = str.charAt(i);
            if (builder.length() == 0) {
                if (Character.isJavaIdentifierStart(ch)) {
                    builder.append(ch);
                } else if (!Character.isWhitespace(ch)) {
                    throw new IllegalArgumentException(
                            Utils.format("Property name must start with letter but we found [%s]", ch));
                }
            } else {
                if (Character.isJavaIdentifierPart(ch) || ch == '.' || ch == '{' || ch == '}') {
                    builder.append(ch);
                } else if (Character.isWhitespace(ch) || ch == '=') {
                    return new Part(i, Utils.applyVariables(builder.toString(), props));
                } else {
                    throw new IllegalArgumentException(
                            Utils.format("Property name can only contain letter and dot but we found [%s]", ch));
                }
            }
        }

        throw new IllegalArgumentException(Utils.format("Missing value for property [%s]", builder.toString()));
    }

    static Part extractPartBefore(String str, int startIndex, int len, Properties props, char... stopChars) {
        StringBuilder builder = new StringBuilder();
        boolean escaped = false;
        for (int i = startIndex; i < len; i++) {
            char ch = str.charAt(i);
            if (escaped) {
                builder.append(ch);
                escaped = false;
            } else if (ch == '\\') {
                escaped = true;
            } else {
                boolean found = false;
                for (int k = 0; k < stopChars.length; k++) {
                    if (ch == stopChars[k]) {
                        found = true;
                        break;
                    }
                }
                if (found) {
                    return new Part(i, Utils.applyVariables(builder.toString().trim(), props));
                } else {
                    builder.append(ch);
                }
            }
        }
        return new Part(len, Utils.applyVariables(builder.toString().trim(), props));
    }

    static Part extractPropertyValuePart(String str, int startIndex, int len, Properties props) {
        Part part = null;
        for (int i = startIndex; i < len; i++) {
            char ch = str.charAt(i);
            if (ch == '\'' || ch == '"' || ch == '`') {
                part = extractQuotedPart(str, i + 1, len, props, ch);
            } else if (ch == ',' || ch == ')') {
                part = new Part(i, Constants.EMPTY_STRING);
            } else if (!Character.isWhitespace(ch)) {
                part = extractPartBefore(str, i, len, props, ',', ')');
            }

            if (part != null) {
                break;
            }
        }

        return part != null ? part : newPart(len, Constants.EMPTY_STRING);
    }

    /**
     * Extracts quoted part from the given string.
     *
     * @param str        non-null string
     * @param startIndex start index, should between 0 and {@code len}
     * @param len        length of the string
     * @param props      optional properties for substitution
     * @param quote      quote character
     * @return quoted part
     */
    static Part extractQuotedPart(String str, int startIndex, int len, Properties props, char quote) {
        StringBuilder builder = new StringBuilder();
        boolean escaped = false;
        for (int i = startIndex; i < len; i++) {
            char ch = str.charAt(i);
            if (escaped) {
                builder.append(ch);
                escaped = false;
            } else if (ch == '\\') {
                escaped = true;
            } else if (ch == quote) {
                if (++i < len && str.charAt(i) == quote) {
                    builder.append(ch);
                } else {
                    return new Part(i, Utils.applyVariables(builder.toString(), props));
                }
            } else {
                builder.append(ch);
            }
        }

        throw new IllegalArgumentException(Utils.format("Missing quote: [%s]", quote));
    }

    static int extractProperties(String str, int startIndex, int len, Properties props) {
        String key = null;
        for (int i = startIndex; i < len; i++) {
            char ch = str.charAt(i);
            if (ch == '=') {
                if (key == null) {
                    key = Constants.EMPTY_STRING;
                } else {
                    Part part = extractPropertyValuePart(str, i + 1, len, props);
                    i = part.endPosition - 1;
                    props.setProperty(key, part.escaped);
                    key = null;
                }
            } else if (ch == ')') {
                return i;
            } else if (ch != ',' && !Character.isWhitespace(ch)) {
                if (key == null) {
                    Part part = extractPropertyNamePart(str, i, len, props);
                    i = part.endPosition - 1;
                    key = part.escaped;
                } else {
                    throw new IllegalArgumentException("Expects = before property value");
                }
            }
        }

        return len - 1;
    }

    static String[] parseExecutableBlock(String block, Properties props) {
        int beginIndex = 0;
        int scope = -1;
        String extension = Constants.EMPTY_STRING;
        String script = null;
        for (int i = 0, len = block.length(); i < len; i++) {
            char ch = block.charAt(i);
            if (scope == -1) { // the very beginning
                if (Character.isJavaIdentifierStart(ch)) {
                    scope = 0;
                    beginIndex = i;
                } else if (!Character.isWhitespace(ch)) {
                    break;
                }
            } else if (scope == 0) { // extension
                if (Character.isJavaIdentifierPart(ch)) {
                } else if (Character.isWhitespace(ch)) {
                    extension = block.substring(beginIndex, i);
                    scope = 1;
                } else if (ch == '(') {
                    extension = block.substring(beginIndex, i);
                    i = extractProperties(block, i + 1, len, props);
                    scope = 1;
                    beginIndex = i + 1;
                } else if (ch == ':') {
                    extension = block.substring(beginIndex, i);
                    scope = 2;
                    beginIndex = i + 1;
                } else {
                    break;
                }
            } else if (scope == 1) { // properties
                if (ch == '(') {
                    i = extractProperties(block, i + 1, len, props);
                    beginIndex = i + 1;
                } else if (ch == ':') {
                    scope = 2;
                    beginIndex = i + 1;
                } else if (Character.isWhitespace(ch)) {
                    beginIndex = i + 1;
                } else {
                    extension = Constants.EMPTY_STRING;
                    break;
                }
            } else if (Character.isWhitespace(ch)) {
                beginIndex = i + 1;
            } else { // script
                script = block.substring(i);
                break;
            }
        }

        if (script == null) {
            script = block.substring(beginIndex);
        }
        return new String[] { extension, script };
    }

    static void addExecutableBlock(StringBuilder builder, String executableBlock, Properties vars, boolean output,
            List<String> parts, List<ExecutableBlock> blocks) {
        parts.add(Utils.applyVariables(builder.toString(), vars));
        builder.setLength(0);

        int id = parts.size();
        Properties props = new Properties();
        if (vars != null) {
            props.putAll(vars);
        }
        String[] parsed = parseExecutableBlock(Utils.applyVariables(executableBlock, vars), props);
        parts.add(Constants.EMPTY_STRING); // placeholder
        blocks.add(new ExecutableBlock(id, parsed[0], props, parsed[1], output));
    }

    /**
     * Parses the given query string. Same as {@code parse(query, vars, '\\')}.
     *
     * @param query the query string to parse
     * @param vars  optional variables for substitution
     * @return non-null parsed query
     */
    public static ParsedQuery parse(String query, Properties vars) {
        return parse(query, vars, '\\');
    }

    /**
     * Parses the given query string to extract discrete parts and executable code
     * blocks.
     *
     * @param query      the query string to parse
     * @param vars       optional variables for substitution
     * @param escapeChar the escape character, typically backslash - cannot be
     *                   percent (%) or brace ({})
     * @return non-null parsed query
     */
    public static ParsedQuery parse(String query, Properties vars, char escapeChar) {
        if (Checker.isNullOrEmpty(query)) {
            return new ParsedQuery(Collections.emptyList(), Collections.emptyList());
        } else if (escapeChar == '%' || escapeChar == '{' || escapeChar == '}') {
            throw new IllegalArgumentException("Escape character cannot be percent or brace");
        }

        final int len = query.length();
        List<String> parts = new LinkedList<>();
        List<ExecutableBlock> blocks = new LinkedList<>();
        StringBuilder builder = new StringBuilder(len);
        boolean escaped = false; // "\\{{" -> "{{", "\\{%" -> "{%"
        for (int i = 0; i < len; i++) {
            char ch = query.charAt(i);
            if (ch == escapeChar) {
                if (escaped) {
                    builder.append(ch).append(ch);
                    escaped = false;
                } else {
                    escaped = true;
                }
            } else if (ch == '{') {
                char nextChar = escapeChar;
                if (++i < len) {
                    nextChar = query.charAt(i);
                } else {
                    builder.append(ch);
                    break;
                }

                if (nextChar == ch && !escaped) { // executable block with output: {{ ... }}
                    int index = indexOf(query, i + 1, "}}", escapeChar);
                    if (index > 0) {
                        for (int k = index; k < len; k++) { // be greedy
                            if (query.charAt(k) == '}') {
                                index++;
                            } else {
                                break;
                            }
                        }
                        if (query.charAt(i + 1) == '-') {
                            log.debug("Skip executable block: %s", query.substring(i - 1, index));
                        } else {
                            addExecutableBlock(builder, query.substring(i + 1, index - 2), vars, true, parts, blocks);
                        }
                        i = index - 1;
                    } else {
                        builder.append(ch).append(nextChar);
                        log.debug("Executable block starts with \"{{\" at %d but missing \"}}\"", i - 1);
                    }
                } else if (nextChar == '%' && !escaped) { // executable block: {% ... %}
                    int index = indexOf(query, i + 1, "%}", escapeChar);
                    if (index > 0) {
                        if (query.charAt(i + 1) == '-') {
                            log.debug("Skip executable block: %s", query.substring(i - 1, index));
                        } else {
                            addExecutableBlock(builder, query.substring(i + 1, index - 2), vars, false, parts, blocks);
                        }
                        i = index - 1;
                    } else {
                        builder.append(ch);
                        log.debug("Executable block starts with \"{%%\" at %d but missing \"%%}\"", i - 1);
                    }
                } else {
                    builder.append(ch).append(nextChar);
                    escaped = false;
                }
            } else {
                if (escaped) {
                    builder.append(escapeChar);
                    escaped = false;
                }
                builder.append(ch);
            }
        }

        if (builder.length() > 0) {
            parts.add(Utils.applyVariables(builder.toString(), vars));
        }

        return new ParsedQuery(parts, blocks);
    }

    private QueryParser() {
    }
}
