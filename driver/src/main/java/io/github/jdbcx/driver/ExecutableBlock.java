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
package io.github.jdbcx.driver;

import java.util.Map.Entry;
import java.util.Objects;
import java.util.Properties;

import io.github.jdbcx.Checker;
import io.github.jdbcx.Option;
import io.github.jdbcx.Utils;
import io.github.jdbcx.VariableTag;

import static io.github.jdbcx.QueryContext.KEY_BRIDGE;

/**
 * An executable block represents either a function, which returns a value, or a
 * procedure, which is executed without returning a value.<br>
 * <ul>
 * <li>A function is defined using the syntax: {@code {{[function(args)]:
 * <body>}}}</li>
 * <li>A procedure is defined using the syntax: {@code {%[procedure(args)]:
 * <body>%}}</li>
 * </ul>
 */
public final class ExecutableBlock {
    static final String BUILTIN_VAR = "_";
    static final String BUILTIN_VAR_PREFIX = BUILTIN_VAR.concat(".");

    // reserved keyword
    static final String KEYWORD_TABLE = "table";
    static final String KEYWORD_VALUES = "values";

    static boolean hasBuiltInVariable(String template, VariableTag tag) {
        final char leftChar = tag.leftChar();
        final char rightChar = tag.rightChar();
        final char varChar = tag.variableChar();
        final char escapeChar = tag.escapeChar();

        boolean escaped = false;
        int startIndex = -1;
        int colonIndex = -1;
        for (int i = 0, len = template.length(); i < len; i++) {
            char ch = template.charAt(i);
            if (escaped) {
                escaped = false;
            } else if (ch == escapeChar) {
                escaped = true;
            } else if (startIndex == -1) {
                if (ch == varChar && i + 2 < len && template.charAt(i + 1) == leftChar) {
                    startIndex = i + 2;
                    i++;
                }
            } else {
                if (ch == ':') {
                    colonIndex = i;
                } else if (ch == rightChar) {
                    final String key = template.substring(startIndex, colonIndex >= 0 ? colonIndex : i).trim();
                    final int klen = key.length();
                    if ((klen > BUILTIN_VAR_PREFIX.length() && key.startsWith(BUILTIN_VAR_PREFIX))
                            || (klen == 1 && key.charAt(0) == '_')) {
                        return true;
                    }
                    startIndex = -1;
                    colonIndex = -1;
                }
            }
        }
        return false;
    }

    /**
     * Checks whether an extension name is for bridge server or not.
     *
     * @param extension extension name
     * @return true if it's for bridge server; false otherwise
     */
    static boolean isForBridge(String extension) {
        return KEYWORD_TABLE.equals(extension) || KEYWORD_VALUES.equals(extension);
    }

    private final int index;
    private final String extension;
    private final Properties props;
    private final String content;
    private final boolean output;

    ExecutableBlock(int index, String extension, VariableTag tag, Properties props, String content, boolean output) {
        this.index = index;
        this.extension = extension;
        this.props = props;
        if (!KEY_BRIDGE.equals(extension) && !isForBridge(extension) && hasBuiltInVariable(content, tag)) {
            Properties vars = new Properties();
            vars.setProperty(BUILTIN_VAR, extension);
            for (Entry<Object, Object> entry : props.entrySet()) {
                vars.put(BUILTIN_VAR_PREFIX + entry.getKey(), entry.getValue());
            }
            this.content = Utils.applyVariablesWithDefault(content, tag, vars);
        } else {
            this.content = content;
        }
        this.output = output;
    }

    // mainly for testing
    ExecutableBlock(int index, String extension, Properties props, String content, boolean output) {
        this(index, extension, VariableTag.BRACE, props, content, output);
    }

    public int getIndex() {
        return index;
    }

    public String getExtensionName() {
        return extension;
    }

    public Properties getProperties() {
        return props;
    }

    public String getContent() {
        return content;
    }

    public boolean sameAs(ExecutableBlock block) {
        if (this == block) {
            return true;
        } else if (block == null) {
            return false;
        }

        return output == block.output && extension.equals(block.extension) && content.equals(block.content)
                && Objects.equals(Option.ID.getValue(props), Option.ID.getValue(block.props));
    }

    public boolean hasNoArguments() {
        return props.isEmpty() && Checker.isNullOrBlank(content, true);
    }

    public boolean hasOutput() {
        return output;
    }

    public boolean useBridge() {
        return KEYWORD_TABLE.equals(extension) || KEYWORD_VALUES.equals(extension);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = prime + index;
        result = prime * result + extension.hashCode();
        result = prime * result + props.hashCode();
        result = prime * result + content.hashCode();
        result = prime * result + (output ? 1231 : 1237);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        ExecutableBlock other = (ExecutableBlock) obj;
        return index == other.index && extension.equals(other.extension) && props.equals(other.props)
                && content.equals(other.content) && output == other.output;
    }
}
