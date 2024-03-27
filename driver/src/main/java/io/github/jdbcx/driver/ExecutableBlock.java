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

import io.github.jdbcx.Checker;

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
    // reserved keyword
    static final String KEYWORD_TABLE = "table";
    static final String KEYWORD_VALUES = "values";

    private final int index;
    private final String extension;
    private final Properties props;
    private final String content;
    private final boolean output;

    ExecutableBlock(int index, String extension, Properties props, String content, boolean output) {
        if (extension == null || props == null || content == null) {
            throw new IllegalArgumentException("Non-null extension name, properties, and content are required!");
        }

        this.index = index;
        this.extension = extension;
        this.props = props;
        this.content = content;
        this.output = output;
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

        return output == block.output && extension.equals(block.extension) && content.equals(block.content);
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
