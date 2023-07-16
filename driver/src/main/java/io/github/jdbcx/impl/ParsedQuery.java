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
package io.github.jdbcx.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class ParsedQuery {
    private final List<String> parts;
    private final List<ExecutableBlock> blocks;

    ParsedQuery(List<String> parts, List<ExecutableBlock> blocks) {
        this.parts = Collections.unmodifiableList(new ArrayList<>(parts));
        this.blocks = Collections.unmodifiableList(new ArrayList<>(blocks));
    }

    public List<String> getQueryParts() {
        return parts;
    }

    public List<ExecutableBlock> getExecutableBlocks() {
        return blocks;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = prime + parts.hashCode();
        result = prime * result + blocks.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        ParsedQuery other = (ParsedQuery) obj;
        return parts.equals(other.parts) && blocks.equals(other.blocks);
    }
}
