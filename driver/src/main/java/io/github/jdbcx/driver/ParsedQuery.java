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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import io.github.jdbcx.Checker;

public final class ParsedQuery {
    static final ParsedQuery EMPTY = new ParsedQuery(Collections.emptyList(), Collections.emptyList());

    private final List<String> parts;
    private final List<ExecutableBlock> blocks;

    private final boolean directQuery;

    ParsedQuery(List<String> parts, List<ExecutableBlock> blocks) {
        if (parts == null || blocks == null) {
            throw new IllegalArgumentException("Non-null static parts and executable blocks are required");
        }

        this.parts = Collections.unmodifiableList(new ArrayList<>(parts));
        this.blocks = Collections.unmodifiableList(new ArrayList<>(blocks));

        boolean found = false;
        for (String p : parts) {
            if (!Checker.isNullOrBlank(p, true)) {
                found = true;
                break;
            }
        }

        if (found) {
            this.directQuery = false;
        } else {
            int count = 0;
            for (ExecutableBlock b : blocks) {
                if (b.hasOutput()) {
                    count++;
                }
            }
            this.directQuery = count < 2;
        }
    }

    public List<String> getStaticParts() {
        return parts;
    }

    public List<ExecutableBlock> getExecutableBlocks() {
        return blocks;
    }

    public boolean isDirectQuery() {
        return directQuery;
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
